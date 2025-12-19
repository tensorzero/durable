use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};
use tokio::time::{Instant, sleep, sleep_until};
use tracing::Instrument;
use uuid::Uuid;

use crate::context::TaskContext;
use crate::error::{ControlFlow, TaskError, serialize_task_error};
use crate::task::TaskRegistry;
use crate::types::{ClaimedTask, ClaimedTaskRow, WorkerOptions};

/// Notifies the worker that the lease has been extended.
/// Used by TaskContext to reset warning/fatal timers.
#[derive(Clone)]
pub(crate) struct LeaseExtender {
    tx: mpsc::Sender<Duration>,
}

impl LeaseExtender {
    /// Signal that the lease has been extended.
    /// Uses try_send to avoid blocking - if buffer is full, timer will reset on next send.
    pub fn notify(&self, extend_by: Duration) {
        let _ = self.tx.try_send(extend_by);
    }

    #[cfg(test)]
    pub fn dummy_for_tests() -> Self {
        Self {
            tx: mpsc::channel(1).0,
        }
    }
}

/// A background worker that processes tasks from a queue.
///
/// Workers are created via [`Durable::start_worker`](crate::Durable::start_worker) and run in the background,
/// polling for tasks and executing them. Multiple workers can process the same
/// queue concurrently for horizontal scaling.
///
/// # Example
///
/// ```ignore
/// let worker = client.start_worker(WorkerOptions {
///     concurrency: 4,
///     ..Default::default()
/// }).await;
///
/// // Worker runs in background...
/// tokio::signal::ctrl_c().await?;
///
/// // Graceful shutdown waits for in-flight tasks
/// worker.shutdown().await;
/// ```
pub struct Worker {
    shutdown_tx: broadcast::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

impl Worker {
    pub(crate) async fn start<State>(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry<State>>>,
        options: WorkerOptions,
        state: State,
    ) -> Self
    where
        State: Clone + Send + Sync + 'static,
    {
        let (shutdown_tx, _) = broadcast::channel(1);
        let shutdown_rx = shutdown_tx.subscribe();

        let worker_id = options.worker_id.clone().unwrap_or_else(|| {
            format!(
                "{}:{}",
                hostname::get()
                    .map(|h| h.to_string_lossy().to_string())
                    .unwrap_or_else(|_| "unknown".to_string()),
                std::process::id()
            )
        });

        let handle = tokio::spawn(Self::run_loop(
            pool,
            queue_name,
            registry,
            options,
            worker_id,
            shutdown_rx,
            state,
        ));

        Self {
            shutdown_tx,
            handle,
        }
    }

    /// Gracefully shut down the worker.
    ///
    /// Signals the worker to stop accepting new tasks and waits for all
    /// in-flight tasks to complete before returning.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
    }

    async fn run_loop<State>(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry<State>>>,
        options: WorkerOptions,
        worker_id: String,
        mut shutdown_rx: broadcast::Receiver<()>,
        state: State,
    ) where
        State: Clone + Send + Sync + 'static,
    {
        let concurrency = options.concurrency;
        let batch_size = options.batch_size.unwrap_or(concurrency);
        let claim_timeout = options.claim_timeout;
        let poll_interval = std::time::Duration::from_secs_f64(options.poll_interval);
        let fatal_on_lease_timeout = options.fatal_on_lease_timeout;

        // Mark worker as active
        #[cfg(feature = "telemetry")]
        crate::telemetry::set_worker_active(&queue_name, &worker_id, true);

        // Semaphore limits concurrent task execution
        let semaphore = Arc::new(Semaphore::new(concurrency));

        // Channel for tracking task completion (for graceful shutdown)
        let (done_tx, mut done_rx) = mpsc::channel::<()>(concurrency);

        loop {
            tokio::select! {
                // Shutdown signal received
                _ = shutdown_rx.recv() => {
                    tracing::info!("Worker shutting down, waiting for in-flight tasks...");

                    #[cfg(feature = "telemetry")]
                    crate::telemetry::set_worker_active(&queue_name, &worker_id, false);

                    drop(done_tx);
                    while done_rx.recv().await.is_some() {}
                    tracing::info!("Worker shutdown complete");
                    break;
                }

                // Poll for new tasks
                _ = sleep(poll_interval) => {
                    // Acquire permits BEFORE claiming tasks to avoid claiming
                    // tasks from DB that we can't immediately execute
                    let mut permits = Vec::new();
                    for _ in 0..batch_size {
                        match semaphore.clone().try_acquire_owned() {
                            Ok(permit) => permits.push(permit),
                            Err(_) => break,
                        }
                    }

                    if permits.is_empty() {
                        continue;
                    }

                    let tasks = match Self::claim_tasks(
                        &pool,
                        &queue_name,
                        &worker_id,
                        claim_timeout,
                        permits.len(),
                    ).await {
                        Ok(tasks) => tasks,
                        Err(e) => {
                            tracing::error!("Failed to claim tasks: {}", e);
                            continue;
                        }
                    };

                    // Return unused permits (if we claimed fewer tasks than permits acquired)
                    let permits = permits.into_iter().take(tasks.len());

                    for (task, permit) in tasks.into_iter().zip(permits) {
                        let pool = pool.clone();
                        let queue_name = queue_name.clone();
                        let registry = registry.clone();
                        let done_tx = done_tx.clone();
                        let state = state.clone();

                        tokio::spawn(async move {
                            Self::execute_task(
                                pool,
                                queue_name,
                                registry,
                                task,
                                claim_timeout,
                                fatal_on_lease_timeout,
                                state,
                            ).await;

                            drop(permit);
                            let _ = done_tx.send(()).await;
                        });
                    }
                }
            }
        }
    }

    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.worker.claim_tasks",
            skip(pool),
            fields(queue = %queue_name, worker_id = %worker_id, count = count)
        )
    )]
    async fn claim_tasks(
        pool: &PgPool,
        queue_name: &str,
        worker_id: &str,
        claim_timeout: u64,
        count: usize,
    ) -> anyhow::Result<Vec<ClaimedTask>> {
        #[cfg(feature = "telemetry")]
        let start = std::time::Instant::now();

        let query = "SELECT run_id, task_id, attempt, task_name, params, retry_strategy,
                    max_attempts, headers, wake_event, event_payload
             FROM durable.claim_task($1, $2, $3, $4)";

        let rows: Vec<ClaimedTaskRow> = sqlx::query_as(query)
            .bind(queue_name)
            .bind(worker_id)
            .bind(claim_timeout as i32)
            .bind(count as i32)
            .fetch_all(pool)
            .await?;

        let tasks: Vec<ClaimedTask> = rows
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        #[cfg(feature = "telemetry")]
        {
            let duration = start.elapsed().as_secs_f64();
            crate::telemetry::record_task_claim_duration(queue_name, duration);
            for _ in &tasks {
                crate::telemetry::record_task_claimed(queue_name);
            }
        }

        Ok(tasks)
    }

    async fn execute_task<State>(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry<State>>>,
        task: ClaimedTask,
        claim_timeout: u64,
        fatal_on_lease_timeout: bool,
        state: State,
    ) where
        State: Clone + Send + Sync + 'static,
    {
        // Create span for task execution, linked to parent trace context if available
        let span = tracing::info_span!(
            "durable.worker.execute_task",
            queue = %queue_name,
            task_id = %task.task_id,
            run_id = %task.run_id,
            task_name = %task.task_name,
            attempt = task.attempt,
        );

        // Extract and set parent trace context from headers (for distributed tracing)
        #[cfg(feature = "telemetry")]
        if let Some(ref headers) = task.headers {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let parent_cx = crate::telemetry::extract_trace_context(headers);
            span.set_parent(parent_cx);
        }

        Self::execute_task_inner(
            pool,
            queue_name,
            registry,
            task,
            claim_timeout,
            fatal_on_lease_timeout,
            state,
        )
        .instrument(span)
        .await
    }

    async fn execute_task_inner<State>(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry<State>>>,
        task: ClaimedTask,
        claim_timeout: u64,
        fatal_on_lease_timeout: bool,
        state: State,
    ) where
        State: Clone + Send + Sync + 'static,
    {
        let task_label = format!("{} ({})", task.task_name, task.task_id);
        let task_id = task.task_id;
        let run_id = task.run_id;
        #[cfg(feature = "telemetry")]
        let task_name = task.task_name.clone();
        #[cfg(feature = "telemetry")]
        let queue_name_for_metrics = queue_name.clone();
        let start_time = Instant::now();

        // Create lease extension channel - TaskContext will notify when lease is extended
        let (lease_tx, mut lease_rx) = mpsc::channel::<Duration>(1);
        let lease_extender = LeaseExtender { tx: lease_tx };

        // Create task context
        let ctx = match TaskContext::create(
            pool.clone(),
            queue_name.clone(),
            task.clone(),
            claim_timeout,
            lease_extender,
            registry.clone(),
            state.clone(),
        )
        .await
        {
            Ok(ctx) => ctx,
            Err(e) => {
                tracing::error!("Failed to create task context: {}", e);
                Self::fail_run(&pool, &queue_name, task.run_id, &e.into()).await;
                return;
            }
        };

        // Look up handler
        let registry = registry.read().await;
        let handler = match registry.get(task.task_name.as_str()) {
            Some(h) => *h,
            None => {
                tracing::error!("Unknown task: {}", task.task_name);
                Self::fail_run(
                    &pool,
                    &queue_name,
                    task.run_id,
                    &TaskError::Validation {
                        message: format!("Unknown task: {}", task.task_name),
                    },
                )
                .await;
                return;
            }
        };
        drop(registry);

        // Execute task with timeout enforcement
        let task_handle = tokio::spawn({
            let params = task.params.clone();
            async move { handler.execute(params, ctx, state).await }
        });
        let abort_handle = task_handle.abort_handle();

        // Resettable timer task that tracks both warn and fatal deadlines.
        // Resets whenever lease_rx receives a notification (on step()/heartbeat()).
        // Only returns when fatal timeout is reached - never exits early.
        let timer_handle = tokio::spawn({
            let task_label = task_label.clone();
            async move {
                let mut warn_duration = Duration::from_secs(claim_timeout);
                let mut fatal_duration = warn_duration * 2;
                let mut warn_fired = false;
                let mut deadline = Instant::now();

                loop {
                    let warn_at = deadline + warn_duration;
                    let fatal_at = deadline + fatal_duration;

                    let channel_fut = async {
                        if lease_rx.is_closed() && lease_rx.is_empty() {
                            // Wait forever, so that we'll hit one of the timeout branches
                            // in the `tokio::select!` below.
                            futures::future::pending().await
                        } else {
                            lease_rx.recv().await
                        }
                    };

                    tokio::select! {
                        biased; // Check channel first to prioritize resets

                        msg = channel_fut => {
                            if let Some(extension) = msg {
                                // Lease extended - reset deadline and warning state
                                warn_duration = extension;
                                fatal_duration = extension * 2;
                                deadline = Instant::now();
                                warn_fired = false;
                            }

                        }

                        _ = sleep_until(warn_at), if !warn_fired => {
                            tracing::warn!(
                                "Task {} exceeded claim timeout of {}s (no heartbeat/step since last extension)",
                                task_label,
                                claim_timeout
                            );
                            warn_fired = true;
                        }

                        _ = sleep_until(fatal_at) => {
                            // Fatal timeout reached
                            return;
                        }
                    }
                }
            }
        });
        let timer_abort_handle = timer_handle.abort_handle();

        // Wait for either task completion or fatal timeout
        let result = tokio::select! {
            result = task_handle => {
                match result {
                    Ok(r) => Some(r),
                    Err(e) if e.is_cancelled() => None, // Task was aborted
                    Err(e) => {
                        tracing::error!("Task {} panicked: {}", task_label, e);
                        Some(Err(TaskError::TaskInternal(anyhow::anyhow!("Task panicked: {e}"))))
                    }
                }
            }
            _ = timer_handle => {
                // Fatal timeout reached - timer only returns on fatal
                let elapsed = start_time.elapsed();
                if fatal_on_lease_timeout {
                    tracing::error!(
                        task_id = %task_id,
                        run_id = %run_id,
                        elapsed_secs = elapsed.as_secs(),
                        claim_timeout_secs = claim_timeout,
                        "Task {} exceeded 2x claim timeout without heartbeat; terminating process",
                        task_label
                    );
                    std::process::exit(1);
                } else {
                    tracing::error!(
                        task_id = %task_id,
                        run_id = %run_id,
                        elapsed_secs = elapsed.as_secs(),
                        claim_timeout_secs = claim_timeout,
                        "Task {} exceeded 2x claim timeout without heartbeat; aborting task",
                        task_label
                    );
                    abort_handle.abort();
                    None
                }
            }
        };

        // Cancel timer task if still running
        timer_abort_handle.abort();

        // Handle result
        let Some(result) = result else {
            // Task was aborted due to timeout - don't mark as failed since
            // another worker will pick it up after claim expires
            tracing::warn!(
                "Task {} aborted due to timeout, will be retried",
                task_label
            );
            return;
        };

        // Record metrics for task execution
        #[cfg(feature = "telemetry")]
        let outcome: &str;

        match result {
            Ok(output) => {
                #[cfg(feature = "telemetry")]
                {
                    outcome = "completed";
                }
                Self::complete_run(&pool, &queue_name, task.run_id, output).await;

                #[cfg(feature = "telemetry")]
                crate::telemetry::record_task_completed(&queue_name_for_metrics, &task_name);
            }
            Err(TaskError::Control(ControlFlow::Suspend)) => {
                // Task suspended - do nothing, scheduler will resume it
                #[cfg(feature = "telemetry")]
                {
                    outcome = "suspended";
                }
                tracing::debug!("Task {} suspended", task_label);
            }
            Err(TaskError::Control(ControlFlow::Cancelled)) => {
                // Task cancelled - do nothing
                #[cfg(feature = "telemetry")]
                {
                    outcome = "cancelled";
                }
                tracing::info!("Task {} was cancelled", task_label);
            }
            Err(ref e) => {
                // All other errors are failures (Timeout, Database, Serialization, ChildFailed, etc.)
                #[cfg(feature = "telemetry")]
                {
                    outcome = "failed";
                }
                tracing::error!("Task {} failed: {}", task_label, e);
                Self::fail_run(&pool, &queue_name, task.run_id, e).await;

                #[cfg(feature = "telemetry")]
                crate::telemetry::record_task_failed(
                    &queue_name_for_metrics,
                    &task_name,
                    "task_error",
                );
            }
        }

        // Record execution duration
        #[cfg(feature = "telemetry")]
        {
            let duration = start_time.elapsed().as_secs_f64();
            crate::telemetry::record_task_execution_duration(
                &queue_name_for_metrics,
                &task_name,
                outcome,
                duration,
            );
        }
    }

    async fn complete_run(pool: &PgPool, queue_name: &str, run_id: Uuid, result: JsonValue) {
        let query = "SELECT durable.complete_run($1, $2, $3)";
        if let Err(e) = sqlx::query(query)
            .bind(queue_name)
            .bind(run_id)
            .bind(&result)
            .execute(pool)
            .await
        {
            tracing::error!("Failed to complete run: {}", e);
        }
    }

    async fn fail_run(pool: &PgPool, queue_name: &str, run_id: Uuid, error: &TaskError) {
        let error_json = serialize_task_error(error);
        let query = "SELECT durable.fail_run($1, $2, $3, $4)";
        if let Err(e) = sqlx::query(query)
            .bind(queue_name)
            .bind(run_id)
            .bind(&error_json)
            .bind(None::<DateTime<Utc>>)
            .execute(pool)
            .await
        {
            tracing::error!("Failed to fail run: {}", e);
        }
    }
}
