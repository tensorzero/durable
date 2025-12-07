use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};
use tokio::time::{Instant, sleep, sleep_until};
use uuid::Uuid;

use crate::context::TaskContext;
use crate::error::{ControlFlow, TaskError, serialize_error};
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
    pub(crate) async fn start(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry>>,
        options: WorkerOptions,
    ) -> Self {
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

    async fn run_loop(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry>>,
        options: WorkerOptions,
        worker_id: String,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let concurrency = options.concurrency;
        let batch_size = options.batch_size.unwrap_or(concurrency);
        let claim_timeout = options.claim_timeout;
        let poll_interval = std::time::Duration::from_secs_f64(options.poll_interval);
        let fatal_on_lease_timeout = options.fatal_on_lease_timeout;

        // Semaphore limits concurrent task execution
        let semaphore = Arc::new(Semaphore::new(concurrency));

        // Channel for tracking task completion (for graceful shutdown)
        let (done_tx, mut done_rx) = mpsc::channel::<()>(concurrency);

        loop {
            tokio::select! {
                // Shutdown signal received
                _ = shutdown_rx.recv() => {
                    tracing::info!("Worker shutting down, waiting for in-flight tasks...");
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

                        tokio::spawn(async move {
                            Self::execute_task(
                                pool,
                                queue_name,
                                registry,
                                task,
                                claim_timeout,
                                fatal_on_lease_timeout,
                            ).await;

                            drop(permit);
                            let _ = done_tx.send(()).await;
                        });
                    }
                }
            }
        }
    }

    async fn claim_tasks(
        pool: &PgPool,
        queue_name: &str,
        worker_id: &str,
        claim_timeout: u64,
        count: usize,
    ) -> anyhow::Result<Vec<ClaimedTask>> {
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

        rows.into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn execute_task(
        pool: PgPool,
        queue_name: String,
        registry: Arc<RwLock<TaskRegistry>>,
        task: ClaimedTask,
        claim_timeout: u64,
        fatal_on_lease_timeout: bool,
    ) {
        let task_label = format!("{} ({})", task.task_name, task.task_id);
        let task_id = task.task_id;
        let run_id = task.run_id;
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
        let handler = match registry.get(&task.task_name) {
            Some(h) => h.clone(),
            None => {
                tracing::error!("Unknown task: {}", task.task_name);
                Self::fail_run(
                    &pool,
                    &queue_name,
                    task.run_id,
                    &anyhow::anyhow!("Unknown task: {}", task.task_name),
                )
                .await;
                return;
            }
        };
        drop(registry);

        // Execute task with timeout enforcement
        let task_handle = tokio::spawn({
            let params = task.params.clone();
            async move { handler.execute(params, ctx).await }
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
                let mut channel_open = true;

                loop {
                    let warn_at = deadline + warn_duration;
                    let fatal_at = deadline + fatal_duration;

                    // If channel is closed, just wait for timeout without checking channel
                    if !channel_open {
                        tokio::select! {
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
                        continue;
                    }

                    tokio::select! {
                        biased; // Check channel first to prioritize resets

                        msg = lease_rx.recv() => {
                            if let Some(extension) = msg {
                                // Lease extended - reset deadline and warning state
                                warn_duration = extension;
                                fatal_duration = extension * 2;
                                deadline = Instant::now();
                                warn_fired = false;
                            } else {
                                // Channel closed - task might be finishing, but keep timing
                                // in case it's actually stuck. The outer select will abort
                                // us when task completes normally.
                                channel_open = false;
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
                        Some(Err(TaskError::Failed(anyhow::anyhow!("Task panicked: {e}"))))
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

        match result {
            Ok(output) => {
                Self::complete_run(&pool, &queue_name, task.run_id, output).await;
            }
            Err(TaskError::Control(ControlFlow::Suspend)) => {
                // Task suspended - do nothing, scheduler will resume it
                tracing::debug!("Task {} suspended", task_label);
            }
            Err(TaskError::Control(ControlFlow::Cancelled)) => {
                // Task cancelled - do nothing
                tracing::info!("Task {} was cancelled", task_label);
            }
            Err(TaskError::Failed(e)) => {
                tracing::error!("Task {} failed: {}", task_label, e);
                Self::fail_run(&pool, &queue_name, task.run_id, &e).await;
            }
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

    async fn fail_run(pool: &PgPool, queue_name: &str, run_id: Uuid, error: &anyhow::Error) {
        let error_json = serialize_error(error);
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
