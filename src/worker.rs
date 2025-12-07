use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};
use tokio::time::sleep;
use uuid::Uuid;

use crate::context::TaskContext;
use crate::error::{ControlFlow, TaskError, serialize_error};
use crate::task::TaskRegistry;
use crate::types::{ClaimedTask, ClaimedTaskRow, WorkerOptions};

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
                    let available = semaphore.available_permits();
                    if available == 0 {
                        continue;
                    }

                    let to_claim = available.min(batch_size);

                    let tasks = match Self::claim_tasks(
                        &pool,
                        &queue_name,
                        &worker_id,
                        claim_timeout,
                        to_claim,
                    ).await {
                        Ok(tasks) => tasks,
                        Err(e) => {
                            tracing::error!("Failed to claim tasks: {}", e);
                            continue;
                        }
                    };

                    for task in tasks {
                        // Semaphore is never closed, so this cannot fail
                        let Ok(permit) = semaphore.clone().acquire_owned().await else {
                            break;
                        };
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

        // Warning timer: fires after claim_timeout
        let warn_handle = tokio::spawn({
            let task_label = task_label.clone();
            async move {
                sleep(std::time::Duration::from_secs(claim_timeout)).await;
                tracing::warn!(
                    "Task {} exceeded claim timeout of {}s",
                    task_label,
                    claim_timeout
                );
            }
        });

        // Fatal timer: fires after 2x claim_timeout (kills process)
        let fatal_handle = if fatal_on_lease_timeout {
            Some(tokio::spawn({
                let task_label = task_label.clone();
                async move {
                    sleep(std::time::Duration::from_secs(claim_timeout * 2)).await;
                    tracing::error!(
                        "Task {} exceeded claim timeout by 100%; terminating process",
                        task_label
                    );
                    std::process::exit(1);
                }
            }))
        } else {
            None
        };

        // Create task context
        let ctx = match TaskContext::create(
            pool.clone(),
            queue_name.clone(),
            task.clone(),
            claim_timeout,
        )
        .await
        {
            Ok(ctx) => ctx,
            Err(e) => {
                tracing::error!("Failed to create task context: {}", e);
                Self::fail_run(&pool, &queue_name, task.run_id, &e.into()).await;
                warn_handle.abort();
                if let Some(h) = fatal_handle {
                    h.abort();
                }
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
                warn_handle.abort();
                if let Some(h) = fatal_handle {
                    h.abort();
                }
                return;
            }
        };
        drop(registry);

        // Execute task
        let result = handler.execute(task.params.clone(), ctx).await;

        // Cancel timers
        warn_handle.abort();
        if let Some(h) = fatal_handle {
            h.abort();
        }

        // Handle result
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
