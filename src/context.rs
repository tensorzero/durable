use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::{ControlFlow, TaskError, TaskResult};
use crate::types::{AwaitEventResult, CheckpointRow, ClaimedTask};

/// Context provided to task execution, enabling checkpointing and suspension.
pub struct TaskContext {
    // Public fields - accessible to task code
    pub task_id: Uuid,
    pub run_id: Uuid,
    pub attempt: i32,

    // Internal state
    pool: PgPool,
    queue_name: String,
    #[allow(dead_code)]
    task: ClaimedTask,
    claim_timeout: u64,

    /// Checkpoint cache: loaded on creation, updated on writes.
    checkpoint_cache: HashMap<String, JsonValue>,

    /// Step name deduplication: tracks how many times each base name
    /// has been used. Generates: "name", "name#2", "name#3", etc.
    step_counters: HashMap<String, u32>,
}

impl TaskContext {
    /// Create a new TaskContext. Called by the worker before executing a task.
    /// Loads all existing checkpoints into the cache.
    pub(crate) async fn create(
        pool: PgPool,
        queue_name: String,
        task: ClaimedTask,
        claim_timeout: u64,
    ) -> Result<Self, sqlx::Error> {
        // Load all checkpoints for this task into cache
        let checkpoints: Vec<CheckpointRow> = sqlx::query_as(
            "SELECT checkpoint_name, state, status, owner_run_id, updated_at
             FROM durable.get_task_checkpoint_states($1, $2, $3)",
        )
        .bind(&queue_name)
        .bind(task.task_id)
        .bind(task.run_id)
        .fetch_all(&pool)
        .await?;

        let mut cache = HashMap::new();
        for row in checkpoints {
            cache.insert(row.checkpoint_name, row.state);
        }

        Ok(Self {
            task_id: task.task_id,
            run_id: task.run_id,
            attempt: task.attempt,
            pool,
            queue_name,
            task,
            claim_timeout,
            checkpoint_cache: cache,
            step_counters: HashMap::new(),
        })
    }

    /// Execute a checkpointed step.
    ///
    /// If the step was already completed in a previous run, returns the
    /// cached result without re-executing the closure. This provides
    /// "exactly-once" semantics for side effects within the step.
    ///
    /// # Arguments
    /// * `name` - Unique name for this step. If called multiple times with
    ///   the same name, auto-increments: "name", "name#2", "name#3"
    /// * `f` - Async closure to execute. Must return a JSON-serializable result.
    ///
    /// # Errors
    /// * `TaskError::Control(Cancelled)` - Task was cancelled
    /// * `TaskError::Failed` - Step execution or serialization failed
    pub async fn step<T, F, Fut>(&mut self, name: &str, f: F) -> TaskResult<T>
    where
        T: Serialize + DeserializeOwned + Send,
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
    {
        let checkpoint_name = self.get_checkpoint_name(name);

        // Return cached value if step was already completed
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            return Ok(serde_json::from_value(cached.clone())?);
        }

        // Execute the step
        let result = f().await?;

        // Persist checkpoint (also extends claim lease)
        self.persist_checkpoint(&checkpoint_name, &result).await?;

        Ok(result)
    }

    /// Generate unique checkpoint name, handling duplicate step names
    fn get_checkpoint_name(&mut self, base_name: &str) -> String {
        let count = self.step_counters.entry(base_name.to_string()).or_insert(0);
        *count += 1;

        if *count == 1 {
            base_name.to_string()
        } else {
            format!("{base_name}#{count}")
        }
    }

    /// Persist checkpoint to database and update cache.
    /// Also extends the claim lease to prevent timeout.
    async fn persist_checkpoint<T: Serialize>(&mut self, name: &str, value: &T) -> TaskResult<()> {
        let state_json = serde_json::to_value(value)?;

        // set_task_checkpoint_state also extends the claim
        let query = "SELECT durable.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)";
        sqlx::query(query)
            .bind(&self.queue_name)
            .bind(self.task_id)
            .bind(name)
            .bind(&state_json)
            .bind(self.run_id)
            .bind(self.claim_timeout as i32)
            .execute(&self.pool)
            .await?;

        self.checkpoint_cache.insert(name.to_string(), state_json);
        Ok(())
    }

    /// Suspend the task for a duration.
    ///
    /// The task will be rescheduled to run after the duration elapses.
    /// This is checkpointed - if the task is retried, the original wake
    /// time is preserved (won't extend the sleep on retry).
    pub async fn sleep_for(&mut self, name: &str, duration: std::time::Duration) -> TaskResult<()> {
        let wake_at = Utc::now()
            + chrono::Duration::from_std(duration)
                .map_err(|e| TaskError::Failed(anyhow::anyhow!("Invalid duration: {e}")))?;
        self.sleep_until(name, wake_at).await
    }

    /// Suspend the task until a specific time.
    ///
    /// The wake time is checkpointed, so code changes won't affect when
    /// the task actually resumes. If the time has already passed when
    /// this is called (e.g., on retry), returns immediately.
    pub async fn sleep_until(&mut self, name: &str, wake_at: DateTime<Utc>) -> TaskResult<()> {
        let checkpoint_name = self.get_checkpoint_name(name);

        // Check if we have a stored wake time from a previous run
        let actual_wake_at = if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let stored: String = serde_json::from_value(cached.clone())?;
            DateTime::parse_from_rfc3339(&stored)
                .map_err(|e| TaskError::Failed(anyhow::anyhow!("Invalid stored time: {e}")))?
                .with_timezone(&Utc)
        } else {
            // Store the wake time for future runs
            self.persist_checkpoint(&checkpoint_name, &wake_at.to_rfc3339())
                .await?;
            wake_at
        };

        // If wake time hasn't passed yet, suspend
        if Utc::now() < actual_wake_at {
            let query = "SELECT durable.schedule_run($1, $2, $3)";
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(self.run_id)
                .bind(actual_wake_at)
                .execute(&self.pool)
                .await?;

            return Err(TaskError::Control(ControlFlow::Suspend));
        }

        // Wake time has passed, continue execution
        Ok(())
    }

    /// Wait for an event by name. Returns the event payload when it arrives.
    ///
    /// # Behavior
    /// - If the event has already been emitted, returns immediately with payload
    /// - Otherwise, suspends the task until the event arrives
    /// - Events are cached like checkpoints - receiving the same event twice
    ///   returns the cached payload
    /// - If timeout is specified and exceeded, returns a timeout error
    ///
    /// # Arguments
    /// * `event_name` - The event to wait for (e.g., "shipment.packed:ORDER-123")
    /// * `timeout` - Optional timeout duration
    pub async fn await_event<T: DeserializeOwned>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> TaskResult<T> {
        let step_name = format!("$awaitEvent:{event_name}");
        let checkpoint_name = self.get_checkpoint_name(&step_name);

        // Check cache for already-received event
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            return Ok(serde_json::from_value(cached.clone())?);
        }

        // Check if we were woken by this event but it timed out (null payload)
        if self.task.wake_event.as_deref() == Some(event_name) && self.task.event_payload.is_none()
        {
            return Err(TaskError::Failed(anyhow::anyhow!(
                "Timed out waiting for event \"{event_name}\""
            )));
        }

        // Call await_event stored procedure
        let timeout_secs = timeout.map(|d| d.as_secs() as i32);

        let query = "SELECT should_suspend, payload
             FROM durable.await_event($1, $2, $3, $4, $5, $6)";

        let result: AwaitEventResult = sqlx::query_as(query)
            .bind(&self.queue_name)
            .bind(self.task_id)
            .bind(self.run_id)
            .bind(&checkpoint_name)
            .bind(event_name)
            .bind(timeout_secs)
            .fetch_one(&self.pool)
            .await?;

        if result.should_suspend {
            return Err(TaskError::Control(ControlFlow::Suspend));
        }

        // Event arrived - cache and return
        let payload = result.payload.unwrap_or(JsonValue::Null);
        self.checkpoint_cache
            .insert(checkpoint_name, payload.clone());
        Ok(serde_json::from_value(payload)?)
    }

    /// Emit an event to this task's queue.
    ///
    /// Events are deduplicated by name - emitting the same event twice
    /// has no effect (first payload wins). Any tasks waiting for this
    /// event will be woken up.
    pub async fn emit_event<T: Serialize>(&self, event_name: &str, payload: &T) -> TaskResult<()> {
        if event_name.is_empty() {
            return Err(TaskError::Failed(anyhow::anyhow!(
                "event_name must be non-empty"
            )));
        }

        let payload_json = serde_json::to_value(payload)?;
        let query = "SELECT durable.emit_event($1, $2, $3)";
        sqlx::query(query)
            .bind(&self.queue_name)
            .bind(event_name)
            .bind(&payload_json)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Extend the task's lease to prevent timeout.
    ///
    /// Use this for long-running operations that don't naturally checkpoint.
    /// Each `step()` call also extends the lease automatically.
    ///
    /// # Arguments
    /// * `duration` - Extension duration. Defaults to original claim_timeout.
    ///
    /// # Errors
    /// Returns `TaskError::Control(Cancelled)` if the task was cancelled.
    pub async fn heartbeat(&self, duration: Option<std::time::Duration>) -> TaskResult<()> {
        let extend_by = duration
            .map(|d| d.as_secs() as i32)
            .unwrap_or(self.claim_timeout as i32);

        let query = "SELECT durable.extend_claim($1, $2, $3)";
        sqlx::query(query)
            .bind(&self.queue_name)
            .bind(self.run_id)
            .bind(extend_by)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
