use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::error::{ControlFlow, TaskError, TaskResult};
use crate::task::Task;
use crate::types::{
    AwaitEventResult, CheckpointRow, ChildCompletePayload, ChildStatus, ClaimedTask, SpawnOptions,
    SpawnResultRow, TaskHandle,
};
use crate::worker::LeaseExtender;

/// Context provided to task execution, enabling checkpointing and suspension.
///
/// The `TaskContext` is the primary interface for interacting with the durable
/// execution system from within a task. It provides:
///
/// - **Checkpointing** via [`step`](Self::step) - Execute operations that are cached
///   and not re-executed on retry
/// - **Sleeping** via [`sleep_for`](Self::sleep_for) - Suspend the task for a duration
/// - **Events** via [`await_event`](Self::await_event) and [`emit_event`](Self::emit_event) -
///   Wait for or emit events to coordinate between tasks
/// - **Lease management** via [`heartbeat`](Self::heartbeat) - Extend the task lease
///   for long-running operations
///
/// # Public Fields
///
/// - `task_id` - Unique identifier for this task (use as idempotency key)
/// - `run_id` - Identifier for the current execution attempt
/// - `attempt` - Current attempt number (starts at 1)
pub struct TaskContext {
    /// Unique identifier for this task. Use this as an idempotency key for
    /// external API calls to achieve "exactly-once" semantics.
    pub task_id: Uuid,
    /// Identifier for the current run (attempt).
    pub run_id: Uuid,
    /// Current attempt number (starts at 1).
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

    /// Notifies the worker when the lease is extended via step() or heartbeat().
    lease_extender: LeaseExtender,
}

/// Validate that a user-provided step name doesn't use reserved prefix.
fn validate_user_name(name: &str) -> TaskResult<()> {
    if name.starts_with('$') {
        return Err(TaskError::Failed(anyhow::anyhow!(
            "Step names cannot start with '$' (reserved for internal use)"
        )));
    }
    Ok(())
}

impl TaskContext {
    /// Create a new TaskContext. Called by the worker before executing a task.
    /// Loads all existing checkpoints into the cache.
    pub(crate) async fn create(
        pool: PgPool,
        queue_name: String,
        task: ClaimedTask,
        claim_timeout: u64,
        lease_extender: LeaseExtender,
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
            lease_extender,
        })
    }

    /// Execute a checkpointed step.
    ///
    /// If the step was already completed in a previous run, returns the
    /// cached result without re-executing the closure. This provides
    /// "exactly-once" semantics for side effects within the step.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique name for this step. If called multiple times with
    ///   the same name, auto-increments: "name", "name#2", "name#3"
    /// * `f` - Async closure to execute. Must return a JSON-serializable result.
    ///
    /// # Errors
    ///
    /// * `TaskError::Control(Cancelled)` - Task was cancelled
    /// * `TaskError::Failed` - Step execution or serialization failed
    ///
    /// # Example
    ///
    /// ```ignore
    /// let payment_id = ctx.step("charge-payment", || async {
    ///     let idempotency_key = format!("{}:charge", ctx.task_id);
    ///     stripe::charge(amount, &idempotency_key).await
    /// }).await?;
    /// ```
    pub async fn step<T, F, Fut>(&mut self, name: &str, f: F) -> TaskResult<T>
    where
        T: Serialize + DeserializeOwned + Send,
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
    {
        validate_user_name(name)?;
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

        // Notify worker that lease was extended so it can reset timers
        self.lease_extender
            .notify(Duration::from_secs(self.claim_timeout));

        Ok(())
    }

    /// Suspend the task for a duration.
    ///
    /// The task will be rescheduled to run after the duration elapses.
    /// This is checkpointed - if the task is retried, the original wake
    /// time is preserved (won't extend the sleep on retry).
    ///
    /// Wake time is computed using the database clock to ensure consistency
    /// with the scheduler and enable deterministic testing via `durable.fake_now`.
    pub async fn sleep_for(&mut self, name: &str, duration: std::time::Duration) -> TaskResult<()> {
        validate_user_name(name)?;
        let checkpoint_name = self.get_checkpoint_name(name);
        let duration_ms = duration.as_millis() as i64;

        let (needs_suspend,): (bool,) = sqlx::query_as("SELECT durable.sleep_for($1, $2, $3, $4)")
            .bind(&self.queue_name)
            .bind(self.run_id)
            .bind(&checkpoint_name)
            .bind(duration_ms)
            .fetch_one(&self.pool)
            .await?;

        if needs_suspend {
            return Err(TaskError::Control(ControlFlow::Suspend));
        }
        Ok(())
    }

    /// Wait for an event by name. Returns the event payload when it arrives.
    ///
    /// # Behavior
    ///
    /// - If the event has already been emitted, returns immediately with payload
    /// - Otherwise, suspends the task until the event arrives
    /// - Events are cached like checkpoints - receiving the same event twice
    ///   returns the cached payload
    /// - If timeout is specified and exceeded, returns a timeout error
    ///
    /// # Arguments
    ///
    /// * `event_name` - The event to wait for (e.g., "shipment.packed:ORDER-123")
    /// * `timeout` - Optional timeout duration
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Wait for a shipment event with 7-day timeout
    /// let shipment: ShipmentEvent = ctx.await_event(
    ///     &format!("packed:{}", order_id),
    ///     Some(Duration::from_secs(7 * 24 * 3600)),
    /// ).await?;
    /// ```
    pub async fn await_event<T: DeserializeOwned>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> TaskResult<T> {
        validate_user_name(event_name)?;
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

        // Notify worker that lease was extended so it can reset timers
        self.lease_extender
            .notify(Duration::from_secs(extend_by as u64));

        Ok(())
    }

    /// Generate a durable random value in [0, 1).
    ///
    /// The value is checkpointed - retries will return the same value.
    /// Each call generates a new checkpoint with auto-incremented name.
    pub async fn rand(&mut self) -> TaskResult<f64> {
        let checkpoint_name = self.get_checkpoint_name("$rand");
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            return Ok(serde_json::from_value(cached.clone())?);
        }
        let value: f64 = rand::random();
        self.persist_checkpoint(&checkpoint_name, &value).await?;
        Ok(value)
    }

    /// Get the current time as a durable checkpoint.
    ///
    /// The timestamp is checkpointed - retries will return the same timestamp.
    /// Each call generates a new checkpoint with auto-incremented name.
    pub async fn now(&mut self) -> TaskResult<DateTime<Utc>> {
        let checkpoint_name = self.get_checkpoint_name("$now");
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let stored: String = serde_json::from_value(cached.clone())?;
            return Ok(DateTime::parse_from_rfc3339(&stored)
                .map_err(|e| TaskError::Failed(anyhow::anyhow!("Invalid stored time: {e}")))?
                .with_timezone(&Utc));
        }
        let value = Utc::now();
        self.persist_checkpoint(&checkpoint_name, &value.to_rfc3339())
            .await?;
        Ok(value)
    }

    /// Generate a durable UUIDv7.
    ///
    /// The UUID is checkpointed - retries will return the same UUID.
    /// Each call generates a new checkpoint with auto-incremented name.
    pub async fn uuid7(&mut self) -> TaskResult<Uuid> {
        let checkpoint_name = self.get_checkpoint_name("$uuid7");
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            return Ok(serde_json::from_value(cached.clone())?);
        }
        let value = Uuid::now_v7();
        self.persist_checkpoint(&checkpoint_name, &value).await?;
        Ok(value)
    }

    /// Spawn a subtask on the same queue.
    ///
    /// The subtask runs independently and can be awaited using [`join`](Self::join).
    /// The spawn is checkpointed - if the parent task retries, the same subtask
    /// handle is returned (the subtask won't be spawned again).
    ///
    /// When the parent task completes, fails, or is cancelled, all of its
    /// subtasks are automatically cancelled (cascade cancellation).
    ///
    /// # Arguments
    ///
    /// * `name` - Unique name for this spawn operation (used for checkpointing)
    /// * `params` - Parameters to pass to the subtask
    /// * `options` - Spawn options (retry strategy, max attempts, etc.)
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] that can be passed to [`join`](Self::join) to wait for
    /// the subtask to complete and retrieve its result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Spawn two subtasks
    /// let h1 = ctx.spawn::<ProcessItem>("item-1", Item { id: 1 }, Default::default()).await?;
    /// let h2 = ctx.spawn::<ProcessItem>("item-2", Item { id: 2 }, SpawnOptions {
    ///     max_attempts: Some(3),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// // Do work while subtasks run...
    ///
    /// // Wait for results
    /// let r1: ItemResult = ctx.join("item-1", h1).await?;
    /// let r2: ItemResult = ctx.join("item-2", h2).await?;
    /// ```
    pub async fn spawn<T: Task>(
        &mut self,
        name: &str,
        params: T::Params,
        options: crate::SpawnOptions,
    ) -> TaskResult<TaskHandle<T::Output>> {
        validate_user_name(name)?;
        let checkpoint_name = self.get_checkpoint_name(&format!("$spawn:{name}"));

        // Return cached task_id if already spawned
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let task_id: Uuid = serde_json::from_value(cached.clone())?;
            return Ok(TaskHandle::new(task_id));
        }

        // Build options JSON, merging user options with parent_task_id
        let params_json = serde_json::to_value(&params)?;
        #[derive(Serialize)]
        struct SubtaskOptions<'a> {
            parent_task_id: Uuid,
            #[serde(flatten)]
            options: &'a SpawnOptions,
        }
        let options_json = serde_json::to_value(SubtaskOptions {
            parent_task_id: self.task_id,
            options: &options,
        })?;

        let row: SpawnResultRow = sqlx::query_as(
            "SELECT task_id, run_id, attempt FROM durable.spawn_task($1, $2, $3, $4)",
        )
        .bind(&self.queue_name)
        .bind(T::NAME)
        .bind(&params_json)
        .bind(&options_json)
        .fetch_one(&self.pool)
        .await?;

        // Checkpoint the spawn
        self.persist_checkpoint(&checkpoint_name, &row.task_id)
            .await?;

        Ok(TaskHandle::new(row.task_id))
    }

    /// Wait for a subtask to complete and return its result.
    ///
    /// If the subtask has already completed, returns immediately with the
    /// cached result. Otherwise, suspends the parent task until the subtask
    /// finishes.
    ///
    /// The join is checkpointed - if the parent task retries after a successful
    /// join, the cached result is returned without waiting.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique name for this join operation (used for checkpointing)
    /// * `handle` - The [`TaskHandle`] returned by [`spawn`](Self::spawn)
    ///
    /// # Errors
    ///
    /// * `TaskError::Failed` - If the subtask failed (with the subtask's error message)
    /// * `TaskError::Failed` - If the subtask was cancelled
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = ctx.spawn::<ComputeTask>("compute", params).await?;
    /// // ... do other work ...
    /// let result: ComputeResult = ctx.join("compute", handle).await?;
    /// ```
    pub async fn join<T: DeserializeOwned>(
        &mut self,
        name: &str,
        handle: TaskHandle<T>,
    ) -> TaskResult<T> {
        validate_user_name(name)?;
        let event_name = format!("$child:{}", handle.task_id);

        // await_event handles checkpointing and suspension
        // We use the internal event name which starts with $ so we need to bypass validation
        let step_name = format!("$awaitEvent:{event_name}");
        let checkpoint_name = self.get_checkpoint_name(&step_name);

        // Check cache for already-received event
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let payload: ChildCompletePayload = serde_json::from_value(cached.clone())?;
            return Self::process_child_payload(payload);
        }

        // Check if we were woken by this event but it timed out (null payload)
        if self.task.wake_event.as_deref() == Some(&event_name) && self.task.event_payload.is_none()
        {
            return Err(TaskError::Failed(anyhow::anyhow!(
                "Timed out waiting for child task to complete"
            )));
        }

        // Call await_event stored procedure (no timeout for join - we wait indefinitely)
        let query = "SELECT should_suspend, payload
             FROM durable.await_event($1, $2, $3, $4, $5, $6)";

        let result: AwaitEventResult = sqlx::query_as(query)
            .bind(&self.queue_name)
            .bind(self.task_id)
            .bind(self.run_id)
            .bind(&checkpoint_name)
            .bind(&event_name)
            .bind(None::<i32>) // No timeout
            .fetch_one(&self.pool)
            .await?;

        if result.should_suspend {
            return Err(TaskError::Control(ControlFlow::Suspend));
        }

        // Event arrived - parse and return
        let payload_json = result.payload.unwrap_or(JsonValue::Null);
        self.checkpoint_cache
            .insert(checkpoint_name, payload_json.clone());

        let payload: ChildCompletePayload = serde_json::from_value(payload_json)?;
        Self::process_child_payload(payload)
    }

    /// Process the child completion payload and return the appropriate result.
    fn process_child_payload<T: DeserializeOwned>(payload: ChildCompletePayload) -> TaskResult<T> {
        match payload.status {
            ChildStatus::Completed => {
                let result = payload.result.ok_or_else(|| {
                    TaskError::Failed(anyhow::anyhow!("Child completed but no result available"))
                })?;
                Ok(serde_json::from_value(result)?)
            }
            ChildStatus::Failed => {
                let msg = payload
                    .error
                    .and_then(|e| e.get("message").and_then(|m| m.as_str()).map(String::from))
                    .unwrap_or_else(|| "Child task failed".to_string());
                Err(TaskError::Failed(anyhow::anyhow!(
                    "Child task failed: {msg}",
                )))
            }
            ChildStatus::Cancelled => Err(TaskError::Failed(anyhow::anyhow!(
                "Child task was cancelled"
            ))),
        }
    }
}
