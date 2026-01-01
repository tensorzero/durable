use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::{ControlFlow, TaskError, TaskResult};
use crate::task::{Task, TaskRegistry};
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
/// # Type Parameter
///
/// * `State` - The application state type. This allows [`spawn`](Self::spawn) to
///   automatically infer the correct state type for child tasks.
///
/// # Public Fields
///
/// - `task_id` - Unique identifier for this task (use as idempotency key)
/// - `run_id` - Identifier for the current execution attempt
/// - `attempt` - Current attempt number (starts at 1)
pub struct TaskContext<State = ()>
where
    State: Clone + Send + Sync + 'static,
{
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
    claim_timeout: Duration,

    state: State,

    /// Checkpoint cache: loaded on creation, updated on writes.
    checkpoint_cache: HashMap<String, JsonValue>,

    /// Step name deduplication: tracks how many times each base name
    /// has been used. Generates: "name", "name#2", "name#3", etc.
    step_counters: HashMap<String, u32>,

    /// Notifies the worker when the lease is extended via step() or heartbeat().
    lease_extender: LeaseExtender,

    /// Task registry for validating spawn_by_name calls.
    registry: Arc<RwLock<TaskRegistry<State>>>,

    /// Default max attempts for subtasks spawned via spawn_by_name.
    default_max_attempts: u32,
}

/// Validate that a user-provided step name doesn't use reserved prefix.
fn validate_user_name(name: &str) -> TaskResult<()> {
    if name.starts_with('$') {
        return Err(TaskError::Validation {
            message: "Step names cannot start with '$' (reserved for internal use)".to_string(),
        });
    }
    Ok(())
}

impl<State> TaskContext<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Create a new TaskContext. Called by the worker before executing a task.
    /// Loads all existing checkpoints into the cache.
    pub(crate) async fn create(
        pool: PgPool,
        queue_name: String,
        task: ClaimedTask,
        claim_timeout: Duration,
        lease_extender: LeaseExtender,
        registry: Arc<RwLock<TaskRegistry<State>>>,
        state: State,
        default_max_attempts: u32,
    ) -> Result<Self, sqlx::Error> {
        // Load all checkpoints for this task into cache
        let checkpoints: Vec<CheckpointRow> = sqlx::query_as(
            "SELECT checkpoint_name, state, owner_run_id, updated_at
             FROM durable.get_task_checkpoint_states($1, $2)",
        )
        .bind(&queue_name)
        .bind(task.task_id)
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
            registry,
            state,
            default_max_attempts,
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
    /// * `params` - Data to pass to the provided `f` closure. This data will
    ///    be serialized, hashed, and included as part of the internal checkpoint name.
    ///    As a result, a previously-completed run if 'step' will only be re-used if
    ///    both `name` *AND* `params` are the same.
    ///    Your `Serialize` implementation for `params` must include *all* of the data
    ///    accessible through `params` (e.g. you should not use `#[serde(skip)]`).
    ///    An incorrect `Serialize` implementation will cause a previous result to
    ///    be incorrectly re-used, even though the second 'step' call was invoked with different
    ///    'params' than the cached result.
    /// * `f` - Async closure to execute. Must return a JSON-serializable result.
    ///    This closure receives two arguments - the `params` argument described above,
    ///    and the current 'state' of the task.
    ///
    ///    Attempting to capture any surrounding variables in `f` will cause
    ///    a compile-time error. Instead, you must pass in any needed state through
    ///    the `params` argument.
    ///
    ///
    /// # Errors
    ///
    /// * `TaskError::Control(Cancelled)` - Task was cancelled
    /// * `TaskError::Failed` - Step execution or serialization failed
    ///
    /// # Example
    ///
    /// ```ignore
    /// let payment_id = ctx.step("charge-payment", ctx.task_id, |task_id, _state| async {
    ///     let idempotency_key = format!("{}:charge", task_id);
    ///     stripe::charge(amount, &idempotency_key).await
    /// }).await?;
    /// ```
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.step",
            skip(self, f, params),
            fields(task_id = %self.task_id, step_name = %base_name)
        )
    )]
    pub async fn step<T, P, Fut>(
        &mut self,
        base_name: &str,
        params: P,
        f: fn(P, State) -> Fut,
    ) -> TaskResult<T>
    where
        P: Serialize,
        T: Serialize + DeserializeOwned + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
        State: Clone,
    {
        validate_user_name(base_name)?;
        let checkpoint_name = self.get_checkpoint_name(base_name, &params)?;

        // Return cached value if step was already completed
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            return Ok(serde_json::from_value(cached.clone())?);
        }

        // Execute the step
        let result = f(params, self.state.clone()).await?;

        // Persist checkpoint (also extends claim lease)
        #[cfg(feature = "telemetry")]
        let checkpoint_start = std::time::Instant::now();

        self.persist_checkpoint(&checkpoint_name, &result).await?;

        #[cfg(feature = "telemetry")]
        {
            let duration = checkpoint_start.elapsed().as_secs_f64();
            crate::telemetry::record_checkpoint_duration(
                &self.queue_name,
                &self.task.task_name,
                duration,
            );
        }

        Ok(result)
    }

    fn get_params_hash<P: Serialize>(&self, data: &P) -> TaskResult<String> {
        let mut json_value = serde_json::to_value(data)?;
        json_value.sort_all_objects();
        let hash = blake3::hash(json_value.to_string().as_bytes());
        Ok(hash.to_string())
    }

    /// Generate unique checkpoint name, handling duplicate step names
    /// The provided 'data' is hashed and concatenated to the base name
    fn get_checkpoint_name<P: Serialize>(
        &mut self,
        base_name: &str,
        data: &P,
    ) -> TaskResult<String> {
        let hash = self.get_params_hash(data)?;
        let name_with_hash = format!("{base_name}-{hash}");
        let count = self
            .step_counters
            .entry(name_with_hash.clone())
            .or_insert(0);
        *count += 1;

        if *count == 1 {
            Ok(name_with_hash)
        } else {
            Ok(format!("{name_with_hash}#{count}"))
        }
    }

    /// Get checkpoint name without incrementing counter.
    /// Use for operations where the base_name is already unique (e.g., contains a UUID).
    fn get_checkpoint_name_no_increment<P: Serialize>(
        &self,
        base_name: &str,
        data: &P,
    ) -> TaskResult<String> {
        let hash = self.get_params_hash(data)?;
        Ok(format!("{base_name}-{hash}"))
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
            .bind(self.claim_timeout.as_secs() as i32)
            .execute(&self.pool)
            .await?;

        self.checkpoint_cache.insert(name.to_string(), state_json);

        // Notify worker that lease was extended so it can reset timers
        self.lease_extender.notify(self.claim_timeout);

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
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.sleep_for",
            skip(self),
            fields(task_id = %self.task_id, duration_ms = duration.as_millis() as u64)
        )
    )]
    pub async fn sleep_for(&mut self, name: &str, duration: std::time::Duration) -> TaskResult<()> {
        validate_user_name(name)?;
        let checkpoint_name = self.get_checkpoint_name(name, &())?;
        let duration_ms = duration.as_millis() as i64;

        let (needs_suspend,): (bool,) =
            sqlx::query_as("SELECT durable.sleep_for($1, $2, $3, $4, $5)")
                .bind(&self.queue_name)
                .bind(self.task_id)
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
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.await_event",
            skip(self, timeout),
            fields(task_id = %self.task_id, event_name = %event_name)
        )
    )]
    pub async fn await_event<T: DeserializeOwned>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> TaskResult<T> {
        validate_user_name(event_name)?;
        let step_name = format!("$awaitEvent:{event_name}");
        let checkpoint_name = self.get_checkpoint_name(&step_name, &())?;

        // Check cache for already-received event
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            return Ok(serde_json::from_value(cached.clone())?);
        }

        // Check if we were woken by this event but it timed out (null payload)
        if self.task.wake_event.as_deref() == Some(event_name) && self.task.event_payload.is_none()
        {
            return Err(TaskError::Timeout {
                step_name: event_name.to_string(),
            });
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
    /// updates the payload (last write wins). Tasks waiting for this event
    /// are woken with the payload at the time of the write that woke them;
    /// subsequent writes do not propagate to already-woken tasks.
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.emit_event",
            skip(self, payload),
            fields(task_id = %self.task_id, event_name = %event_name)
        )
    )]
    pub async fn emit_event<T: Serialize>(&self, event_name: &str, payload: &T) -> TaskResult<()> {
        if event_name.is_empty() {
            return Err(TaskError::Validation {
                message: "event_name must be non-empty".to_string(),
            });
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
    ///   Must be at least 1 second.
    ///
    /// # Errors
    /// - Returns `TaskError::Validation` if duration is less than 1 second.
    /// - Returns `TaskError::Control(Cancelled)` if the task was cancelled.
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.heartbeat",
            skip(self),
            fields(task_id = %self.task_id)
        )
    )]
    pub async fn heartbeat(&self, duration: Option<std::time::Duration>) -> TaskResult<()> {
        let extend_by = duration.unwrap_or(self.claim_timeout);

        if extend_by < std::time::Duration::from_secs(1) {
            return Err(TaskError::Validation {
                message: "heartbeat duration must be at least 1 second".to_string(),
            });
        }

        let query = "SELECT durable.extend_claim($1, $2, $3)";
        sqlx::query(query)
            .bind(&self.queue_name)
            .bind(self.run_id)
            .bind(extend_by.as_secs() as i32)
            .execute(&self.pool)
            .await?;

        // Notify worker that lease was extended so it can reset timers
        self.lease_extender.notify(extend_by);

        Ok(())
    }

    /// Generate a durable random value in [0, 1).
    ///
    /// The value is checkpointed - retries will return the same value.
    /// Each call generates a new checkpoint with auto-incremented name.
    pub async fn rand(&mut self) -> TaskResult<f64> {
        let checkpoint_name = self.get_checkpoint_name("$rand", &())?;
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
        let checkpoint_name = self.get_checkpoint_name("$now", &())?;
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let stored: String = serde_json::from_value(cached.clone())?;
            return Ok(DateTime::parse_from_rfc3339(&stored)
                .map_err(|e| TaskError::Validation {
                    message: format!("Invalid stored time: {e}"),
                })?
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
        let checkpoint_name = self.get_checkpoint_name("$uuid7", &())?;
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
    /// let r1: ItemResult = ctx.join(h1).await?;
    /// let r2: ItemResult = ctx.join(h2).await?;
    /// ```
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.spawn",
            skip(self, params, options),
            fields(task_id = %self.task_id, subtask_name = %T::name())
        )
    )]
    pub async fn spawn<T>(
        &mut self,
        name: &str,
        params: T::Params,
        options: crate::SpawnOptions,
    ) -> TaskResult<TaskHandle<T::Output>>
    where
        T: Task<State>,
    {
        let params_json = serde_json::to_value(&params)?;
        self.spawn_by_name(name, &T::name(), params_json, options)
            .await
    }

    /// Spawn a subtask by task name (dynamic version).
    ///
    /// This is similar to [`spawn`](Self::spawn) but works with task names
    /// instead of requiring a concrete type. Useful for dynamic task invocation
    /// where the task type isn't known at compile time.
    ///
    /// The spawn is checkpointed - if this task retries after spawning, the
    /// same subtask ID is returned without spawning a duplicate.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique name for this spawn operation (used for checkpointing)
    /// * `task_name` - The registered name of the task to spawn
    /// * `params` - JSON parameters to pass to the task
    /// * `options` - Spawn options (max_attempts, priority, etc.)
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] that can be passed to [`join`](Self::join) to wait for
    /// the result. The output type `T` must match the actual task's output type.
    ///
    /// # Errors
    ///
    /// * `TaskError::Failed` - If the task name is not registered in the registry
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Spawn a task by name
    /// let handle: TaskHandle<ProcessResult> = ctx.spawn_by_name(
    ///     "process-item",
    ///     "process-item-task",
    ///     serde_json::json!({ "item_id": 123 }),
    ///     Default::default(),
    /// ).await?;
    ///
    /// // Wait for result
    /// let result: ProcessResult = ctx.join(handle).await?;
    /// ```
    pub async fn spawn_by_name<T: DeserializeOwned>(
        &mut self,
        name: &str,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> TaskResult<TaskHandle<T>> {
        validate_user_name(name)?;
        let checkpoint_name = self.get_checkpoint_name(&format!("$spawn:{name}"), &params)?;

        // Validate headers don't use reserved prefix
        if let Some(ref headers) = options.headers {
            for key in headers.keys() {
                if key.starts_with("durable::") {
                    return Err(TaskError::Validation {
                        message: format!(
                            "Header key '{}' uses reserved prefix 'durable::'. User headers cannot start with 'durable::'.",
                            key
                        ),
                    });
                }
            }
        }

        // Return cached task_id if already spawned
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let task_id: Uuid = serde_json::from_value(cached.clone())?;
            return Ok(TaskHandle::new(task_id));
        }

        // Validate that the task is registered
        {
            let registry = self.registry.read().await;
            if !registry.contains_key(task_name) {
                return Err(TaskError::Validation {
                    message: format!(
                        "Unknown task: {}. Task must be registered before spawning.",
                        task_name
                    ),
                });
            }
        }

        // Apply default max_attempts if not set
        let options = SpawnOptions {
            max_attempts: Some(options.max_attempts.unwrap_or(self.default_max_attempts)),
            ..options
        };

        // Build options JSON, merging user options with parent_task_id
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
        .bind(task_name)
        .bind(&params)
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
    /// let result: ComputeResult = ctx.join(handle).await?;
    /// ```
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.task.join",
            skip(self, handle),
            fields(task_id = %self.task_id, child_task_id = %handle.task_id)
        )
    )]
    pub async fn join<T: DeserializeOwned>(&mut self, handle: TaskHandle<T>) -> TaskResult<T> {
        let event_name = format!("$child:{}", handle.task_id);

        // await_event handles checkpointing and suspension
        // We use the internal event name which starts with $ so we need to bypass validation
        let step_name = format!("$awaitEvent:{event_name}");
        let checkpoint_name = self.get_checkpoint_name_no_increment(&step_name, &())?;

        // Check cache for already-received event
        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let payload: ChildCompletePayload = serde_json::from_value(cached.clone())?;
            return Self::process_child_payload(&step_name, payload);
        }

        // Check if we were woken by this event but it timed out (null payload)
        if self.task.wake_event.as_deref() == Some(&event_name) && self.task.event_payload.is_none()
        {
            return Err(TaskError::Timeout {
                step_name: step_name.to_string(),
            });
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
        Self::process_child_payload(&step_name, payload)
    }

    /// Process the child completion payload and return the appropriate result.
    fn process_child_payload<T: DeserializeOwned>(
        step_name: &str,
        payload: ChildCompletePayload,
    ) -> TaskResult<T> {
        match payload.status {
            ChildStatus::Completed => {
                let result = payload.result.ok_or_else(|| TaskError::Validation {
                    message: "Child completed but no result available".to_string(),
                })?;
                Ok(serde_json::from_value(result)?)
            }
            ChildStatus::Failed => {
                let message = payload
                    .error
                    .and_then(|e| e.get("message").and_then(|m| m.as_str()).map(String::from))
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(TaskError::ChildFailed {
                    step_name: step_name.to_string(),
                    message,
                })
            }
            ChildStatus::Cancelled => Err(TaskError::ChildCancelled {
                step_name: step_name.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::{Durable, MIGRATOR};

    // Note that this is a 'unit' test in order to call private methods, but it still needs Postgres to be running
    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_checkpoint_name_hashing(pool: PgPool) {
        let client = Durable::builder()
            .pool(pool.clone())
            .queue_name("my_test_queue")
            .build()
            .await
            .expect("Failed to create Durable client");
        client.create_queue(None).await.unwrap();
        let mut ctx = TaskContext::create(
            pool,
            "my_test_queue".to_string(),
            ClaimedTask {
                task_id: Uuid::now_v7(),
                run_id: Uuid::now_v7(),
                attempt: 1,
                task_name: "my_test_task".to_string(),
                params: JsonValue::Null,
                retry_strategy: None,
                max_attempts: None,
                event_payload: None,
                headers: None,
                wake_event: None,
            },
            Duration::from_secs(10),
            LeaseExtender::dummy_for_tests(),
            Arc::new(RwLock::new(TaskRegistry::new())),
            (),
            5, // default_max_attempts
        )
        .await
        .unwrap();

        let first_name = ctx
            .get_checkpoint_name("my_step", &"my_string_data")
            .unwrap();

        assert_eq!(
            first_name,
            "my_step-f3c75b4ef2b1412ee609c4a60004408b4f9d168ddc48e36d80418617c00fbc48"
        );

        // The hash should be the same, so just an ID should get appended
        let second_name = ctx
            .get_checkpoint_name("my_step", &"my_string_data")
            .unwrap();
        assert_eq!(
            second_name,
            format!("my_step-f3c75b4ef2b1412ee609c4a60004408b4f9d168ddc48e36d80418617c00fbc48#2")
        );

        let first_map = HashMap::from([
            ("first_key".to_string(), "first_value".to_string()),
            ("second_key".to_string(), "second_value".to_string()),
        ]);

        let swapped_map = HashMap::from([
            ("second_key".to_string(), "second_value".to_string()),
            ("first_key".to_string(), "first_value".to_string()),
        ]);

        let first_map_name = ctx.get_checkpoint_name("my_map_step", &first_map).unwrap();
        let second_map_name = ctx
            .get_checkpoint_name("my_map_step", &swapped_map)
            .unwrap();
        // The two maps should end up with the same hash, since we sort the keys
        // after serializing to a JSON value
        assert_eq!(second_map_name, format!("{first_map_name}#2"));

        let distinct_map = HashMap::from([
            ("first_key".to_string(), "first_value".to_string()),
            ("second_key".to_string(), "new_second_value".to_string()),
        ]);

        let distinct_map_name = ctx
            .get_checkpoint_name("my_map_step", &distinct_map)
            .unwrap();
        assert_eq!(
            distinct_map_name,
            format!("my_map_step-3b522428f7afb6db8bfadc8f32e02922e45a479ba417fe218f639d72e3b22021")
        );
    }
}
