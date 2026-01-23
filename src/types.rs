use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_with::{DurationSeconds, serde_as};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use uuid::Uuid;

// Default value functions for RetryStrategy
fn default_base_delay() -> Duration {
    Duration::from_secs(5)
}
fn default_factor() -> f64 {
    2.0
}
fn default_max_backoff() -> Duration {
    Duration::from_secs(300)
}

/// Retry strategy for failed tasks.
///
/// Controls how long to wait between retry attempts when a task fails.
/// The default strategy is [`RetryStrategy::Fixed`] with a 5-second delay.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use durable::{RetryStrategy, SpawnOptions};
///
/// let options = SpawnOptions {
///     retry_strategy: Some(RetryStrategy::Exponential {
///         base_delay: Duration::from_secs(1),
///         factor: 2.0,
///         max_backoff: Duration::from_secs(60),
///     }),
///     ..Default::default()
/// };
/// ```
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetryStrategy {
    /// No retries - task fails permanently on first error
    None,

    /// Fixed delay between retries
    Fixed {
        /// Delay between retry attempts (default: 5 seconds)
        #[serde(default = "default_base_delay", rename = "base_seconds")]
        #[serde_as(as = "DurationSeconds<u64>")]
        base_delay: Duration,
    },

    /// Exponential backoff: delay = base_delay * (factor ^ (attempt - 1))
    Exponential {
        /// Initial delay (default: 5 seconds)
        #[serde(default = "default_base_delay", rename = "base_seconds")]
        #[serde_as(as = "DurationSeconds<u64>")]
        base_delay: Duration,
        /// Multiplier for each subsequent attempt (default: 2.0)
        #[serde(default = "default_factor")]
        factor: f64,
        /// Maximum delay cap (default: 300 seconds)
        #[serde(default = "default_max_backoff", rename = "max_seconds")]
        #[serde_as(as = "DurationSeconds<u64>")]
        max_backoff: Duration,
    },
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::Fixed {
            base_delay: default_base_delay(),
        }
    }
}

/// Automatic cancellation policy for tasks.
///
/// Allows tasks to be automatically cancelled based on how long they've been
/// waiting or running. Useful for preventing stale tasks from consuming resources.
#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CancellationPolicy {
    /// Cancel if task has been pending for more than this duration.
    /// Checked when the task would be claimed.
    #[serde(skip_serializing_if = "Option::is_none", rename = "max_delay")]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub max_pending_time: Option<Duration>,

    /// Cancel if task has been running for more than this duration total
    /// (across all attempts). Checked on retry.
    #[serde(skip_serializing_if = "Option::is_none", rename = "max_duration")]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub max_running_time: Option<Duration>,
}

/// Options for spawning a task.
///
/// All fields are optional and will use defaults if not specified.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use durable::{SpawnOptions, RetryStrategy};
///
/// let options = SpawnOptions {
///     max_attempts: Some(3),
///     retry_strategy: Some(RetryStrategy::Exponential {
///         base_delay: Duration::from_secs(5),
///         factor: 2.0,
///         max_backoff: Duration::from_secs(300),
///     }),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Default, Serialize)]
pub struct SpawnOptions {
    /// Maximum number of attempts before permanent failure (default: 5)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,

    /// Retry strategy (default: Fixed with 5s delay)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_strategy: Option<RetryStrategy>,

    /// Custom headers stored with the task (arbitrary metadata)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, JsonValue>>,

    /// Cancellation policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<CancellationPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_task_id: Option<Uuid>,
}

/// Options for configuring a worker.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use durable::WorkerOptions;
///
/// let options = WorkerOptions {
///     concurrency: 4,
///     claim_timeout: Duration::from_secs(120),
///     poll_interval: Duration::from_millis(500),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Unique worker identifier (default: hostname:pid)
    pub worker_id: Option<String>,

    /// Task lease duration (default: 120 seconds).
    /// Tasks must complete or checkpoint within this time.
    pub claim_timeout: Duration,

    /// Maximum tasks to claim per poll (default: same as concurrency)
    pub batch_size: Option<usize>,

    /// Maximum parallel task executions (default: 1)
    pub concurrency: usize,

    /// Interval between polls when queue is empty (default: 250ms)
    pub poll_interval: Duration,

    /// Terminate process if task exceeds 2x claim_timeout (default: false).
    /// When false, the task is aborted but other tasks continue running.
    /// Set to true if you need to guarantee no duplicate task execution.
    pub fatal_on_lease_timeout: bool,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            worker_id: None,
            claim_timeout: Duration::from_secs(120),
            batch_size: None,
            concurrency: 1,
            poll_interval: Duration::from_millis(250),
            fatal_on_lease_timeout: false,
        }
    }
}

/// A task that has been claimed by a worker
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub run_id: Uuid,
    pub task_id: Uuid,
    pub task_name: String,
    pub attempt: i32,
    pub params: JsonValue,
    pub retry_strategy: Option<RetryStrategy>,
    pub max_attempts: Option<i32>,
    pub headers: Option<HashMap<String, JsonValue>>,
    /// Event name that woke this task (if resuming from await_event)
    pub wake_event: Option<String>,
    /// Event payload (if resuming from await_event, None if timed out)
    pub event_payload: Option<JsonValue>,
}

/// Result returned when spawning a task
#[derive(Debug, Clone)]
pub struct SpawnResult {
    /// Unique identifier for this task
    pub task_id: Uuid,
    /// Identifier for the current run (attempt)
    pub run_id: Uuid,
    /// Current attempt number (starts at 1)
    pub attempt: i32,
}

/// Internal: Row returned from get_task_checkpoint_states
#[derive(Debug, Clone, sqlx::FromRow)]
#[allow(dead_code)]
pub struct CheckpointRow {
    pub checkpoint_name: String,
    pub state: JsonValue,
    pub owner_run_id: Uuid,
    pub updated_at: DateTime<Utc>,
}

/// Internal: Row returned from claim_task
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClaimedTaskRow {
    pub run_id: Uuid,
    pub task_id: Uuid,
    pub attempt: i32,
    pub task_name: String,
    pub params: JsonValue,
    pub retry_strategy: Option<JsonValue>,
    pub max_attempts: Option<i32>,
    pub headers: Option<JsonValue>,
    pub wake_event: Option<String>,
    pub event_payload: Option<JsonValue>,
}

impl TryFrom<ClaimedTaskRow> for ClaimedTask {
    type Error = serde_json::Error;

    fn try_from(row: ClaimedTaskRow) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: row.run_id,
            task_id: row.task_id,
            attempt: row.attempt,
            task_name: row.task_name,
            params: row.params,
            retry_strategy: row.retry_strategy.map(serde_json::from_value).transpose()?,
            max_attempts: row.max_attempts,
            headers: row.headers.map(serde_json::from_value).transpose()?,
            wake_event: row.wake_event,
            event_payload: row.event_payload,
        })
    }
}

/// Internal: Row returned from spawn_task
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SpawnResultRow {
    pub task_id: Uuid,
    pub run_id: Uuid,
    pub attempt: i32,
}

/// Internal: Row returned from await_event
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AwaitEventResult {
    pub should_suspend: bool,
    pub payload: Option<JsonValue>,
    #[allow(unused)]
    pub metadata: Option<JsonValue>,
}

/// Handle to a spawned subtask.
///
/// This type is returned by [`TaskContext::spawn`] and can be passed to
/// [`TaskContext::join`] to wait for the subtask to complete and retrieve
/// its result.
///
/// `TaskHandle` is serializable and will be checkpointed, ensuring that
/// retries of the parent task receive the same handle (pointing to the
/// same subtask).
///
/// # Type Parameter
///
/// The type parameter `T` represents the output type of the spawned task.
/// This provides compile-time type safety when joining.
///
/// # Example
///
/// ```ignore
/// let handle: TaskHandle<ProcessResult> = ctx.spawn::<ProcessTask>("process", params).await?;
/// let result: ProcessResult = ctx.join("process", handle).await?;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHandle<T> {
    /// The spawned subtask's task_id
    pub task_id: Uuid,
    /// Phantom for type safety
    #[serde(skip)]
    _phantom: PhantomData<T>,
}

impl<T> TaskHandle<T> {
    /// Create a new TaskHandle with the given task_id.
    pub(crate) fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            _phantom: PhantomData,
        }
    }
}

/// Default settings for spawned tasks.
///
/// Groups the default `max_attempts`, `retry_strategy`, and `cancellation`
/// settings that are applied when spawning tasks (either from the client
/// or from within a task context).
#[derive(Debug, Clone, Default)]
pub struct SpawnDefaults {
    /// Default max attempts for spawned tasks (default: 5)
    pub max_attempts: u32,
    /// Default retry strategy for spawned tasks
    pub retry_strategy: Option<RetryStrategy>,
    /// Default cancellation policy for spawned tasks
    pub cancellation: Option<CancellationPolicy>,
}

/// Terminal status of a child task.
///
/// This enum represents the possible terminal states a subtask can be in
/// when the parent joins on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChildStatus {
    /// Task completed successfully
    Completed,
    /// Task failed after exhausting retries
    Failed,
    /// Task was cancelled (manually or via cascade cancellation)
    Cancelled,
}

/// Event payload emitted when a child task reaches a terminal state.
///
/// This is used internally by the `join` mechanism to receive completion
/// notifications from subtasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChildCompletePayload {
    /// The terminal status of the child task
    pub status: ChildStatus,
    /// The task's output (only present if status is Completed)
    pub result: Option<JsonValue>,
    /// Error information (only present if status is Failed)
    pub error: Option<JsonValue>,
}
