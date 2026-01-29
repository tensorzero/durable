use serde_json::Value as JsonValue;
use thiserror::Error;

/// Signals that interrupt task execution without indicating failure.
///
/// These are not errors - they represent intentional control flow that the worker
/// handles specially. When a task returns `Err(TaskError::Control(_))`, the worker
/// will not mark it as failed or trigger retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlow {
    /// Task should suspend and resume later.
    ///
    /// Returned by [`TaskContext::sleep_for`](crate::TaskContext::sleep_for)
    /// and [`TaskContext::await_event`](crate::TaskContext::await_event)
    /// when the task needs to wait.
    Suspend,
    /// Task was cancelled.
    ///
    /// Detected when database operations return error code AB001, indicating
    /// the task was cancelled via [`Durable::cancel_task`](crate::Durable::cancel_task).
    Cancelled,
    /// Task lease expired (claim lost).
    ///
    /// Detected when database operations return error code AB002. Treated as control
    /// flow to avoid double-failing runs that were already failed by `claim_task`,
    /// and to let the next claim sweep fail the run if it hasn't happened yet.
    LeaseExpired,
}

/// Error type for task execution.
///
/// This enum distinguishes between control flow signals (suspension, cancellation)
/// and actual failures. The worker handles these differently:
///
/// - `Control(Suspend)` - Task is waiting; worker does nothing (scheduler will resume it)
/// - `Control(Cancelled)` - Task was cancelled; worker does nothing
/// - `Control(LeaseExpired)` - Task lost its lease; worker stops without failing the run
/// - All other variants - Actual errors; worker records failure and may retry
///
/// # Example
///
/// ```ignore
/// match ctx.await_event::<MyPayload>("my-event", Some(Duration::from_secs(30))).await {
///     Ok(payload) => { /* handle payload */ }
///     Err(TaskError::Timeout { step_name }) => {
///         println!("Timed out waiting for {}", step_name);
///     }
///     Err(TaskError::Control(ControlFlow::Cancelled)) => {
///         println!("Task was cancelled");
///     }
///     Err(e) => { /* handle other errors */ }
/// }
/// ```
#[derive(Debug, Error)]
pub enum TaskError {
    /// Control flow signal - not an actual error.
    ///
    /// The worker will not mark the task as failed or trigger retries.
    #[error("control flow: {0:?}")]
    Control(ControlFlow),

    /// The operation timed out.
    ///
    /// Returned by [`TaskContext::await_event`](crate::TaskContext::await_event) when
    /// a timeout is specified and the event doesn't arrive in time, or by
    /// [`TaskContext::join`](crate::TaskContext::join) when waiting for a child task.
    #[error("timed out waiting for '{step_name}'")]
    Timeout {
        /// The name of the step or event that timed out.
        step_name: String,
    },

    /// Database operation failed.
    ///
    /// This includes connection errors, query failures, and transaction issues.
    #[error("database error: {0}")]
    Database(sqlx::Error),

    /// JSON serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(serde_json::Error),

    //// Error occurred while trying to spawn a subtask
    #[error("failed to spawn subtask `{name}`: {error}")]
    SubtaskSpawnFailed { name: String, error: DurableError },

    /// Error occurred while trying to emit an event.
    #[error("failed to emit event `{event_name}`: {error}")]
    EmitEventFailed {
        event_name: String,
        error: DurableError,
    },

    /// A child task failed.
    ///
    /// Returned by [`TaskContext::join`](crate::TaskContext::join) when the child
    /// task completed with an error.
    #[error("child task failed at '{step_name}': {message}")]
    ChildFailed {
        /// The step name used when joining the child task.
        step_name: String,
        /// The error message from the child task.
        message: String,
    },

    /// A child task was cancelled.
    ///
    /// Returned by [`TaskContext::join`](crate::TaskContext::join) when the child
    /// task was cancelled before completion.
    #[error("child task was cancelled at '{step_name}'")]
    ChildCancelled {
        /// The step name used when joining the child task.
        step_name: String,
    },

    /// A validation error occurred.
    ///
    /// This includes errors like reserved step name prefixes, empty event names,
    /// reserved header prefixes, and unregistered task names.
    #[error("{message}")]
    Validation {
        /// A description of the validation error.
        message: String,
    },

    /// A user error from task code.
    ///
    /// This variant stores a serialized user error for persistence and retrieval.
    /// Created via [`TaskError::user()`] or [`TaskError::user_message()`].
    #[error("{message}")]
    User {
        /// The error message (extracted from "message" field or stringified data)
        message: String,
        /// Serialized error data for storage/retrieval
        error_data: JsonValue,
    },

    /// An internal error from user task code.
    ///
    /// This is the catch-all variant for errors propagated via `?` on anyhow errors.
    /// For structured user errors, prefer using [`TaskError::user()`].
    #[error(transparent)]
    TaskInternal(#[from] anyhow::Error),
}

/// Result type alias for task execution.
///
/// Use this as the return type for [`Task::run`](crate::Task::run) implementations.
pub type TaskResult<T> = Result<T, TaskError>;

impl TaskError {
    /// Create a user error from arbitrary JSON data.
    ///
    /// If the JSON is an object with a "message" field, that's used for display.
    /// Otherwise, the JSON is stringified for the display message.
    ///
    /// ```ignore
    /// // With structured data
    /// Err(TaskError::user(json!({"message": "Not found", "code": 404})))
    ///
    /// // With any serializable type
    /// Err(TaskError::user(MyError { code: 404, details: "..." }))
    /// ```
    pub fn user(error_data: impl serde::Serialize) -> Self {
        let error_data = serde_json::to_value(&error_data).unwrap_or(serde_json::Value::Null);
        let message = error_data
            .get("message")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| error_data.to_string());
        TaskError::User {
            message,
            error_data,
        }
    }

    /// Create a user error from just a message string.
    pub fn user_message(message: impl Into<String>) -> Self {
        let message = message.into();
        TaskError::User {
            error_data: serde_json::Value::String(message.clone()),
            message,
        }
    }
}

impl From<serde_json::Error> for TaskError {
    fn from(err: serde_json::Error) -> Self {
        TaskError::Serialization(err)
    }
}

impl From<sqlx::Error> for TaskError {
    fn from(err: sqlx::Error) -> Self {
        if is_cancelled_error(&err) {
            TaskError::Control(ControlFlow::Cancelled)
        } else if is_lease_expired_error(&err) {
            TaskError::Control(ControlFlow::LeaseExpired)
        } else {
            TaskError::Database(err)
        }
    }
}

/// Check if a sqlx error indicates task cancellation (error code AB001)
pub fn is_cancelled_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        db_err.code().is_some_and(|c| c == "AB001")
    } else {
        false
    }
}

/// Check if a sqlx error indicates lease expiration (error code AB002)
pub fn is_lease_expired_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        db_err.code().is_some_and(|c| c == "AB002")
    } else {
        false
    }
}

/// Serialize a TaskError for storage in fail_run
pub fn serialize_task_error(err: &TaskError) -> JsonValue {
    match err {
        TaskError::Control(_) => {
            // Control flow signals should never be serialized as failures
            serde_json::json!({
                "name": "ControlFlow",
                "message": err.to_string(),
            })
        }
        TaskError::Timeout { step_name } => {
            serde_json::json!({
                "name": "Timeout",
                "message": err.to_string(),
                "step_name": step_name,
            })
        }
        TaskError::Database(e) => {
            serde_json::json!({
                "name": "Database",
                "message": e.to_string(),
            })
        }
        TaskError::Serialization(e) => {
            serde_json::json!({
                "name": "Serialization",
                "message": e.to_string(),
            })
        }
        TaskError::SubtaskSpawnFailed { name, error } => {
            serde_json::json!({
                "name": "SubtaskSpawnFailed",
                "message": error.to_string(),
                "subtask_name": name,
            })
        }
        TaskError::EmitEventFailed { event_name, error } => {
            serde_json::json!({
                "name": "EmitEventFailed",
                "message": error.to_string(),
                "event_name": event_name,
            })
        }
        TaskError::ChildFailed { step_name, message } => {
            serde_json::json!({
                "name": "ChildFailed",
                "message": message,
                "step_name": step_name,
            })
        }
        TaskError::ChildCancelled { step_name } => {
            serde_json::json!({
                "name": "ChildCancelled",
                "message": err.to_string(),
                "step_name": step_name,
            })
        }
        TaskError::Validation { message } => {
            serde_json::json!({
                "name": "Validation",
                "message": message,
            })
        }
        TaskError::User {
            message,
            error_data,
        } => {
            serde_json::json!({
                "name": "User",
                "message": message,
                "error_data": error_data,
            })
        }
        TaskError::TaskInternal(e) => {
            serde_json::json!({
                "name": "TaskInternal",
                "message": e.to_string(),
                "backtrace": format!("{:?}", e)
            })
        }
    }
}

/// Error type for Client API operations.
///
/// This enum covers all errors that can occur when using the [`Durable`](crate::Durable) client
/// to spawn tasks, manage queues, and emit events. Unlike [`TaskError`], which is used
/// during task execution and intentionally wraps `anyhow::Error` for flexibility,
/// `DurableError` provides typed variants that callers can match on.
///
/// # Example
///
/// ```ignore
/// match client.spawn::<MyTask>(params).await {
///     Ok(result) => println!("Spawned task {}", result.task_id),
///     Err(DurableError::Database(e)) => eprintln!("Database error: {}", e),
///     Err(DurableError::TaskNotRegistered { task_name }) => {
///         eprintln!("Task {} not registered", task_name);
///     }
///     Err(e) => eprintln!("Other error: {}", e),
/// }
/// ```
#[derive(Debug, Error)]
pub enum DurableError {
    /// Database operation failed.
    ///
    /// This includes connection errors, query failures, pool exhaustion, and transaction issues.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// JSON serialization or deserialization failed.
    ///
    /// Occurs when task parameters or event payloads cannot be serialized to JSON.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Task is not registered with this client.
    ///
    /// Tasks must be registered via [`Durable::register`](crate::Durable::register) before spawning.
    #[error("Unknown task: '{task_name}'. Task must be registered before spawning.")]
    TaskNotRegistered {
        /// The name of the task that was not found in the registry.
        task_name: String,
    },

    /// Task is already registered with this client.
    ///
    /// Each task name must be unique within a client instance.
    #[error("task '{task_name}' is already registered. Each task name must be unique.")]
    TaskAlreadyRegistered {
        /// The name of the task that was already registered.
        task_name: String,
    },

    //// Task params validation failed.
    ///
    /// Returned when the task definition in the registry fails to validate the params
    /// (before we attempt to spawn the task in Postgres).
    #[error("invalid task parameters for '{task_name}': {message}")]
    InvalidTaskParams {
        /// The name of the task being spawned
        task_name: String,
        /// The error message from the task.
        message: String,
    },

    /// Header key uses a reserved prefix.
    ///
    /// User-provided headers cannot start with "durable::" as this prefix
    /// is reserved for internal use.
    #[error(
        "header key '{key}' uses reserved prefix 'durable::'. User headers cannot start with 'durable::'."
    )]
    ReservedHeaderPrefix {
        /// The header key that used the reserved prefix.
        key: String,
    },

    /// Event name validation failed.
    #[error("invalid event name: {reason}")]
    InvalidEventName {
        /// The reason the event name was invalid.
        reason: String,
    },

    /// Configuration validation failed.
    ///
    /// Returned when worker options contain invalid values.
    #[error("invalid configuration: {reason}")]
    InvalidConfiguration {
        /// The reason the configuration was invalid.
        reason: String,
    },
}

/// Result type alias for Client API operations.
///
/// Use this as the return type for [`Durable`](crate::Durable) method calls.
pub type DurableResult<T> = Result<T, DurableError>;
