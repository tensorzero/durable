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
}

/// Error type for task execution.
///
/// This enum distinguishes between control flow signals (suspension, cancellation)
/// and actual failures. The worker handles these differently:
///
/// - `Control(Suspend)` - Task is waiting; worker does nothing (scheduler will resume it)
/// - `Control(Cancelled)` - Task was cancelled; worker does nothing
/// - `Failed(_)` - Actual error; worker records failure and may retry
#[derive(Debug, Error)]
pub enum TaskError {
    /// Control flow signal - not an actual error.
    ///
    /// The worker will not mark the task as failed or trigger retries.
    #[error("control flow: {0:?}")]
    Control(ControlFlow),

    /// An error occurred during task execution.
    ///
    /// The worker will record this failure and may retry the task based on
    /// the configured [`RetryStrategy`](crate::RetryStrategy).
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// Result type alias for task execution.
///
/// Use this as the return type for [`Task::run`](crate::Task::run) implementations.
pub type TaskResult<T> = Result<T, TaskError>;

impl From<serde_json::Error> for TaskError {
    fn from(err: serde_json::Error) -> Self {
        TaskError::Failed(err.into())
    }
}

impl From<sqlx::Error> for TaskError {
    fn from(err: sqlx::Error) -> Self {
        if is_cancelled_error(&err) {
            TaskError::Control(ControlFlow::Cancelled)
        } else {
            TaskError::Failed(err.into())
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

/// Serialize error for storage in fail_run
pub fn serialize_error(err: &anyhow::Error) -> JsonValue {
    serde_json::json!({
        "name": "Error",
        "message": err.to_string(),
        "backtrace": format!("{:?}", err)
    })
}
