use serde_json::Value as JsonValue;
use thiserror::Error;

/// Signals that interrupt task execution (these are not errors!)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlow {
    /// Task should suspend and resume later (sleep, await_event)
    Suspend,
    /// Task was cancelled (detected via AB001 error from database)
    Cancelled,
}

/// Error type for task execution
#[derive(Debug, Error)]
pub enum TaskError {
    /// Control flow signal - not an actual error.
    /// Worker will not mark the task as failed.
    #[error("control flow: {0:?}")]
    Control(ControlFlow),

    /// Any other error during task execution.
    /// Worker will call fail_run and potentially retry.
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// Convenience type alias for task return types
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
