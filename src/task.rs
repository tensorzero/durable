use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::context::TaskContext;
use crate::error::{TaskError, TaskResult};

/// Defines a task with typed parameters and output.
///
/// Implement this trait for your task types. The worker will:
/// 1. Deserialize params from JSON into `Params` type
/// 2. Call `run()` with the typed params and a TaskContext
/// 3. Serialize the result back to JSON for storage
///
/// # Example
/// ```ignore
/// struct SendEmailTask;
///
/// #[async_trait]
/// impl Task for SendEmailTask {
///     const NAME: &'static str = "send-email";
///     type Params = SendEmailParams;
///     type Output = SendEmailResult;
///
///     async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
///         let result = ctx.step("send", || async {
///             email_service::send(&params.to, &params.subject, &params.body).await
///         }).await?;
///
///         Ok(SendEmailResult { message_id: result.id })
///     }
/// }
/// ```
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Task name as stored in the database.
    /// Should be unique across your application.
    const NAME: &'static str;

    /// Parameter type (must be JSON-serializable)
    type Params: Serialize + DeserializeOwned + Send;

    /// Output type (must be JSON-serializable)
    type Output: Serialize + DeserializeOwned + Send;

    /// Execute the task logic.
    ///
    /// Return `Ok(output)` on success, or `Err(TaskError)` on failure.
    /// Use `?` freely - errors will propagate and the task will be retried
    /// according to its [`RetryStrategy`](crate::RetryStrategy).
    ///
    /// The [`TaskContext`] provides methods for checkpointing, sleeping,
    /// and waiting for events. See [`TaskContext`] for details.
    async fn run(params: Self::Params, ctx: TaskContext) -> TaskResult<Self::Output>;
}

/// Internal trait for storing heterogeneous tasks in a HashMap.
/// Converts between typed Task interface and JSON values.
#[async_trait]
#[allow(dead_code)]
pub trait ErasedTask: Send + Sync {
    fn name(&self) -> &'static str;
    async fn execute(&self, params: JsonValue, ctx: TaskContext) -> Result<JsonValue, TaskError>;
}

/// Wrapper that implements ErasedTask for any Task type
pub struct TaskWrapper<T: Task>(std::marker::PhantomData<T>);

impl<T: Task> TaskWrapper<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T: Task> Default for TaskWrapper<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Task> ErasedTask for TaskWrapper<T> {
    fn name(&self) -> &'static str {
        T::NAME
    }

    async fn execute(&self, params: JsonValue, ctx: TaskContext) -> Result<JsonValue, TaskError> {
        let typed_params: T::Params = serde_json::from_value(params)?;
        let result = T::run(typed_params, ctx).await?;
        Ok(serde_json::to_value(&result)?)
    }
}

/// Type alias for the task registry
pub type TaskRegistry = std::collections::HashMap<String, Arc<dyn ErasedTask>>;
