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
/// 2. Call `run()` with the typed params, a TaskContext, and your application context
/// 3. Serialize the result back to JSON for storage
///
/// # Type Parameter
///
/// * `Ctx` - Application context type (e.g., HTTP clients, database pools).
///   Use `()` if you don't need any context.
///
/// # Example
/// ```ignore
/// struct SendEmailTask;
///
/// #[async_trait]
/// impl Task<()> for SendEmailTask {
///     const NAME: &'static str = "send-email";
///     type Params = SendEmailParams;
///     type Output = SendEmailResult;
///
///     async fn run(params: Self::Params, mut ctx: TaskContext, _app_ctx: ()) -> TaskResult<Self::Output> {
///         let result = ctx.step("send", || async {
///             email_service::send(&params.to, &params.subject, &params.body).await
///         }).await?;
///
///         Ok(SendEmailResult { message_id: result.id })
///     }
/// }
///
/// // With application context:
/// #[derive(Clone)]
/// struct AppContext {
///     http_client: reqwest::Client,
/// }
///
/// struct FetchUrlTask;
///
/// #[async_trait]
/// impl Task<AppContext> for FetchUrlTask {
///     const NAME: &'static str = "fetch-url";
///     type Params = String;
///     type Output = String;
///
///     async fn run(url: Self::Params, mut ctx: TaskContext, app_ctx: AppContext) -> TaskResult<Self::Output> {
///         let body = ctx.step("fetch", || async {
///             app_ctx.http_client.get(&url).send().await?.text().await
///                 .map_err(|e| anyhow::anyhow!(e))
///         }).await?;
///         Ok(body)
///     }
/// }
/// ```
#[async_trait]
pub trait Task<Ctx>: Send + Sync + 'static
where
    Ctx: Clone + Send + Sync + 'static,
{
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
    ///
    /// The `app_ctx` parameter provides access to application-level resources
    /// like HTTP clients, database pools, etc.
    async fn run(params: Self::Params, ctx: TaskContext, app_ctx: Ctx) -> TaskResult<Self::Output>;
}

/// Internal trait for storing heterogeneous tasks in a HashMap.
/// Converts between typed Task interface and JSON values.
#[async_trait]
#[allow(dead_code)]
pub trait ErasedTask<Ctx>: Send + Sync
where
    Ctx: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &'static str;
    async fn execute(
        &self,
        params: JsonValue,
        ctx: TaskContext,
        app_ctx: Ctx,
    ) -> Result<JsonValue, TaskError>;
}

/// Wrapper that implements ErasedTask for any Task type
pub struct TaskWrapper<T, Ctx>(std::marker::PhantomData<(T, Ctx)>)
where
    T: Task<Ctx>,
    Ctx: Clone + Send + Sync + 'static;

impl<T, Ctx> TaskWrapper<T, Ctx>
where
    T: Task<Ctx>,
    Ctx: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T, Ctx> Default for TaskWrapper<T, Ctx>
where
    T: Task<Ctx>,
    Ctx: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T, Ctx> ErasedTask<Ctx> for TaskWrapper<T, Ctx>
where
    T: Task<Ctx>,
    Ctx: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        T::NAME
    }

    async fn execute(
        &self,
        params: JsonValue,
        ctx: TaskContext,
        app_ctx: Ctx,
    ) -> Result<JsonValue, TaskError> {
        let typed_params: T::Params = serde_json::from_value(params)?;
        let result = T::run(typed_params, ctx, app_ctx).await?;
        Ok(serde_json::to_value(&result)?)
    }
}

/// Type alias for the task registry
pub type TaskRegistry<Ctx> = std::collections::HashMap<String, Arc<dyn ErasedTask<Ctx>>>;
