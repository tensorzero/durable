use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::TaskContext;
use crate::error::{TaskError, TaskResult};

/// Defines a task with typed parameters and output.
///
/// Implement this trait for your task types. The worker will:
/// 1. Deserialize params from JSON into `Params` type
/// 2. Call `run()` with the typed params, a TaskContext, and your application state
/// 3. Serialize the result back to JSON for storage
///
/// # Type Parameter
///
/// * `State` - Application state type (e.g., HTTP clients, database pools).
///   Use `()` if you don't need any state.
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
///     async fn run(params: Self::Params, mut ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
///         let result = ctx.step("send", || async {
///             email_service::send(&params.to, &params.subject, &params.body).await
///         }).await?;
///
///         Ok(SendEmailResult { message_id: result.id })
///     }
/// }
///
/// // With application state:
/// #[derive(Clone)]
/// struct AppState {
///     http_client: reqwest::Client,
/// }
///
/// struct FetchUrlTask;
///
/// #[async_trait]
/// impl Task<AppState> for FetchUrlTask {
///     const NAME: &'static str = "fetch-url";
///     type Params = String;
///     type Output = String;
///
///     async fn run(url: Self::Params, mut ctx: TaskContext, state: AppState) -> TaskResult<Self::Output> {
///         let body = ctx.step("fetch", || async {
///             state.http_client.get(&url).send().await?.text().await
///                 .map_err(|e| anyhow::anyhow!(e))
///         }).await?;
///         Ok(body)
///     }
/// }
/// ```
#[async_trait]
pub trait Task<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
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
    /// The `state` parameter provides access to application-level resources
    /// like HTTP clients, database pools, etc.
    async fn run(
        params: Self::Params,
        ctx: TaskContext<State>,
        state: State,
    ) -> TaskResult<Self::Output>;
}

/// Internal trait for storing heterogeneous tasks in a HashMap.
/// Converts between typed Task interface and JSON values.
#[async_trait]
#[allow(dead_code)]
pub trait ErasedTask<State>: Send + Sync
where
    State: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &'static str;
    async fn execute(
        &self,
        params: JsonValue,
        ctx: TaskContext<State>,
        state: State,
    ) -> Result<JsonValue, TaskError>;
}

#[async_trait]
impl<T, State> ErasedTask<State> for PhantomData<T>
where
    T: Task<State>,
    State: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        T::NAME
    }

    async fn execute(
        &self,
        params: JsonValue,
        ctx: TaskContext<State>,
        state: State,
    ) -> Result<JsonValue, TaskError> {
        let typed_params: T::Params = serde_json::from_value(params)?;
        let result = T::run(typed_params, ctx, state).await?;
        Ok(serde_json::to_value(&result)?)
    }
}

/// Type alias for the task registry
pub type TaskRegistry<State> = std::collections::HashMap<String, Arc<dyn ErasedTask<State>>>;
