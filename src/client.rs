use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::{Executor, PgPool, Postgres};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::task::{Task, TaskRegistry};
use crate::types::{
    CancellationPolicy, RetryStrategy, SpawnOptions, SpawnResult, SpawnResultRow, WorkerOptions,
};

/// Internal struct for serializing spawn options to the database.
#[derive(Serialize)]
struct SpawnOptionsDb<'a> {
    max_attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<&'a HashMap<String, JsonValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_strategy: Option<&'a RetryStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancellation: Option<CancellationPolicyDb>,
}

/// Internal struct for serializing cancellation policy (only non-None fields).
#[derive(Serialize)]
struct CancellationPolicyDb {
    #[serde(skip_serializing_if = "Option::is_none")]
    max_delay: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_duration: Option<u64>,
}

impl CancellationPolicyDb {
    fn from_policy(policy: &CancellationPolicy) -> Option<Self> {
        if policy.max_delay.is_none() && policy.max_duration.is_none() {
            None
        } else {
            Some(Self {
                max_delay: policy.max_delay,
                max_duration: policy.max_duration,
            })
        }
    }
}

use crate::worker::Worker;

/// The main client for interacting with durable workflows.
///
/// Use this client to:
/// - Register task types with [`register`](Self::register)
/// - Spawn tasks with [`spawn`](Self::spawn) or [`spawn_with_options`](Self::spawn_with_options)
/// - Start workers with [`start_worker`](Self::start_worker)
/// - Manage queues with [`create_queue`](Self::create_queue), [`drop_queue`](Self::drop_queue)
/// - Emit events with [`emit_event`](Self::emit_event)
/// - Cancel tasks with [`cancel_task`](Self::cancel_task)
///
/// # Type Parameter
///
/// * `State` - Application state type passed to task handlers. Use `()` if you
///   don't need any state. The state must implement `Clone + Send + Sync + 'static`.
///
/// # Example
///
/// ```ignore
/// // Without state (default)
/// let client = Durable::builder()
///     .database_url("postgres://localhost/myapp")
///     .queue_name("tasks")
///     .build()
///     .await?;
///
/// // With application state
/// #[derive(Clone)]
/// struct AppState {
///     http_client: reqwest::Client,
/// }
///
/// let app_state = AppState { http_client: reqwest::Client::new() };
/// let client = Durable::builder()
///     .database_url("postgres://localhost/myapp")
///     .queue_name("tasks")
///     .build_with_state(app_state)
///     .await?;
///
/// client.register::<MyTask>().await;
/// client.spawn::<MyTask>(params).await?;
/// ```
pub struct Durable<State = ()>
where
    State: Clone + Send + Sync + 'static,
{
    pool: PgPool,
    owns_pool: bool,
    queue_name: String,
    default_max_attempts: u32,
    registry: Arc<RwLock<TaskRegistry<State>>>,
    state: State,
}

/// Builder for configuring a [`Durable`] client.
///
/// # Example
///
/// ```ignore
/// // Without state
/// let client = Durable::builder()
///     .database_url("postgres://localhost/myapp")
///     .queue_name("orders")
///     .default_max_attempts(3)
///     .build()
///     .await?;
///
/// // With state
/// let client = Durable::builder()
///     .database_url("postgres://localhost/myapp")
///     .build_with_state(my_app_state)
///     .await?;
/// ```
pub struct DurableBuilder {
    database_url: Option<String>,
    pool: Option<PgPool>,
    queue_name: String,
    default_max_attempts: u32,
}

impl DurableBuilder {
    pub fn new() -> Self {
        Self {
            database_url: None,
            pool: None,
            queue_name: "default".to_string(),
            default_max_attempts: 5,
        }
    }

    /// Set database URL (will create a new connection pool)
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    /// Use an existing connection pool (Durable will NOT close it)
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Set the default queue name (default: "default")
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// Set default max attempts for spawned tasks (default: 5)
    pub fn default_max_attempts(mut self, attempts: u32) -> Self {
        self.default_max_attempts = attempts;
        self
    }

    /// Build the Durable client without application state.
    ///
    /// Use this when your tasks don't need access to shared resources
    /// like HTTP clients or database pools.
    pub async fn build(self) -> anyhow::Result<Durable<()>> {
        self.build_with_state(()).await
    }

    /// Build the Durable client with application state.
    ///
    /// The state will be cloned and passed to each task execution.
    /// Use this to provide shared resources like HTTP clients, database pools,
    /// or other application state to your tasks.
    ///
    /// # Example
    ///
    /// ```ignore
    /// #[derive(Clone)]
    /// struct AppState {
    ///     http_client: reqwest::Client,
    ///     db_pool: PgPool,
    /// }
    ///
    /// let state = AppState {
    ///     http_client: reqwest::Client::new(),
    ///     db_pool: pool.clone(),
    /// };
    ///
    /// let client = Durable::builder()
    ///     .database_url("postgres://localhost/myapp")
    ///     .build_with_state(state)
    ///     .await?;
    /// ```
    pub async fn build_with_state<State>(self, state: State) -> anyhow::Result<Durable<State>>
    where
        State: Clone + Send + Sync + 'static,
    {
        let (pool, owns_pool) = if let Some(pool) = self.pool {
            (pool, false)
        } else {
            let url = self
                .database_url
                .or_else(|| std::env::var("DURABLE_DATABASE_URL").ok())
                .unwrap_or_else(|| "postgresql://localhost/durable".to_string());
            (PgPool::connect(&url).await?, true)
        };

        Ok(Durable {
            pool,
            owns_pool,
            queue_name: self.queue_name,
            default_max_attempts: self.default_max_attempts,
            registry: Arc::new(RwLock::new(HashMap::new())),
            state,
        })
    }
}

impl Default for DurableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Durable<()> {
    /// Create a new client with default settings (no application state).
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        DurableBuilder::new()
            .database_url(database_url)
            .build()
            .await
    }

    /// Access the builder for custom configuration
    pub fn builder() -> DurableBuilder {
        DurableBuilder::new()
    }
}

impl<State> Durable<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Get a reference to the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the queue name this client is configured for
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Get a reference to the application state
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Register a task type. Required before spawning or processing.
    pub async fn register<T: Task<State>>(&self) -> &Self {
        let mut registry = self.registry.write().await;
        registry.insert(T::NAME.to_string(), &PhantomData::<T>);
        self
    }

    /// Spawn a task (type-safe version)
    pub async fn spawn<T: Task<State>>(&self, params: T::Params) -> anyhow::Result<SpawnResult> {
        self.spawn_with_options::<T>(params, SpawnOptions::default())
            .await
    }

    /// Spawn a task with options (type-safe version)
    pub async fn spawn_with_options<T: Task<State>>(
        &self,
        params: T::Params,
        options: SpawnOptions,
    ) -> anyhow::Result<SpawnResult> {
        self.spawn_by_name(T::NAME, serde_json::to_value(&params)?, options)
            .await
    }

    /// Spawn a task by name (dynamic version for unregistered tasks)
    pub async fn spawn_by_name(
        &self,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> anyhow::Result<SpawnResult> {
        self.spawn_by_name_with(&self.pool, task_name, params, options)
            .await
    }

    /// Spawn a task with a custom executor (e.g., a transaction).
    ///
    /// This allows you to atomically enqueue a task as part of a larger transaction.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut tx = client.pool().begin().await?;
    ///
    /// sqlx::query("INSERT INTO orders (id) VALUES ($1)")
    ///     .bind(order_id)
    ///     .execute(&mut *tx)
    ///     .await?;
    ///
    /// client.spawn_with::<ProcessOrder, _>(&mut *tx, params).await?;
    ///
    /// tx.commit().await?;
    /// ```
    pub async fn spawn_with<'e, T, E>(
        &self,
        executor: E,
        params: T::Params,
    ) -> anyhow::Result<SpawnResult>
    where
        T: Task<State>,
        E: Executor<'e, Database = Postgres>,
    {
        self.spawn_with_options_with::<T, E>(executor, params, SpawnOptions::default())
            .await
    }

    /// Spawn a task with options using a custom executor.
    pub async fn spawn_with_options_with<'e, T, E>(
        &self,
        executor: E,
        params: T::Params,
        options: SpawnOptions,
    ) -> anyhow::Result<SpawnResult>
    where
        T: Task<State>,
        E: Executor<'e, Database = Postgres>,
    {
        self.spawn_by_name_with(executor, T::NAME, serde_json::to_value(&params)?, options)
            .await
    }

    /// Spawn a task by name using a custom executor.
    pub async fn spawn_by_name_with<'e, E>(
        &self,
        executor: E,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> anyhow::Result<SpawnResult>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let max_attempts = options.max_attempts.unwrap_or(self.default_max_attempts);

        let db_options = Self::serialize_spawn_options(&options, max_attempts)?;

        let query = "SELECT task_id, run_id, attempt
             FROM durable.spawn_task($1, $2, $3, $4)";

        let row: SpawnResultRow = sqlx::query_as(query)
            .bind(&self.queue_name)
            .bind(task_name)
            .bind(&params)
            .bind(&db_options)
            .fetch_one(executor)
            .await?;

        Ok(SpawnResult {
            task_id: row.task_id,
            run_id: row.run_id,
            attempt: row.attempt,
        })
    }

    fn serialize_spawn_options(
        options: &SpawnOptions,
        max_attempts: u32,
    ) -> serde_json::Result<JsonValue> {
        let db_options = SpawnOptionsDb {
            max_attempts,
            headers: options.headers.as_ref(),
            retry_strategy: options.retry_strategy.as_ref(),
            cancellation: options
                .cancellation
                .as_ref()
                .and_then(CancellationPolicyDb::from_policy),
        };
        serde_json::to_value(db_options)
    }

    /// Create a queue (defaults to this client's queue name)
    pub async fn create_queue(&self, queue_name: Option<&str>) -> anyhow::Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        let query = "SELECT durable.create_queue($1)";
        sqlx::query(query).bind(queue).execute(&self.pool).await?;
        Ok(())
    }

    /// Drop a queue and all its data
    pub async fn drop_queue(&self, queue_name: Option<&str>) -> anyhow::Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        let query = "SELECT durable.drop_queue($1)";
        sqlx::query(query).bind(queue).execute(&self.pool).await?;
        Ok(())
    }

    /// List all queues
    pub async fn list_queues(&self) -> anyhow::Result<Vec<String>> {
        let query = "SELECT queue_name FROM durable.list_queues()";
        let rows: Vec<(String,)> = sqlx::query_as(query).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    /// Emit an event to a queue (defaults to this client's queue)
    pub async fn emit_event<T: Serialize>(
        &self,
        event_name: &str,
        payload: &T,
        queue_name: Option<&str>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!event_name.is_empty(), "event_name must be non-empty");

        let queue = queue_name.unwrap_or(&self.queue_name);
        let payload_json = serde_json::to_value(payload)?;

        let query = "SELECT durable.emit_event($1, $2, $3)";
        sqlx::query(query)
            .bind(queue)
            .bind(event_name)
            .bind(&payload_json)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Cancel a task by ID. Running tasks will be cancelled at
    /// their next checkpoint or heartbeat.
    pub async fn cancel_task(&self, task_id: Uuid, queue_name: Option<&str>) -> anyhow::Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        let query = "SELECT durable.cancel_task($1, $2)";
        sqlx::query(query)
            .bind(queue)
            .bind(task_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Start a worker that processes tasks from the queue
    pub async fn start_worker(&self, options: WorkerOptions) -> Worker {
        Worker::start(
            self.pool.clone(),
            self.queue_name.clone(),
            self.registry.clone(),
            options,
            self.state.clone(),
        )
        .await
    }

    /// Close the client. Closes the pool if owned.
    pub async fn close(self) -> anyhow::Result<()> {
        if self.owns_pool {
            self.pool.close().await;
        }
        Ok(())
    }
}
