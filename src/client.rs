use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::{Executor, PgPool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::{DurableError, DurableResult};
use crate::task::{Task, TaskRegistry, TaskWrapper};
use crate::types::{
    CancellationPolicy, RetryStrategy, SpawnDefaults, SpawnOptions, SpawnResult, SpawnResultRow,
    WorkerOptions,
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
        if policy.max_pending_time.is_none() && policy.max_running_time.is_none() {
            None
        } else {
            Some(Self {
                max_delay: policy.max_pending_time.map(|d| d.as_secs()),
                max_duration: policy.max_running_time.map(|d| d.as_secs()),
            })
        }
    }
}

use crate::worker::Worker;

/// Validates that user-provided headers don't use reserved prefixes.
fn validate_headers(headers: &Option<HashMap<String, JsonValue>>) -> DurableResult<()> {
    if let Some(headers) = headers {
        for key in headers.keys() {
            if key.starts_with("durable::") {
                return Err(DurableError::ReservedHeaderPrefix { key: key.clone() });
            }
        }
    }
    Ok(())
}

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
/// client.register::<MyTask>().await?;
/// client.spawn::<MyTask>(params).await?;
/// ```
pub struct Durable<State = ()>
where
    State: Clone + Send + Sync + 'static,
{
    pool: PgPool,
    owns_pool: bool,
    queue_name: String,
    spawn_defaults: SpawnDefaults,
    registry: Arc<RwLock<TaskRegistry<State>>>,
    state: State,
}

/// Builder for configuring a [`Durable`] client.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
/// use durable::{Durable, RetryStrategy, CancellationPolicy};
///
/// // Without state
/// let client = Durable::builder()
///     .database_url("postgres://localhost/myapp")
///     .queue_name("orders")
///     .default_max_attempts(3)
///     .default_retry_strategy(RetryStrategy::Exponential {
///         base_delay: Duration::from_secs(5),
///         factor: 2.0,
///         max_backoff: Duration::from_secs(300),
///     })
///     .default_cancellation(CancellationPolicy {
///         max_pending_time: Some(Duration::from_secs(3600)),
///         max_running_time: None,
///     })
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
    spawn_defaults: SpawnDefaults,
}

impl DurableBuilder {
    pub fn new() -> Self {
        Self {
            database_url: None,
            pool: None,
            queue_name: "default".to_string(),
            spawn_defaults: SpawnDefaults {
                max_attempts: 5,
                retry_strategy: None,
                cancellation: None,
            },
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
        self.spawn_defaults.max_attempts = attempts;
        self
    }

    /// Set default retry strategy for spawned tasks (default: Fixed with 5s delay)
    pub fn default_retry_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.spawn_defaults.retry_strategy = Some(strategy);
        self
    }

    /// Set default cancellation policy for spawned tasks (default: no auto-cancellation)
    pub fn default_cancellation(mut self, policy: CancellationPolicy) -> Self {
        self.spawn_defaults.cancellation = Some(policy);
        self
    }

    /// Build the Durable client without application state.
    ///
    /// Use this when your tasks don't need access to shared resources
    /// like HTTP clients or database pools.
    pub async fn build(self) -> DurableResult<Durable<()>> {
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
    pub async fn build_with_state<State>(self, state: State) -> DurableResult<Durable<State>>
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
            spawn_defaults: self.spawn_defaults,
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
    pub async fn new(database_url: &str) -> DurableResult<Self> {
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
    ///
    /// Returns an error if a task with the same name is already registered.
    pub async fn register<T: Task<State> + Default>(&self) -> DurableResult<&Self> {
        self.register_instance(T::default()).await
    }

    /// Register a task instance. Required before spawning or processing.
    ///
    /// Use this when you need to register a task with runtime-determined metadata
    /// (e.g., a TypeScript tool loaded from a config file).
    ///
    /// Returns an error if a task with the same name is already registered.
    pub async fn register_instance<T: Task<State>>(&self, task: T) -> DurableResult<&Self> {
        let mut registry = self.registry.write().await;
        let name = task.name();
        if registry.contains_key(name.as_ref()) {
            return Err(DurableError::TaskAlreadyRegistered {
                task_name: name.to_string(),
            });
        }
        registry.insert(name, Arc::new(TaskWrapper::new(task)));
        Ok(self)
    }

    /// Spawn a task (type-safe version)
    pub async fn spawn<T: Task<State> + Default>(
        &self,
        params: T::Params,
    ) -> DurableResult<SpawnResult> {
        self.spawn_with_options::<T>(params, SpawnOptions::default())
            .await
    }

    /// Spawn a task with options (type-safe version)
    pub async fn spawn_with_options<T: Task<State> + Default>(
        &self,
        params: T::Params,
        options: SpawnOptions,
    ) -> DurableResult<SpawnResult> {
        let task = T::default();
        self.spawn_by_name(&task.name(), serde_json::to_value(&params)?, options)
            .await
    }

    /// Spawn a task by name (dynamic version).
    ///
    /// The task must be registered before spawning.
    pub async fn spawn_by_name(
        &self,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> DurableResult<SpawnResult> {
        // Validate that the task is registered
        {
            let registry = self.registry.read().await;
            if !registry.contains_key(task_name) {
                return Err(DurableError::TaskNotRegistered {
                    task_name: task_name.to_string(),
                });
            }
        }

        self.spawn_by_name_internal(&self.pool, task_name, params, options)
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
    ) -> DurableResult<SpawnResult>
    where
        T: Task<State> + Default,
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
    ) -> DurableResult<SpawnResult>
    where
        T: Task<State> + Default,
        E: Executor<'e, Database = Postgres>,
    {
        let task = T::default();
        self.spawn_by_name_internal(
            executor,
            &task.name(),
            serde_json::to_value(&params)?,
            options,
        )
        .await
    }

    /// Spawn a task by name using a custom executor.
    ///
    /// The task must be registered before spawning.
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.client.spawn",
            skip(self, executor, params, options),
            fields(queue, task_name = %task_name)
        )
    )]
    pub async fn spawn_by_name_with<'e, E>(
        &self,
        executor: E,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> DurableResult<SpawnResult>
    where
        E: Executor<'e, Database = Postgres>,
    {
        // Validate that the task is registered
        {
            let registry = self.registry.read().await;
            if !registry.contains_key(task_name) {
                return Err(DurableError::TaskNotRegistered {
                    task_name: task_name.to_string(),
                });
            }
        }

        self.spawn_by_name_internal(executor, task_name, params, options)
            .await
    }

    /// Spawn a task by name WITHOUT registry validation.
    ///
    /// Use this when you know the task will be registered on the worker,
    /// but the spawning client doesn't have the task registered locally.
    pub async fn spawn_by_name_unchecked(
        &self,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> DurableResult<SpawnResult> {
        self.spawn_by_name_internal(&self.pool, task_name, params, options)
            .await
    }

    /// Spawn a task by name using a custom executor WITHOUT registry validation.
    ///
    /// Use this when you know the task will be registered on the worker,
    /// but the spawning client doesn't have the task registered locally.
    pub async fn spawn_by_name_unchecked_with<'e, E>(
        &self,
        executor: E,
        task_name: &str,
        params: JsonValue,
        options: SpawnOptions,
    ) -> DurableResult<SpawnResult>
    where
        E: Executor<'e, Database = Postgres>,
    {
        self.spawn_by_name_internal(executor, task_name, params, options)
            .await
    }

    /// Internal spawn implementation without registry validation.
    #[allow(unused_mut)] // mut is needed when telemetry feature is enabled
    async fn spawn_by_name_internal<'e, E>(
        &self,
        executor: E,
        task_name: &str,
        params: JsonValue,
        mut options: SpawnOptions,
    ) -> DurableResult<SpawnResult>
    where
        E: Executor<'e, Database = Postgres>,
    {
        // Validate user headers don't use reserved prefix
        validate_headers(&options.headers)?;

        // Inject trace context into headers for distributed tracing
        #[cfg(feature = "telemetry")]
        {
            let headers = options.headers.get_or_insert_with(HashMap::new);
            crate::telemetry::inject_trace_context(headers);
        }

        #[cfg(feature = "telemetry")]
        tracing::Span::current().record("queue", &self.queue_name);

        // Apply defaults if not set
        let max_attempts = options
            .max_attempts
            .unwrap_or(self.spawn_defaults.max_attempts);
        let options = SpawnOptions {
            retry_strategy: options
                .retry_strategy
                .or_else(|| self.spawn_defaults.retry_strategy.clone()),
            cancellation: options
                .cancellation
                .or_else(|| self.spawn_defaults.cancellation.clone()),
            ..options
        };

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

        #[cfg(feature = "telemetry")]
        crate::telemetry::record_task_spawned(&self.queue_name, task_name);

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
    pub async fn create_queue(&self, queue_name: Option<&str>) -> DurableResult<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        let query = "SELECT durable.create_queue($1)";
        sqlx::query(query).bind(queue).execute(&self.pool).await?;
        Ok(())
    }

    /// Drop a queue and all its data
    pub async fn drop_queue(&self, queue_name: Option<&str>) -> DurableResult<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        let query = "SELECT durable.drop_queue($1)";
        sqlx::query(query).bind(queue).execute(&self.pool).await?;
        Ok(())
    }

    /// List all queues
    pub async fn list_queues(&self) -> DurableResult<Vec<String>> {
        let query = "SELECT queue_name FROM durable.list_queues()";
        let rows: Vec<(String,)> = sqlx::query_as(query).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    /// Emit an event to a queue (defaults to this client's queue)
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.client.emit_event",
            skip(self, payload),
            fields(queue, event_name = %event_name)
        )
    )]
    pub async fn emit_event<T: Serialize>(
        &self,
        event_name: &str,
        payload: &T,
        queue_name: Option<&str>,
    ) -> DurableResult<()> {
        self.emit_event_with(&self.pool, event_name, payload, queue_name)
            .await
    }

    /// Emit an event with a custom executor (e.g., a transaction).
    ///
    /// This allows you to atomically emit an event as part of a larger transaction.
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
    /// client.emit_event_with(&mut *tx, "order_created", &order_id, None).await?;
    ///
    /// tx.commit().await?;
    /// ```
    #[cfg_attr(
        feature = "telemetry",
        tracing::instrument(
            name = "durable.client.emit_event",
            skip(self, executor, payload),
            fields(queue, event_name = %event_name)
        )
    )]
    pub async fn emit_event_with<'e, T, E>(
        &self,
        executor: E,
        event_name: &str,
        payload: &T,
        queue_name: Option<&str>,
    ) -> DurableResult<()>
    where
        T: Serialize,
        E: Executor<'e, Database = Postgres>,
    {
        if event_name.is_empty() {
            return Err(DurableError::InvalidEventName {
                reason: "event_name must be non-empty".to_string(),
            });
        }

        let queue = queue_name.unwrap_or(&self.queue_name);

        #[cfg(feature = "telemetry")]
        tracing::Span::current().record("queue", queue);

        let payload_json = serde_json::to_value(payload)?;

        let query = "SELECT durable.emit_event($1, $2, $3)";
        sqlx::query(query)
            .bind(queue)
            .bind(event_name)
            .bind(&payload_json)
            .execute(executor)
            .await?;

        #[cfg(feature = "telemetry")]
        crate::telemetry::record_event_emitted(queue, event_name);

        Ok(())
    }

    /// Cancel a task by ID. Running tasks will be cancelled at
    /// their next checkpoint or heartbeat.
    pub async fn cancel_task(&self, task_id: Uuid, queue_name: Option<&str>) -> DurableResult<()> {
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
    ///
    /// # Errors
    ///
    /// Returns [`DurableError::InvalidConfiguration`] if `claim_timeout` is less than 1 second.
    pub async fn start_worker(&self, options: WorkerOptions) -> DurableResult<Worker> {
        if options.claim_timeout < Duration::from_secs(1) {
            return Err(DurableError::InvalidConfiguration {
                reason: "claim_timeout must be at least 1 second".to_string(),
            });
        }

        Ok(Worker::start(
            self.pool.clone(),
            self.queue_name.clone(),
            self.registry.clone(),
            options,
            self.state.clone(),
            self.spawn_defaults.clone(),
        )
        .await)
    }

    /// Close the client. Closes the pool if owned.
    pub async fn close(self) {
        if self.owns_pool {
            self.pool.close().await;
        }
    }
}
