//! A Rust SDK for building durable, fault-tolerant workflows using PostgreSQL.
//!
//! # Overview
//!
//! `durable` enables you to write long-running tasks that checkpoint their progress,
//! suspend for events or time delays, and recover gracefully from failures. Unlike
//! exception-based durable execution systems, this SDK uses Rust's `Result` type
//! for suspension control flow.
//!
//! # Quick Start
//!
//! ```ignore
//! use durable::{Durable, Task, TaskContext, TaskResult, WorkerOptions, async_trait};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyParams { value: i32 }
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyOutput { result: i32 }
//!
//! struct MyTask;
//!
//! #[async_trait]
//! impl Task<()> for MyTask {
//!     fn name() -> Cow<'static, str> { Cow::Borrowed("my-task") }
//!     type Params = MyParams;
//!     type Output = MyOutput;
//!
//!     async fn run(params: Self::Params, mut ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
//!         let doubled = ctx.step("double", || async {
//!             Ok(params.value * 2)
//!         }).await?;
//!
//!         Ok(MyOutput { result: doubled })
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let client = Durable::new("postgres://localhost/myapp").await?;
//!     client.register::<MyTask>().await?;
//!
//!     client.spawn::<MyTask>(MyParams { value: 21 }).await?;
//!
//!     let worker = client.start_worker(WorkerOptions::default()).await;
//!     // ... worker processes tasks until shutdown
//!     worker.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! # Application State
//!
//! Tasks can receive application state (like HTTP clients, database pools) via the
//! generic `State` type parameter:
//!
//! ```ignore
//! #[derive(Clone)]
//! struct AppState {
//!     http_client: reqwest::Client,
//! }
//!
//! struct FetchTask;
//!
//! #[async_trait]
//! impl Task<AppState> for FetchTask {
//!     fn name() -> Cow<'static, str> { Cow::Borrowed("fetch") }
//!     type Params = String;
//!     type Output = String;
//!
//!     async fn run(url: Self::Params, mut ctx: TaskContext, state: AppState) -> TaskResult<Self::Output> {
//!         ctx.step("fetch", || async {
//!             state.http_client.get(&url).send().await?.text().await
//!                 .map_err(|e| anyhow::anyhow!(e))
//!         }).await
//!     }
//! }
//!
//! // Build client with state
//! let app_state = AppState { http_client: reqwest::Client::new() };
//! let client = Durable::builder()
//!     .database_url("postgres://localhost/myapp")
//!     .build_with_state(app_state)
//!     .await?;
//! ```
//!
//! # Key Types
//!
//! - [`Durable`] - Main client for spawning tasks and managing queues
//! - [`Task`] - Trait to implement for defining task types
//! - [`TaskContext`] - Passed to task execution, provides `step`, `sleep_for`, `await_event`, etc.
//! - [`Worker`] - Background processor that executes tasks from the queue

mod client;
mod context;
mod error;
mod task;
#[cfg(feature = "telemetry")]
pub mod telemetry;
mod types;
mod worker;

// Re-export public API
pub use client::{Durable, DurableBuilder};
pub use context::TaskContext;
pub use error::{ControlFlow, DurableError, DurableResult, TaskError, TaskResult};
pub use task::Task;
pub use types::{
    CancellationPolicy, ClaimedTask, RetryStrategy, SpawnOptions, SpawnResult, TaskFailureInfo,
    TaskHandle, TaskStatus, WaitOptions, WorkerOptions,
};
pub use worker::Worker;

// Re-export async_trait for convenience
pub use async_trait::async_trait;

/// Static migrator for running database migrations.
/// Used by #[sqlx::test] and for manual migration runs.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("src/postgres/migrations");
