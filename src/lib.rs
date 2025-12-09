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
//! impl Task for MyTask {
//!     const NAME: &'static str = "my-task";
//!     type Params = MyParams;
//!     type Output = MyOutput;
//!
//!     async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
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
//!     client.register::<MyTask>().await;
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
pub use error::{ControlFlow, TaskError, TaskResult};
pub use task::Task;
pub use types::{
    CancellationPolicy, ClaimedTask, RetryStrategy, SpawnOptions, SpawnResult, TaskHandle,
    WorkerOptions,
};
pub use worker::Worker;

// Re-export async_trait for convenience
pub use async_trait::async_trait;

/// Static migrator for running database migrations.
/// Used by #[sqlx::test] and for manual migration runs.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("src/postgres/migrations");
