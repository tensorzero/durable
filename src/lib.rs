mod client;
mod context;
mod error;
mod task;
mod types;
mod worker;

// Re-export public API
pub use client::{Durable, DurableBuilder};
pub use context::TaskContext;
pub use error::{ControlFlow, TaskError, TaskResult};
pub use task::Task;
pub use types::{
    CancellationPolicy, ClaimedTask, RetryStrategy, SpawnOptions, SpawnResult, WorkerOptions,
};
pub use worker::Worker;

// Re-export async_trait for convenience
pub use async_trait::async_trait;

/// Returns the migrator for running database migrations.
pub fn make_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("src/postgres/migrations")
}
