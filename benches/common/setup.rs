#![allow(clippy::unwrap_used, clippy::expect_used)]

use durable::{Durable, DurableBuilder, MIGRATOR, WorkerOptions};
use sqlx::{AssertSqlSafe, PgPool};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Counter for unique queue names across benchmark iterations
static QUEUE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Context for running a single benchmark iteration.
/// Provides isolated database state via unique queue names.
pub struct BenchContext {
    pub pool: PgPool,
    pub client: Durable,
    pub queue_name: String,
}

impl BenchContext {
    /// Create a new benchmark context with a unique queue.
    /// Uses DATABASE_URL environment variable (same as tests).
    pub async fn new() -> Self {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5436/test".to_string());

        let pool = PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to database");

        // Run migrations once per connection (idempotent)
        MIGRATOR.run(&pool).await.expect("Failed to run migrations");

        // Generate unique queue name for this benchmark run
        let counter = QUEUE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let queue_name = format!("bench_{}", counter);

        let client = DurableBuilder::new()
            .pool(pool.clone())
            .queue_name(&queue_name)
            .build()
            .await
            .expect("Failed to create Durable client");

        client
            .create_queue(None)
            .await
            .expect("Failed to create queue");

        Self {
            pool,
            client,
            queue_name,
        }
    }

    /// Create a new Durable client using the same pool and queue.
    /// Useful for creating multiple workers.
    #[allow(dead_code)]
    pub async fn new_client(&self) -> Durable {
        DurableBuilder::new()
            .pool(self.pool.clone())
            .queue_name(&self.queue_name)
            .build()
            .await
            .expect("Failed to create Durable client")
    }

    /// Clean up the queue after benchmark
    pub async fn cleanup(self) {
        self.client
            .drop_queue(None)
            .await
            .expect("Failed to drop queue");
    }
}

/// Helper to wait for a specific number of tasks to reach a terminal state.
pub async fn wait_for_tasks_complete(
    pool: &PgPool,
    queue: &str,
    expected_count: usize,
    timeout_secs: u64,
) -> bool {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    loop {
        let query = AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM durable.t_{} WHERE state IN ('completed', 'failed', 'cancelled')",
            queue
        ));
        let (count,): (i64,) = sqlx::query_as(query)
            .fetch_one(pool)
            .await
            .expect("Failed to count tasks");

        if count as usize >= expected_count {
            return true;
        }

        if start.elapsed() > timeout {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Helper to clear completed tasks from the queue for clean iteration.
#[allow(dead_code)]
pub async fn clear_completed_tasks(pool: &PgPool, queue: &str) {
    let query = AssertSqlSafe(format!(
        "DELETE FROM durable.t_{} WHERE state IN ('completed', 'failed', 'cancelled')",
        queue
    ));
    sqlx::query(query)
        .execute(pool)
        .await
        .expect("Failed to clear completed tasks");
}

/// Default worker options optimized for benchmarking
pub fn bench_worker_options(concurrency: usize, claim_timeout: u64) -> WorkerOptions {
    WorkerOptions {
        worker_id: None,
        concurrency,
        poll_interval: 0.001, // Very fast polling for accurate timing
        claim_timeout,
        batch_size: None, // Use default (= concurrency)
        fatal_on_lease_timeout: false,
    }
}
