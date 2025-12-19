#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{advance_time, count_runs_for_task, set_fake_time, wait_for_task_terminal};
use common::tasks::{FailingParams, FailingTask};
use durable::{Durable, MIGRATOR, RetryStrategy, SpawnOptions, WorkerOptions};
use sqlx::{AssertSqlSafe, PgPool};
use std::time::Duration;

async fn create_client(pool: PgPool, queue_name: &str) -> Durable {
    Durable::builder()
        .pool(pool)
        .queue_name(queue_name)
        .build()
        .await
        .expect("Failed to create Durable client")
}

// ============================================================================
// Retry Strategy Tests
// ============================================================================

/// Test that RetryStrategy::None creates no retry run.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_retry_strategy_none_no_retry(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "retry_none").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await.unwrap();

    // Spawn task with no retry strategy
    let spawn_result = client
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "intentional failure".to_string(),
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::None),
                max_attempts: Some(1),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    let terminal = wait_for_task_terminal(
        &pool,
        "retry_none",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("failed".to_string()));

    // Verify only 1 run was created (no retry)
    let run_count = count_runs_for_task(&pool, "retry_none", spawn_result.task_id).await?;
    assert_eq!(run_count, 1, "Should have exactly 1 run (no retry)");

    Ok(())
}

/// Test that RetryStrategy::Fixed creates retry at T + base_seconds.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_retry_strategy_fixed_delay(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "retry_fixed").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await.unwrap();

    // Set fake time for deterministic testing
    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    // Spawn task with fixed retry strategy (5 second delay)
    let spawn_result = client
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "intentional failure".to_string(),
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::Fixed { base_seconds: 5 }),
                max_attempts: Some(2),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for first attempt to fail
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Check that a retry run was created
    let run_count = count_runs_for_task(&pool, "retry_fixed", spawn_result.task_id).await?;
    assert_eq!(run_count, 2, "Should have 2 runs (original + retry)");

    // Check the retry is scheduled for ~5 seconds later
    let query = AssertSqlSafe(
        "SELECT available_at FROM durable.r_retry_fixed WHERE task_id = $1 AND attempt = 2"
            .to_string(),
    );
    let result: (chrono::DateTime<chrono::Utc>,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    let delay = (result.0 - start_time).num_seconds();
    assert!(
        (4..=6).contains(&delay),
        "Retry should be scheduled ~5 seconds later, got {} seconds",
        delay
    );

    // Advance time past the retry delay
    advance_time(&pool, 6).await?;

    // Wait for retry to complete (and fail again, hitting max_attempts)
    let terminal = wait_for_task_terminal(
        &pool,
        "retry_fixed",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("failed".to_string()));

    Ok(())
}

/// Test that RetryStrategy::Exponential increases delays correctly.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_retry_strategy_exponential_backoff(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "retry_exp").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    // Spawn task with exponential retry (base=2, factor=2)
    // Delays should be: 2, 4, 8, ... seconds
    let spawn_result = client
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "intentional failure".to_string(),
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::Exponential {
                    base_seconds: 2,
                    factor: 2.0,
                    max_seconds: 100,
                }),
                max_attempts: Some(3),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for first attempt to fail
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Check first retry delay (should be ~2 seconds)
    let query = AssertSqlSafe(
        "SELECT available_at FROM durable.r_retry_exp WHERE task_id = $1 AND attempt = 2"
            .to_string(),
    );
    let result: (chrono::DateTime<chrono::Utc>,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    let delay1 = (result.0 - start_time).num_seconds();
    assert!(
        (1..=3).contains(&delay1),
        "First retry should be ~2 seconds, got {}",
        delay1
    );

    // Advance time and trigger second attempt
    advance_time(&pool, 3).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Check second retry delay (should be ~4 seconds from attempt 2's time)
    let run_count = count_runs_for_task(&pool, "retry_exp", spawn_result.task_id).await?;
    assert_eq!(run_count, 3, "Should have 3 runs");

    // Advance time to complete all retries
    advance_time(&pool, 10).await?;

    let terminal = wait_for_task_terminal(
        &pool,
        "retry_exp",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("failed".to_string()));

    Ok(())
}

/// Test that max_attempts is honored and task fails permanently after N attempts.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_max_attempts_honored(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "retry_max").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    // Spawn task with max_attempts = 3
    let spawn_result = client
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "intentional failure".to_string(),
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::Fixed { base_seconds: 0 }),
                max_attempts: Some(3),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for all attempts to complete
    let terminal = wait_for_task_terminal(
        &pool,
        "retry_max",
        spawn_result.task_id,
        Duration::from_secs(10),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("failed".to_string()));

    // Verify exactly 3 runs were created
    let run_count = count_runs_for_task(&pool, "retry_max", spawn_result.task_id).await?;
    assert_eq!(run_count, 3, "Should have exactly 3 runs (max_attempts)");

    // Verify the task's attempts counter
    let query =
        AssertSqlSafe("SELECT attempts FROM durable.t_retry_max WHERE task_id = $1".to_string());
    let result: (i32,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, 3, "Task should show 3 attempts");

    Ok(())
}
