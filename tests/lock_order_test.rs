#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Tests for lock ordering in SQL functions.
//!
//! These tests verify that functions that touch both tasks and runs
//! acquire locks in a consistent order (task first, then run) to prevent
//! deadlocks.
//!
//! The lock ordering pattern is:
//! 1. Find the task_id (no lock)
//! 2. Lock the task FOR UPDATE
//! 3. Lock the run FOR UPDATE
//!
//! Without this ordering, two concurrent transactions could deadlock:
//! - Transaction A: locks run, waits for task
//! - Transaction B: locks task, waits for run

mod common;

use common::helpers::{get_task_state, single_conn_pool, wait_for_task_terminal};
use common::tasks::{
    DoubleParams, DoubleTask, FailingParams, FailingTask, SleepParams, SleepingTask,
};
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
// Lock Ordering Tests
// ============================================================================

/// Test that complete_run works correctly with the lock ordering.
/// Completes a task and verifies the task reaches completed state.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_complete_run_with_lock_ordering(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lock_complete").await;
    client.create_queue(None).await.unwrap();
    client.register::<DoubleTask>().await.unwrap();

    let spawn_result = client
        .spawn::<DoubleTask>(DoubleParams { value: 21 })
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
        "lock_complete",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    Ok(())
}

/// Test that fail_run works correctly with the lock ordering.
/// Fails a task and verifies it eventually reaches failed state after retries.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_fail_run_with_lock_ordering(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lock_fail").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await.unwrap();

    let spawn_result = client
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "intentional failure".to_string(),
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::Fixed { base_seconds: 0 }),
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

    let terminal = wait_for_task_terminal(
        &pool,
        "lock_fail",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("failed".to_string()));

    Ok(())
}

/// Test that sleep_for works correctly with the lock ordering.
/// Sleeps and verifies the task suspends and then completes.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_sleep_for_with_lock_ordering(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lock_sleep").await;
    client.create_queue(None).await.unwrap();
    client.register::<SleepingTask>().await.unwrap();

    let spawn_result = client
        .spawn::<SleepingTask>(SleepParams { seconds: 1 })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for task to complete
    let terminal = wait_for_task_terminal(
        &pool,
        "lock_sleep",
        spawn_result.task_id,
        Duration::from_secs(10),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    Ok(())
}

/// Test concurrent complete and cancel operations don't deadlock.
/// This would deadlock if lock ordering were inconsistent.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_concurrent_complete_and_cancel(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lock_conc_cc").await;
    client.create_queue(None).await.unwrap();
    client.register::<SleepingTask>().await.unwrap();

    // Spawn several tasks
    let mut task_ids = Vec::new();
    for _ in 0..5 {
        let spawn_result = client
            .spawn::<SleepingTask>(SleepParams { seconds: 1 })
            .await
            .expect("Failed to spawn task");
        task_ids.push(spawn_result.task_id);
    }

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 5,
            ..Default::default()
        })
        .await;

    // Let tasks start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cancel some tasks while others are completing
    for (i, task_id) in task_ids.iter().enumerate() {
        if i % 2 == 0 {
            // Ignore errors - task might already be completed
            let _ = client.cancel_task(*task_id, None).await;
        }
    }

    // Wait for all tasks to reach terminal state
    for task_id in &task_ids {
        let _ =
            wait_for_task_terminal(&pool, "lock_conc_cc", *task_id, Duration::from_secs(5)).await?;
    }

    worker.shutdown().await;

    // All tasks should be in terminal state (completed or cancelled)
    for task_id in &task_ids {
        let state = get_task_state(&pool, "lock_conc_cc", *task_id).await?;
        assert!(
            state == Some("completed".to_string()) || state == Some("cancelled".to_string()),
            "Task should be terminal, got {:?}",
            state
        );
    }

    Ok(())
}

/// Test that emit_event wakes sleeping tasks correctly with lock ordering.
/// This tests the emit_event function's locked_tasks CTE.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_with_lock_ordering(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{EventWaitParams, EventWaitingTask};

    let client = create_client(pool.clone(), "lock_emit").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    let spawn_result = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "test_event".to_string(),
            timeout_seconds: Some(30),
        })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for task to start sleeping (awaiting event)
    tokio::time::sleep(Duration::from_millis(200)).await;

    let state = get_task_state(&pool, "lock_emit", spawn_result.task_id).await?;
    assert_eq!(
        state,
        Some("sleeping".to_string()),
        "Task should be sleeping waiting for event"
    );

    // Emit the event
    let emit_query = AssertSqlSafe(
        "SELECT durable.emit_event('lock_emit', 'test_event', '\"hello\"'::jsonb)".to_string(),
    );
    sqlx::query(emit_query).execute(&pool).await?;

    // Wait for task to complete
    let terminal = wait_for_task_terminal(
        &pool,
        "lock_emit",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    Ok(())
}

/// Test concurrent emit and cancel operations.
/// This tests that emit_event's locked_tasks CTE properly handles
/// tasks being cancelled concurrently.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_concurrent_emit_and_cancel(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{EventWaitParams, EventWaitingTask};

    // Use single-conn pool to ensure deterministic event emission
    let test_pool = single_conn_pool(&pool).await;

    let client = create_client(pool.clone(), "lock_emit_cancel").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Spawn multiple tasks waiting for the same event
    let mut task_ids = Vec::new();
    for _ in 0..3 {
        let spawn_result = client
            .spawn::<EventWaitingTask>(EventWaitParams {
                event_name: "shared_event".to_string(),
                timeout_seconds: Some(30),
            })
            .await
            .expect("Failed to spawn task");
        task_ids.push(spawn_result.task_id);
    }

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 5,
            ..Default::default()
        })
        .await;

    // Wait for all tasks to start sleeping
    tokio::time::sleep(Duration::from_millis(500)).await;

    for task_id in &task_ids {
        let state = get_task_state(&pool, "lock_emit_cancel", *task_id).await?;
        assert_eq!(
            state,
            Some("sleeping".to_string()),
            "Task should be sleeping"
        );
    }

    // Cancel one task while emitting the event
    let cancel_task_id = task_ids[0];
    let emit_handle = tokio::spawn({
        let test_pool = test_pool.clone();
        async move {
            let emit_query = AssertSqlSafe(
                "SELECT durable.emit_event('lock_emit_cancel', 'shared_event', '\"wakeup\"'::jsonb)"
                    .to_string(),
            );
            sqlx::query(emit_query).execute(&test_pool).await
        }
    });

    // Cancel concurrently
    let _ = client.cancel_task(cancel_task_id, None).await;

    // Wait for emit to complete
    emit_handle.await.unwrap()?;

    // Wait for all tasks to reach terminal state
    for task_id in &task_ids {
        let _ = wait_for_task_terminal(&pool, "lock_emit_cancel", *task_id, Duration::from_secs(5))
            .await?;
    }

    worker.shutdown().await;

    // The cancelled task should be cancelled, others should be completed
    let cancelled_state = get_task_state(&pool, "lock_emit_cancel", cancel_task_id).await?;
    assert_eq!(
        cancelled_state,
        Some("cancelled".to_string()),
        "Cancelled task should be cancelled"
    );

    for task_id in &task_ids[1..] {
        let state = get_task_state(&pool, "lock_emit_cancel", *task_id).await?;
        assert_eq!(
            state,
            Some("completed".to_string()),
            "Non-cancelled task should be completed"
        );
    }

    Ok(())
}
