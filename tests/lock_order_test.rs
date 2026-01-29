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
use durable::{Durable, DurableEventPayload, MIGRATOR, RetryStrategy, SpawnOptions, WorkerOptions};
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::{AssertSqlSafe, Connection, PgPool};
use std::time::{Duration, Instant};
use uuid::Uuid;

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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .unwrap();

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
            {
                let mut opts = SpawnOptions::default();
                opts.retry_strategy = Some(RetryStrategy::Fixed {
                    base_delay: Duration::from_secs(0),
                });
                opts.max_attempts = Some(2);
                opts
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .unwrap();

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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .unwrap();

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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            concurrency: 5,
            ..Default::default()
        })
        .await
        .unwrap();

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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for task to start sleeping (awaiting event)
    tokio::time::sleep(Duration::from_millis(200)).await;

    let state = get_task_state(&pool, "lock_emit", spawn_result.task_id).await?;
    assert_eq!(
        state,
        Some("sleeping".to_string()),
        "Task should be sleeping waiting for event"
    );

    // Emit the event - payload must use DurableEventPayload format
    let payload = serde_json::to_value(DurableEventPayload {
        inner: json!("hello"),
        metadata: json!({}),
    })
    .unwrap();
    sqlx::query("SELECT durable.emit_event($1, $2, $3)")
        .bind("lock_emit")
        .bind("test_event")
        .bind(&payload)
        .execute(&pool)
        .await?;

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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            concurrency: 5,
            ..Default::default()
        })
        .await
        .unwrap();

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

    // Cancel one task while emitting the event - payload must use DurableEventPayload format
    let cancel_task_id = task_ids[0];
    let payload = serde_json::to_value(DurableEventPayload {
        inner: json!("wakeup"),
        metadata: json!({}),
    })
    .unwrap();
    let emit_handle = tokio::spawn({
        let test_pool = test_pool.clone();
        async move {
            sqlx::query("SELECT durable.emit_event($1, $2, $3)")
                .bind("lock_emit_cancel")
                .bind("shared_event")
                .bind(&payload)
                .execute(&test_pool)
                .await
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

/// Regression test: claim_task uses SKIP LOCKED to avoid deadlock with emit_event.
///
/// emit_event locks tasks first (locked_tasks CTE with FOR UPDATE), then updates runs.
/// claim_task joins runs+tasks with FOR UPDATE SKIP LOCKED.
///
/// This test verifies that when a task is locked (simulating emit_event holding the lock),
/// claim_task skips that task instead of blocking (which would cause deadlock).
///
/// We make the test deterministic by:
/// - Creating a task and making it claimable (pending state)
/// - Holding a FOR UPDATE lock on the task row in a separate connection
/// - Calling claim_task - it should complete immediately with 0 results (not block)
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_claim_task_skips_locked_tasks_no_deadlock(pool: PgPool) -> sqlx::Result<()> {
    let queue = "skip_locked_test";

    // Setup: Create queue and spawn a task
    sqlx::query("SELECT durable.create_queue($1)")
        .bind(queue)
        .execute(&pool)
        .await?;

    let (task_id, run_id): (Uuid, Uuid) =
        sqlx::query_as("SELECT task_id, run_id FROM durable.spawn_task($1, $2, $3, $4)")
            .bind(queue)
            .bind("test-task")
            .bind(serde_json::json!({}))
            .bind(serde_json::json!({}))
            .fetch_one(&pool)
            .await?;

    // Verify task is pending and claimable
    let state = get_task_state(&pool, queue, task_id).await?;
    assert_eq!(state, Some("pending".to_string()));

    // Get connect options from pool for creating separate connections
    let connect_opts: PgConnectOptions = (*pool.connect_options()).clone();

    // Open lock connection and hold FOR UPDATE lock on the task row
    // This simulates emit_event's locked_tasks CTE holding the lock mid-transaction
    let lock_opts = connect_opts.clone().application_name("durable-task-locker");
    let mut lock_conn = PgConnection::connect_with(&lock_opts).await?;

    sqlx::query("BEGIN").execute(&mut lock_conn).await?;
    sqlx::query(AssertSqlSafe(format!(
        "SELECT 1 FROM durable.t_{} WHERE task_id = $1 FOR UPDATE",
        queue
    )))
    .bind(task_id)
    .execute(&mut lock_conn)
    .await?;

    // Wait until the lock is confirmed held by checking pg_stat_activity
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state FROM pg_stat_activity WHERE application_name = $1")
                .bind("durable-task-locker")
                .fetch_optional(&pool)
                .await?;

        if let Some((ref state,)) = row
            && state == "idle in transaction"
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Lock connection did not reach expected state"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Now call claim_task from another connection
    // If SKIP LOCKED works correctly, it should complete immediately with 0 results
    // If SKIP LOCKED didn't apply to the task table, it would block and timeout
    let claim_opts = connect_opts.clone().application_name("durable-claimer");
    let mut claim_conn = PgConnection::connect_with(&claim_opts).await?;

    // Set a short statement timeout - if claim_task blocks, it will fail
    sqlx::query("SET statement_timeout = '500ms'")
        .execute(&mut claim_conn)
        .await?;

    let claim_result: Vec<(Uuid,)> =
        sqlx::query_as("SELECT run_id FROM durable.claim_task($1, $2, $3, $4)")
            .bind(queue)
            .bind("worker")
            .bind(60)
            .bind(1)
            .fetch_all(&mut claim_conn)
            .await?;

    // claim_task should have completed (not timed out) and returned 0 results
    // because the task was locked and SKIP LOCKED caused it to be skipped
    assert!(
        claim_result.is_empty(),
        "claim_task should skip locked task, but got {} results",
        claim_result.len()
    );

    // Reset statement timeout
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut claim_conn)
        .await?;

    // Release the lock
    sqlx::query("ROLLBACK").execute(&mut lock_conn).await?;
    drop(lock_conn);

    // Now claim_task should be able to claim the task
    let claim_result2: Vec<(Uuid,)> =
        sqlx::query_as("SELECT run_id FROM durable.claim_task($1, $2, $3, $4)")
            .bind(queue)
            .bind("worker")
            .bind(60)
            .bind(1)
            .fetch_all(&mut claim_conn)
            .await?;

    assert_eq!(
        claim_result2.len(),
        1,
        "claim_task should claim the task after lock is released"
    );
    assert_eq!(claim_result2[0].0, run_id);

    Ok(())
}
