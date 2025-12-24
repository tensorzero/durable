#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{get_task_state, wait_for_task_terminal};
use common::tasks::{EventEmitterParams, EventEmitterTask, EventWaitParams, EventWaitingTask};
use durable::{Durable, MIGRATOR, RetryStrategy, SpawnOptions, WorkerOptions};
use serde_json::json;
use sqlx::postgres::PgConnectOptions;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool};
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
// Event Tests
// ============================================================================

/// Test that emit_event wakes a task blocked on await_event.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_wakes_waiter(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_wake").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Spawn task that waits for an event
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

    // Wait for task to start waiting
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Task should be sleeping (waiting for event)
    let state = get_task_state(&pool, "event_wake", spawn_result.task_id).await?;
    assert!(
        state == Some("sleeping".to_string()) || state == Some("running".to_string()),
        "Task should be sleeping or running while waiting for event, got {:?}",
        state
    );

    // Emit the event
    client
        .emit_event("test_event", &json!({"data": "test_value"}), None)
        .await
        .expect("Failed to emit event");

    // Wait for task to complete
    let terminal = wait_for_task_terminal(
        &pool,
        "event_wake",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Verify the payload was received
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_wake WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"data": "test_value"}));

    Ok(())
}

/// Test that await_event returns immediately if event already exists.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_already_emitted_returns_immediately(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_pre").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Emit the event BEFORE spawning the task
    client
        .emit_event("pre_event", &json!({"pre": "emitted"}), None)
        .await
        .expect("Failed to emit event");

    // Now spawn task that waits for the already-emitted event
    let spawn_result = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "pre_event".to_string(),
            timeout_seconds: Some(5),
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

    // Task should complete quickly since event exists
    let terminal = wait_for_task_terminal(
        &pool,
        "event_pre",
        spawn_result.task_id,
        Duration::from_secs(2),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Verify the pre-emitted payload was received
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_pre WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"pre": "emitted"}));

    Ok(())
}

/// Test that await_event times out correctly.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_timeout_triggers(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_timeout").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Spawn task with short timeout, never emit event
    let spawn_result = client
        .spawn_with_options::<EventWaitingTask>(
            EventWaitParams {
                event_name: "never_emitted".to_string(),
                timeout_seconds: Some(1), // 1 second timeout
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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for task to fail due to timeout
    let terminal = wait_for_task_terminal(
        &pool,
        "event_timeout",
        spawn_result.task_id,
        Duration::from_secs(10),
    )
    .await?;
    worker.shutdown().await;

    // Task should fail due to timeout (not completed)
    assert_eq!(terminal, Some("failed".to_string()));

    Ok(())
}

/// Test that multiple tasks waiting for the same event all wake up.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_multiple_waiters_same_event(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_multi").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Spawn multiple tasks waiting for the same event
    let task1 = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "shared_event".to_string(),
            timeout_seconds: Some(30),
        })
        .await
        .expect("Failed to spawn task 1");

    let task2 = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "shared_event".to_string(),
            timeout_seconds: Some(30),
        })
        .await
        .expect("Failed to spawn task 2");

    let task3 = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "shared_event".to_string(),
            timeout_seconds: Some(30),
        })
        .await
        .expect("Failed to spawn task 3");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            concurrency: 3,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for all tasks to start waiting
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Emit the shared event
    client
        .emit_event("shared_event", &json!({"shared": true}), None)
        .await
        .expect("Failed to emit event");

    // All tasks should complete
    let term1 =
        wait_for_task_terminal(&pool, "event_multi", task1.task_id, Duration::from_secs(5)).await?;
    let term2 =
        wait_for_task_terminal(&pool, "event_multi", task2.task_id, Duration::from_secs(5)).await?;
    let term3 =
        wait_for_task_terminal(&pool, "event_multi", task3.task_id, Duration::from_secs(5)).await?;
    worker.shutdown().await;

    assert_eq!(term1, Some("completed".to_string()));
    assert_eq!(term2, Some("completed".to_string()));
    assert_eq!(term3, Some("completed".to_string()));

    Ok(())
}

/// Test that event payload is preserved on retry via checkpoint.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_payload_preserved_on_retry(pool: PgPool) -> sqlx::Result<()> {
    // This test verifies that if a task receives an event, fails, and retries,
    // the event payload is cached in a checkpoint and reused.
    // We use a custom task that fails after receiving the event.

    use common::tasks::{EventThenFailParams, EventThenFailTask, reset_event_then_fail_state};

    let client = create_client(pool.clone(), "event_retry").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventThenFailTask>().await.unwrap();

    reset_event_then_fail_state();

    let spawn_result = client
        .spawn_with_options::<EventThenFailTask>(
            EventThenFailParams {
                event_name: "retry_event".to_string(),
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::Fixed {
                    base_delay: Duration::from_secs(0),
                }),
                max_attempts: Some(2),
                ..Default::default()
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

    // Wait for task to start waiting for event
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Emit the event
    client
        .emit_event("retry_event", &json!({"original": "payload"}), None)
        .await
        .expect("Failed to emit event");

    // Task should complete on second attempt
    let terminal = wait_for_task_terminal(
        &pool,
        "event_retry",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Verify the original payload was preserved
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_retry WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"original": "payload"}));

    Ok(())
}

/// Test that emitting an event with the same name keeps the first payload (first-writer-wins).
/// Subsequent emits for the same event are no-ops to maintain consistency with lost-wakeup prevention.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_first_writer_wins(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_dedup").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Emit the event twice with different payloads
    client
        .emit_event("dedup_event", &json!({"version": "first"}), None)
        .await
        .expect("Failed to emit first event");

    client
        .emit_event("dedup_event", &json!({"version": "second"}), None)
        .await
        .expect("Failed to emit second event");

    // Spawn task to receive the event
    let spawn_result = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "dedup_event".to_string(),
            timeout_seconds: Some(5),
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

    let terminal = wait_for_task_terminal(
        &pool,
        "event_dedup",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Should receive the first payload (first-writer-wins)
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_dedup WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"version": "first"}));

    Ok(())
}

/// Test that a task can await multiple distinct events.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_multiple_distinct_events(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{MultiEventParams, MultiEventTask};

    let client = create_client(pool.clone(), "event_distinct").await;
    client.create_queue(None).await.unwrap();
    client.register::<MultiEventTask>().await.unwrap();

    // Spawn task that waits for two events
    let spawn_result = client
        .spawn::<MultiEventTask>(MultiEventParams {
            event1_name: "event_a".to_string(),
            event2_name: "event_b".to_string(),
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

    // Wait for task to start
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Emit both events
    client
        .emit_event("event_a", &json!({"a": 1}), None)
        .await
        .expect("Failed to emit event_a");

    client
        .emit_event("event_b", &json!({"b": 2}), None)
        .await
        .expect("Failed to emit event_b");

    let terminal = wait_for_task_terminal(
        &pool,
        "event_distinct",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Verify both event payloads were received
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_distinct WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    // MultiEventTask returns combined payloads
    let output = result.0;
    assert_eq!(output["event1"], json!({"a": 1}));
    assert_eq!(output["event2"], json!({"b": 2}));

    Ok(())
}

/// Test that subsequent event writes don't propagate to already-woken tasks.
/// When a task is woken by an event, it receives the payload at wake time;
/// later writes to the same event don't update what the already-woken task sees.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_write_does_not_propagate_after_wake(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{EventThenDelayParams, EventThenDelayTask};

    let client = create_client(pool.clone(), "event_no_propagate").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventThenDelayTask>().await.unwrap();

    // Spawn task that waits for event, then delays before completing
    let spawn_result = client
        .spawn::<EventThenDelayTask>(EventThenDelayParams {
            event_name: "propagate_test".to_string(),
            delay_ms: 500, // Delay after receiving event
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

    // Wait for task to start waiting for event
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Emit the first event - this wakes the task
    client
        .emit_event("propagate_test", &json!({"version": "first"}), None)
        .await
        .expect("Failed to emit first event");

    // Wait a bit for the task to wake and start its delay
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Emit a second event with different payload while task is still running
    client
        .emit_event("propagate_test", &json!({"version": "second"}), None)
        .await
        .expect("Failed to emit second event");

    // Wait for task to complete
    let terminal = wait_for_task_terminal(
        &pool,
        "event_no_propagate",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Task should have received the FIRST payload (the one that woke it),
    // not the second one that was emitted while it was running
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_no_propagate WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"version": "first"}));

    Ok(())
}

/// Test that one task can emit an event that another task awaits.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_from_different_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_cross").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();
    client.register::<EventEmitterTask>().await.unwrap();

    // Spawn the waiter task first
    let waiter = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "cross_event".to_string(),
            timeout_seconds: Some(30),
        })
        .await
        .expect("Failed to spawn waiter task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            concurrency: 2,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for waiter to start
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Spawn the emitter task
    let _emitter = client
        .spawn::<EventEmitterTask>(EventEmitterParams {
            event_name: "cross_event".to_string(),
            payload: json!({"from": "emitter_task"}),
        })
        .await
        .expect("Failed to spawn emitter task");

    // Waiter should complete after emitter runs
    let terminal =
        wait_for_task_terminal(&pool, "event_cross", waiter.task_id, Duration::from_secs(5))
            .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Verify the waiter received the payload from emitter
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_cross WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(waiter.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"from": "emitter_task"}));

    Ok(())
}

// ============================================================================
// Transaction Tests for emit_event
// ============================================================================

/// Test that emit_event_with in a committed transaction persists the event.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_with_transaction_commit(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_tx_commit").await;
    client.create_queue(None).await.unwrap();

    // Create a test table
    sqlx::query("CREATE TABLE test_event_orders (id UUID PRIMARY KEY, status TEXT)")
        .execute(&pool)
        .await?;

    let order_id = uuid::Uuid::now_v7();
    let event_name = format!("order_created_{}", order_id);

    // Start a transaction and do both operations
    let mut tx = pool.begin().await?;

    sqlx::query("INSERT INTO test_event_orders (id, status) VALUES ($1, $2)")
        .bind(order_id)
        .bind("pending")
        .execute(&mut *tx)
        .await?;

    client
        .emit_event_with(&mut *tx, &event_name, &json!({"order_id": order_id}), None)
        .await
        .expect("Failed to emit event in transaction");

    tx.commit().await?;

    // Verify both the order and event exist
    let order_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM test_event_orders WHERE id = $1)")
            .bind(order_id)
            .fetch_one(&pool)
            .await?;
    assert!(order_exists, "Order should exist after commit");

    let event_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM durable.e_event_tx_commit WHERE event_name = $1)",
    )
    .bind(&event_name)
    .fetch_one(&pool)
    .await?;
    assert!(event_exists, "Event should exist after commit");

    Ok(())
}

/// Test that emit_event_with in a rolled back transaction does NOT persist the event.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_with_transaction_rollback(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_tx_rollback").await;
    client.create_queue(None).await.unwrap();

    // Create a test table
    sqlx::query("CREATE TABLE test_event_orders_rb (id UUID PRIMARY KEY, status TEXT)")
        .execute(&pool)
        .await?;

    let order_id = uuid::Uuid::now_v7();
    let event_name = format!("order_created_{}", order_id);

    // Start a transaction and do both operations, then rollback
    let mut tx = pool.begin().await?;

    sqlx::query("INSERT INTO test_event_orders_rb (id, status) VALUES ($1, $2)")
        .bind(order_id)
        .bind("pending")
        .execute(&mut *tx)
        .await?;

    client
        .emit_event_with(&mut *tx, &event_name, &json!({"order_id": order_id}), None)
        .await
        .expect("Failed to emit event in transaction");

    // Rollback instead of commit
    tx.rollback().await?;

    // Verify neither the order nor event exist
    let order_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM test_event_orders_rb WHERE id = $1)")
            .bind(order_id)
            .fetch_one(&pool)
            .await?;
    assert!(!order_exists, "Order should NOT exist after rollback");

    let event_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM durable.e_event_tx_rollback WHERE event_name = $1)",
    )
    .bind(&event_name)
    .fetch_one(&pool)
    .await?;
    assert!(!event_exists, "Event should NOT exist after rollback");

    Ok(())
}

// ============================================================================
// Error Type Verification Tests
// ============================================================================

/// Test that event timeout produces correct error payload structure.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_timeout_error_payload(pool: PgPool) -> sqlx::Result<()> {
    use common::helpers::get_failed_payload;

    let client = create_client(pool.clone(), "event_timeout_payload").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    // Spawn task with short timeout, never emit event
    let spawn_result = client
        .spawn_with_options::<EventWaitingTask>(
            EventWaitParams {
                event_name: "never_arrives".to_string(),
                timeout_seconds: Some(1),
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
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for task to fail due to timeout
    let terminal = wait_for_task_terminal(
        &pool,
        "event_timeout_payload",
        spawn_result.task_id,
        Duration::from_secs(10),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("failed".to_string()));

    // Verify the error payload structure
    let failed_payload = get_failed_payload(&pool, "event_timeout_payload", spawn_result.task_id)
        .await?
        .expect("Should have failed_payload");

    assert_eq!(
        failed_payload.get("name").and_then(|v| v.as_str()),
        Some("Timeout"),
        "Error name should be 'Timeout'"
    );
    assert_eq!(
        failed_payload.get("step_name").and_then(|v| v.as_str()),
        Some("never_arrives"),
        "step_name should match event name"
    );
    assert!(
        failed_payload.get("message").is_some(),
        "Should have error message"
    );

    Ok(())
}

/// Test that emit_event with empty name fails with InvalidEventName error.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_with_empty_name_fails(pool: PgPool) -> sqlx::Result<()> {
    use durable::DurableError;

    let client = create_client(pool.clone(), "event_empty_name").await;
    client.create_queue(None).await.unwrap();

    // Try to emit event with empty name
    let result = client.emit_event("", &json!({"data": "test"}), None).await;

    match result {
        Err(DurableError::InvalidEventName { reason }) => {
            assert!(
                reason.contains("non-empty"),
                "Error should mention non-empty requirement, got: {}",
                reason
            );
        }
        Err(e) => panic!("Expected InvalidEventName error, got: {:?}", e),
        Ok(_) => panic!("Expected error, but emit_event succeeded"),
    }

    Ok(())
}

// ============================================================================
// Lock Tests
// ============================================================================

/// Stress test to verify that locking prevent lost wakeups.
/// This test spawns many tasks waiting on distinct events and emits all events
/// with jittered timing to maximize race condition likelihood.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_race_stress(pool: PgPool) -> sqlx::Result<()> {
    // Configurable via environment variables for CI tuning
    let rounds: usize = std::env::var("DURABLE_EVENT_RACE_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let tasks_per_round: usize = std::env::var("DURABLE_EVENT_RACE_TASKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let jitter_ms: u64 = std::env::var("DURABLE_EVENT_RACE_JITTER_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let client = create_client(pool.clone(), "event_race").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(10),
            claim_timeout: Duration::from_secs(60),
            concurrency: 32,
            ..Default::default()
        })
        .await
        .unwrap();

    for round in 0..rounds {
        let mut task_ids = Vec::with_capacity(tasks_per_round);
        let mut event_names = Vec::with_capacity(tasks_per_round);

        // Spawn tasks waiting on unique events
        for i in 0..tasks_per_round {
            let event_name = format!("race_event_r{}_{}", round, i);
            event_names.push(event_name.clone());

            let spawn_result = client
                .spawn::<EventWaitingTask>(EventWaitParams {
                    event_name,
                    timeout_seconds: Some(30),
                })
                .await
                .expect("Failed to spawn task");
            task_ids.push(spawn_result.task_id);
        }

        // Brief pause to let tasks start waiting
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Emit all events with jittered timing to maximize race conditions
        let pool_for_emit = pool.clone();
        let emit_handles: Vec<_> = event_names
            .into_iter()
            .enumerate()
            .map(|(i, event_name)| {
                let pool = pool_for_emit.clone();
                tokio::spawn(async move {
                    // Jitter: vary start times
                    let jitter = Duration::from_micros((i as u64 * 17) % (jitter_ms * 1000));
                    tokio::time::sleep(jitter).await;
                    let emit_client = create_client(pool, "event_race").await;
                    emit_client
                        .emit_event::<serde_json::Value>(&event_name, &json!({"idx": i}), None)
                        .await
                        .expect("Failed to emit event");
                })
            })
            .collect();

        // Wait for all emits to complete
        for handle in emit_handles {
            handle.await.expect("Emit task panicked");
        }

        // Check for orphaned waiters (wait registrations for already-emitted events)
        // This indicates a lost wakeup
        let orphaned_count: (i64,) = sqlx::query_as(AssertSqlSafe(
            "SELECT COUNT(*) FROM durable.w_event_race w
             WHERE EXISTS (SELECT 1 FROM durable.e_event_race e WHERE e.event_name = w.event_name)"
                .to_string(),
        ))
        .fetch_one(&pool)
        .await?;

        if orphaned_count.0 > 0 {
            panic!(
                "Round {}: Found {} orphaned waiters for already-emitted events (lost wakeup detected!)",
                round, orphaned_count.0
            );
        }

        // Wait for all tasks to complete
        for task_id in task_ids {
            let terminal =
                wait_for_task_terminal(&pool, "event_race", task_id, Duration::from_secs(10))
                    .await?;
            assert_eq!(
                terminal,
                Some("completed".to_string()),
                "Round {}: Task should complete after event is emitted",
                round
            );
        }
    }

    worker.shutdown().await;
    Ok(())
}

/// Regression test for the "lost wakeup" race between await_event() and emit_event().
///
/// We make the race deterministic by:
/// - pre-creating a dummy wait row for (run_id, step_name)
/// - holding a row lock on it so await_event blocks in the UPSERT path
/// - trying to emit the event while await_event is blocked (should block too)
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_await_emit_event_race_does_not_lose_wakeup(pool: PgPool) -> sqlx::Result<()> {
    let queue = "event_race_gate";
    let event_name = "race-event";
    let payload = json!({"value": 42});

    // Setup: Create queue, spawn task, claim it
    sqlx::query("SELECT durable.create_queue($1)")
        .bind(queue)
        .execute(&pool)
        .await?;

    let (task_id, run_id): (Uuid, Uuid) =
        sqlx::query_as("SELECT task_id, run_id FROM durable.spawn_task($1, $2, $3, $4)")
            .bind(queue)
            .bind("waiter")
            .bind(json!({"step": 1}))
            .bind(json!({}))
            .fetch_one(&pool)
            .await?;

    let claim: (Uuid, Uuid) =
        sqlx::query_as("SELECT run_id, task_id FROM durable.claim_task($1, $2, $3, $4)")
            .bind(queue)
            .bind("worker")
            .bind(60)
            .bind(1)
            .fetch_one(&pool)
            .await?;
    assert_eq!(claim.0, run_id);
    assert_eq!(claim.1, task_id);

    // Create a dummy wait row so await_event hits the UPDATE path and can block.
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO durable.w_{} (task_id, run_id, step_name, event_name, timeout_at)
         VALUES ($1, $2, $3, $4, NULL)",
        queue
    )))
    .bind(task_id)
    .bind(run_id)
    .bind("wait")
    .bind("dummy")
    .execute(&pool)
    .await?;

    // Get connect options from pool for creating separate connections
    let connect_opts: PgConnectOptions = (*pool.connect_options()).clone();

    // Open lock connection and hold FOR UPDATE lock on the wait row
    let lock_opts = connect_opts.clone().application_name("durable-locker");
    let mut lock_conn = PgConnection::connect_with(&lock_opts).await?;

    sqlx::query("BEGIN").execute(&mut lock_conn).await?;
    sqlx::query(AssertSqlSafe(format!(
        "SELECT 1 FROM durable.w_{} WHERE run_id = $1 AND step_name = $2 FOR UPDATE",
        queue
    )))
    .bind(run_id)
    .bind("wait")
    .execute(&mut lock_conn)
    .await?;

    // Spawn async task to call await_event - it will block on the lock
    let await_opts = connect_opts.clone().application_name("durable-await-race");
    let queue_clone = queue.to_string();
    let event_name_clone = event_name.to_string();
    let await_handle = tokio::spawn(async move {
        let mut conn = PgConnection::connect_with(&await_opts).await?;

        let result: (bool, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT should_suspend, payload FROM durable.await_event($1, $2, $3, $4, $5, $6)",
        )
        .bind(&queue_clone)
        .bind(task_id)
        .bind(run_id)
        .bind("wait")
        .bind(&event_name_clone)
        .bind(None::<i32>)
        .fetch_one(&mut conn)
        .await?;

        Ok::<_, sqlx::Error>(result)
    });

    // Wait until await_event is blocked on a lock (the w_<queue> row lock)
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT wait_event_type FROM pg_stat_activity WHERE application_name = $1",
        )
        .bind("durable-await-race")
        .fetch_optional(&pool)
        .await?;

        if let Some((Some(ref wait_type),)) = row
            && wait_type == "Lock"
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "await_event did not block as expected"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // While await_event is blocked, emit_event should block on the event-row lock.
    // We use a short statement_timeout to verify it blocks.
    let emit_opts = connect_opts.clone().application_name("durable-emit");
    let mut emit_conn = PgConnection::connect_with(&emit_opts).await?;
    sqlx::query("SET statement_timeout = '200ms'")
        .execute(&mut emit_conn)
        .await?;

    let emit_result = sqlx::query("SELECT durable.emit_event($1, $2, $3)")
        .bind(queue)
        .bind(event_name)
        .bind(&payload)
        .execute(&mut emit_conn)
        .await;

    // Should timeout/be cancelled because it's blocked
    assert!(
        emit_result.is_err(),
        "emit_event should have blocked and timed out"
    );

    // Reset statement_timeout for later use
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut emit_conn)
        .await?;

    // Let await_event proceed; it should suspend (no event delivered yet).
    sqlx::query("ROLLBACK").execute(&mut lock_conn).await?;
    drop(lock_conn);

    let await_result = await_handle
        .await
        .expect("await task panicked")
        .expect("await_event failed");
    let (should_suspend, got_payload) = await_result;
    assert!(should_suspend, "should_suspend should be true");
    assert!(got_payload.is_none(), "payload should be null on suspend");

    // Now emit for real; it must wake the sleeping run and create the checkpoint.
    sqlx::query("SELECT durable.emit_event($1, $2, $3)")
        .bind(queue)
        .bind(event_name)
        .bind(&payload)
        .execute(&pool)
        .await?;

    // Run should now be pending
    let (state,): (String,) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state FROM durable.r_{} WHERE run_id = $1",
        queue
    )))
    .bind(run_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(state, "pending");

    // Claim the task again
    let claim2: (Uuid,) = sqlx::query_as("SELECT run_id FROM durable.claim_task($1, $2, $3, $4)")
        .bind(queue)
        .bind("worker")
        .bind(60)
        .bind(1)
        .fetch_one(&pool)
        .await?;
    assert_eq!(claim2.0, run_id);

    // await_event should now return the payload (should_suspend = false)
    let resume: (bool, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT should_suspend, payload FROM durable.await_event($1, $2, $3, $4, $5, $6)",
    )
    .bind(queue)
    .bind(task_id)
    .bind(run_id)
    .bind("wait")
    .bind(event_name)
    .bind(None::<i32>)
    .fetch_one(&pool)
    .await?;

    assert!(!resume.0, "should_suspend should be false on resume");
    assert_eq!(
        resume.1,
        Some(payload),
        "payload should match emitted value"
    );

    Ok(())
}

/// Regression test: a task that awaits an event AFTER another task already
/// registered a waiter and the event was emitted should still see the payload.
///
/// This tests the bug where await_event creates a placeholder row with NULL payload,
/// and emit_event's ON CONFLICT DO NOTHING would fail to update it, causing late
/// joiners to see NULL and sleep forever.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_await_event_late_joiner_sees_payload(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "late_join").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    let event_name = "late-joiner-event";
    let payload = json!({"late": "joiner"});

    // Spawn Task A that waits for the event
    let task_a = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: event_name.to_string(),
            timeout_seconds: Some(30),
        })
        .await
        .expect("Failed to spawn task A");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for Task A to start waiting (creates placeholder row in e_ table)
    tokio::time::sleep(Duration::from_millis(300)).await;

    let state = get_task_state(&pool, "late_join", task_a.task_id).await?;
    assert!(
        state == Some("sleeping".to_string()) || state == Some("running".to_string()),
        "Task A should be sleeping or running, got {:?}",
        state
    );

    // Emit the event - this should update the placeholder row with the real payload
    client
        .emit_event(event_name, &payload, None)
        .await
        .expect("Failed to emit event");

    // Wait for Task A to complete
    let terminal_a =
        wait_for_task_terminal(&pool, "late_join", task_a.task_id, Duration::from_secs(5)).await?;
    assert_eq!(
        terminal_a,
        Some("completed".to_string()),
        "Task A should complete"
    );

    // Now spawn Task B - it should see the event was already emitted and complete immediately
    let task_b = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: event_name.to_string(),
            timeout_seconds: Some(2), // Short timeout to fail fast if bug exists
        })
        .await
        .expect("Failed to spawn task B");

    // Task B should complete quickly since event already exists with payload
    let terminal_b =
        wait_for_task_terminal(&pool, "late_join", task_b.task_id, Duration::from_secs(3)).await?;

    worker.shutdown().await;

    assert_eq!(
        terminal_b,
        Some("completed".to_string()),
        "Task B (late joiner) should complete immediately, not sleep forever"
    );

    // Verify Task B received the correct payload
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_late_join WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(task_b.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(
        result.0, payload,
        "Task B should receive the emitted payload"
    );

    Ok(())
}
