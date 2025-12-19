#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{get_task_state, wait_for_task_terminal};
use common::tasks::{EventEmitterParams, EventEmitterTask, EventWaitParams, EventWaitingTask};
use durable::{Durable, MIGRATOR, RetryStrategy, SpawnOptions, WorkerOptions};
use futures::future::join_all;
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use std::sync::Arc;
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
// Event Tests
// ============================================================================

/// Ensure await/emit functions use advisory locks to avoid lost wakeups.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_functions_use_advisory_locks(pool: PgPool) -> sqlx::Result<()> {
    let await_def: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('durable.await_event(text, uuid, uuid, text, text, integer)'::regprocedure)",
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        await_def.contains("lock_event"),
        "await_event should call lock_event to avoid lost wakeups"
    );

    let emit_def: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('durable.emit_event(text, text, jsonb)'::regprocedure)",
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        emit_def.contains("lock_event"),
        "emit_event should call lock_event to avoid lost wakeups"
    );

    Ok(())
}

/// Stress test concurrent waits/emits to avoid lost wakeups under load.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_race_stress(pool: PgPool) -> sqlx::Result<()> {
    let client = Arc::new(create_client(pool.clone(), "event_race").await);
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    let rounds = std::env::var("DURABLE_EVENT_RACE_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let task_count = std::env::var("DURABLE_EVENT_RACE_TASKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let jitter_ms = std::env::var("DURABLE_EVENT_RACE_JITTER_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let worker = client
        .start_worker(WorkerOptions {
            concurrency: 8,
            poll_interval: 0.01,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    for round in 0..rounds {
        let mut spawned = Vec::with_capacity(task_count);
        let mut emit_tasks = Vec::with_capacity(task_count);

        for i in 0..task_count {
            let event_name = format!("race_event_{round}_{i}");
            let spawn_result = client
                .spawn::<EventWaitingTask>(EventWaitParams {
                    event_name: event_name.clone(),
                    timeout_seconds: Some(4),
                })
                .await
                .expect("Failed to spawn task");
            spawned.push(spawn_result.task_id);

            let client = Arc::clone(&client);
            let delay_ms = ((i * 7 + round * 13) % jitter_ms) as u64;
            emit_tasks.push(tokio::spawn(async move {
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                client
                    .emit_event(&event_name, &json!({"ok": true}), None)
                    .await
            }));
        }

        for result in join_all(emit_tasks).await {
            result
                .expect("emit task join failed")
                .expect("emit_event failed");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let (orphaned_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) \
             FROM durable.w_event_race w \
             JOIN durable.e_event_race e ON e.event_name = w.event_name",
        )
        .fetch_one(&pool)
        .await?;

        if orphaned_count > 0 {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT w.event_name \
                 FROM durable.w_event_race w \
                 JOIN durable.e_event_race e ON e.event_name = w.event_name \
                 ORDER BY w.created_at ASC \
                 LIMIT 10",
            )
            .fetch_all(&pool)
            .await?;

            panic!(
                "Detected {orphaned_count} waiters for already-emitted events (sample: {:?})",
                rows
            );
        }

        let wait_futures = spawned.iter().map(|task_id| {
            wait_for_task_terminal(&pool, "event_race", *task_id, Duration::from_secs(12))
        });
        for result in join_all(wait_futures).await {
            let terminal = result?;
            assert_eq!(terminal, Some("completed".to_string()));
        }
    }

    worker.shutdown().await;
    Ok(())
}

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 3,
            ..Default::default()
        })
        .await;

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

/// Test that emitting an event with the same name updates the payload (last-write-wins).
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_event_last_write_wins(pool: PgPool) -> sqlx::Result<()> {
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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    let terminal = wait_for_task_terminal(
        &pool,
        "event_dedup",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // Should receive the second payload (last-write-wins)
    let query = AssertSqlSafe(
        "SELECT completed_payload FROM durable.t_event_dedup WHERE task_id = $1".to_string(),
    );
    let result: (serde_json::Value,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(result.0, json!({"version": "second"}));

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
