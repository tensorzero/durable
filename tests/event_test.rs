#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{get_task_state, wait_for_task_terminal};
use common::tasks::{EventEmitterParams, EventEmitterTask, EventWaitParams, EventWaitingTask};
use durable::{Durable, MIGRATOR, RetryStrategy, SpawnOptions, WorkerOptions};
use serde_json::json;
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
// Event Tests
// ============================================================================

/// Test that emit_event wakes a task blocked on await_event.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_wakes_waiter(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_wake").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await;

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
    client.register::<EventWaitingTask>().await;

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
    client.register::<EventWaitingTask>().await;

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
    client.register::<EventWaitingTask>().await;

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
    client.register::<EventThenFailTask>().await;

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
    client.register::<EventWaitingTask>().await;

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
    client.register::<MultiEventTask>().await;

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

/// Test that one task can emit an event that another task awaits.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_from_different_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "event_cross").await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await;
    client.register::<EventEmitterTask>().await;

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
