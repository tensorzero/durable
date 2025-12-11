#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::tasks::{EchoParams, EchoTask, FailingParams, FailingTask};
use durable::{CancellationPolicy, Durable, MIGRATOR, RetryStrategy, SpawnOptions};
use sqlx::PgPool;
use std::collections::HashMap;

/// Helper to create a Durable client from the test pool.
async fn create_client(pool: PgPool, queue_name: &str) -> Durable {
    Durable::builder()
        .pool(pool)
        .queue_name(queue_name)
        .build()
        .await
        .expect("Failed to create Durable client")
}

// ============================================================================
// Basic Spawning Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_returns_valid_ids(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_test").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let result = client
        .spawn::<EchoTask>(EchoParams {
            message: "hello".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    // Verify the result has valid UUIDs
    assert!(!result.task_id.is_nil(), "task_id should not be nil");
    assert!(!result.run_id.is_nil(), "run_id should not be nil");
    assert_eq!(result.attempt, 1, "First spawn should have attempt=1");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_multiple_tasks_get_unique_ids(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_multi").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let result1 = client
        .spawn::<EchoTask>(EchoParams {
            message: "first".to_string(),
        })
        .await
        .expect("Failed to spawn first task");

    let result2 = client
        .spawn::<EchoTask>(EchoParams {
            message: "second".to_string(),
        })
        .await
        .expect("Failed to spawn second task");

    assert_ne!(
        result1.task_id, result2.task_id,
        "Task IDs should be unique"
    );
    assert_ne!(result1.run_id, result2.run_id, "Run IDs should be unique");

    Ok(())
}

// ============================================================================
// Spawn with Options Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_custom_max_attempts(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_attempts").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let options = SpawnOptions {
        max_attempts: Some(10),
        ..Default::default()
    };

    let result = client
        .spawn_with_options::<EchoTask>(
            EchoParams {
                message: "test".to_string(),
            },
            options,
        )
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);
    // Note: We can't easily verify max_attempts was stored without querying the task table directly

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_retry_strategy_none(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_retry_none").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await;

    let options = SpawnOptions {
        retry_strategy: Some(RetryStrategy::None),
        ..Default::default()
    };

    let result = client
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "test error".to_string(),
            },
            options,
        )
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_retry_strategy_fixed(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_retry_fixed").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let options = SpawnOptions {
        retry_strategy: Some(RetryStrategy::Fixed { base_seconds: 10 }),
        ..Default::default()
    };

    let result = client
        .spawn_with_options::<EchoTask>(
            EchoParams {
                message: "test".to_string(),
            },
            options,
        )
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_retry_strategy_exponential(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_retry_exp").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let options = SpawnOptions {
        retry_strategy: Some(RetryStrategy::Exponential {
            base_seconds: 5,
            factor: 2.0,
            max_seconds: 300,
        }),
        ..Default::default()
    };

    let result = client
        .spawn_with_options::<EchoTask>(
            EchoParams {
                message: "test".to_string(),
            },
            options,
        )
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_headers(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_headers").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let mut headers = HashMap::new();
    headers.insert("correlation_id".to_string(), serde_json::json!("abc-123"));
    headers.insert("priority".to_string(), serde_json::json!(5));

    let options = SpawnOptions {
        headers: Some(headers),
        ..Default::default()
    };

    let result = client
        .spawn_with_options::<EchoTask>(
            EchoParams {
                message: "test".to_string(),
            },
            options,
        )
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_cancellation_policy(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_cancel").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let options = SpawnOptions {
        cancellation: Some(CancellationPolicy {
            max_delay: Some(60),
            max_duration: Some(300),
        }),
        ..Default::default()
    };

    let result = client
        .spawn_with_options::<EchoTask>(
            EchoParams {
                message: "test".to_string(),
            },
            options,
        )
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);

    Ok(())
}

// ============================================================================
// Spawn by Name Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_by_name(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_by_name").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let params = serde_json::json!({
        "message": "dynamic spawn"
    });

    let result = client
        .spawn_by_name("echo", params, SpawnOptions::default())
        .await
        .expect("Failed to spawn task by name");

    assert_eq!(result.attempt, 1);

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_by_name_with_options(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_by_name_opts").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let params = serde_json::json!({
        "message": "value"
    });

    let options = SpawnOptions {
        max_attempts: Some(3),
        retry_strategy: Some(RetryStrategy::Fixed { base_seconds: 5 }),
        ..Default::default()
    };

    let result = client
        .spawn_by_name("echo", params, options)
        .await
        .expect("Failed to spawn task by name with options");

    assert_eq!(result.attempt, 1);

    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_empty_params(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_empty").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Empty object is valid JSON params for EchoTask (message will be missing but that's ok for this test)
    let result = client
        .spawn_by_name("echo", serde_json::json!({}), SpawnOptions::default())
        .await
        .expect("Failed to spawn task with empty params");

    assert_eq!(result.attempt, 1);

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_complex_params(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_complex").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Complex nested JSON structure - the params don't need to match the task's Params type
    // because spawn_by_name accepts arbitrary JSON
    let params = serde_json::json!({
        "nested": {
            "array": [1, 2, 3],
            "object": {
                "key": "value"
            }
        },
        "string": "hello",
        "number": 42,
        "boolean": true,
        "null_value": null
    });

    let result = client
        .spawn_by_name("echo", params, SpawnOptions::default())
        .await
        .expect("Failed to spawn task with complex params");

    assert_eq!(result.attempt, 1);

    Ok(())
}

// ============================================================================
// Default Max Attempts Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_client_default_max_attempts(pool: PgPool) -> sqlx::Result<()> {
    let client = Durable::builder()
        .pool(pool)
        .queue_name("default_attempts")
        .default_max_attempts(3)
        .build()
        .await
        .expect("Failed to create client");

    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Spawn without specifying max_attempts - should use default of 3
    let result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    assert_eq!(result.attempt, 1);

    Ok(())
}

// ============================================================================
// Transactional Spawn Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_transaction_commit(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_tx_commit").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Create a test table
    sqlx::query("CREATE TABLE test_orders (id UUID PRIMARY KEY, status TEXT)")
        .execute(&pool)
        .await?;

    let order_id = uuid::Uuid::now_v7();

    // Start a transaction and do both operations
    let mut tx = pool.begin().await?;

    sqlx::query("INSERT INTO test_orders (id, status) VALUES ($1, $2)")
        .bind(order_id)
        .bind("pending")
        .execute(&mut *tx)
        .await?;

    let result = client
        .spawn_with::<EchoTask, _>(
            &mut *tx,
            EchoParams {
                message: format!("process order {}", order_id),
            },
        )
        .await
        .expect("Failed to spawn task in transaction");

    tx.commit().await?;

    // Verify both the order and task exist
    let order_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM test_orders WHERE id = $1)")
            .bind(order_id)
            .fetch_one(&pool)
            .await?;
    assert!(order_exists, "Order should exist after commit");

    let task_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM durable.t_spawn_tx_commit WHERE task_id = $1)",
    )
    .bind(result.task_id)
    .fetch_one(&pool)
    .await?;
    assert!(task_exists, "Task should exist after commit");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_with_transaction_rollback(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "spawn_tx_rollback").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Create a test table
    sqlx::query("CREATE TABLE test_orders_rb (id UUID PRIMARY KEY, status TEXT)")
        .execute(&pool)
        .await?;

    let order_id = uuid::Uuid::now_v7();

    // Start a transaction and do both operations, then rollback
    let mut tx = pool.begin().await?;

    sqlx::query("INSERT INTO test_orders_rb (id, status) VALUES ($1, $2)")
        .bind(order_id)
        .bind("pending")
        .execute(&mut *tx)
        .await?;

    let result = client
        .spawn_with::<EchoTask, _>(
            &mut *tx,
            EchoParams {
                message: format!("process order {}", order_id),
            },
        )
        .await
        .expect("Failed to spawn task in transaction");

    let task_id = result.task_id;

    // Rollback instead of commit
    tx.rollback().await?;

    // Verify neither the order nor task exist
    let order_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM test_orders_rb WHERE id = $1)")
            .bind(order_id)
            .fetch_one(&pool)
            .await?;
    assert!(!order_exists, "Order should NOT exist after rollback");

    let task_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM durable.t_spawn_tx_rollback WHERE task_id = $1)",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await?;
    assert!(!task_exists, "Task should NOT exist after rollback");

    Ok(())
}
