#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use durable::{Durable, MIGRATOR};
use sqlx::PgPool;

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
// Queue Creation Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_queue_successfully(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "test_queue").await;

    // Create the queue
    client
        .create_queue(None)
        .await
        .expect("Failed to create queue");

    // Verify it exists by listing queues
    let queues = client.list_queues().await.expect("Failed to list queues");
    assert!(queues.contains(&"test_queue".to_string()));

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_queue_is_idempotent(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "idempotent_queue").await;

    // Create the same queue twice - should not error
    client
        .create_queue(None)
        .await
        .expect("First create should succeed");
    client
        .create_queue(None)
        .await
        .expect("Second create should also succeed (idempotent)");

    // Verify only one queue exists
    let queues = client.list_queues().await.expect("Failed to list queues");
    let count = queues.iter().filter(|q| *q == "idempotent_queue").count();
    assert_eq!(count, 1, "Queue should appear exactly once");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_queue_with_explicit_name(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "default_queue").await;

    // Create a queue with an explicit name different from default
    client
        .create_queue(Some("explicit_queue"))
        .await
        .expect("Failed to create queue");

    let queues = client.list_queues().await.expect("Failed to list queues");
    assert!(queues.contains(&"explicit_queue".to_string()));

    Ok(())
}

// ============================================================================
// Queue Dropping Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_drop_queue_removes_it(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "drop_test_queue").await;

    // Create then drop the queue
    client
        .create_queue(None)
        .await
        .expect("Failed to create queue");

    let queues_before = client.list_queues().await.expect("Failed to list queues");
    assert!(queues_before.contains(&"drop_test_queue".to_string()));

    client.drop_queue(None).await.expect("Failed to drop queue");

    let queues_after = client.list_queues().await.expect("Failed to list queues");
    assert!(!queues_after.contains(&"drop_test_queue".to_string()));

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_drop_queue_with_explicit_name(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "default").await;

    // Create a queue with explicit name
    client
        .create_queue(Some("to_drop"))
        .await
        .expect("Failed to create queue");

    // Drop it with explicit name
    client
        .drop_queue(Some("to_drop"))
        .await
        .expect("Failed to drop queue");

    let queues = client.list_queues().await.expect("Failed to list queues");
    assert!(!queues.contains(&"to_drop".to_string()));

    Ok(())
}

// ============================================================================
// Queue Listing Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_list_queues_returns_all_created_queues(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "default").await;

    // Create multiple queues
    client
        .create_queue(Some("queue_a"))
        .await
        .expect("Failed to create queue_a");
    client
        .create_queue(Some("queue_b"))
        .await
        .expect("Failed to create queue_b");
    client
        .create_queue(Some("queue_c"))
        .await
        .expect("Failed to create queue_c");

    let queues = client.list_queues().await.expect("Failed to list queues");

    assert!(queues.contains(&"queue_a".to_string()));
    assert!(queues.contains(&"queue_b".to_string()));
    assert!(queues.contains(&"queue_c".to_string()));

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_list_queues_empty_initially(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "default").await;

    // Without creating any queues, list should be empty
    let queues = client.list_queues().await.expect("Failed to list queues");
    assert!(queues.is_empty(), "Expected no queues initially");

    Ok(())
}

// ============================================================================
// Queue Name Validation Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_queue_with_underscores(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "queue_with_underscores").await;

    client
        .create_queue(None)
        .await
        .expect("Failed to create queue");

    let queues = client.list_queues().await.expect("Failed to list queues");
    assert!(queues.contains(&"queue_with_underscores".to_string()));

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_queue_with_numbers(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "queue123").await;

    client
        .create_queue(None)
        .await
        .expect("Failed to create queue");

    let queues = client.list_queues().await.expect("Failed to list queues");
    assert!(queues.contains(&"queue123".to_string()));

    Ok(())
}
