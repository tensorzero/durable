#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::tasks::{EchoParams, EchoTask};
use durable::{Durable, DurableBuilder, MIGRATOR, WorkerOptions};
use sqlx::{AssertSqlSafe, PgPool};
use std::time::Duration;

/// Helper to create a DurableBuilder from the test pool.
fn create_client(pool: PgPool, queue_name: &str) -> DurableBuilder {
    Durable::builder().pool(pool).queue_name(queue_name)
}

// ============================================================================
// count_unclaimed_ready_tasks Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_count_unclaimed_empty_queue(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "count_empty")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();
    client.create_queue(None).await.unwrap();

    let count = client
        .count_unclaimed_ready_tasks(None)
        .await
        .expect("Failed to count unclaimed tasks");
    assert_eq!(count, 0, "Empty queue should have 0 unclaimed tasks");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_count_unclaimed_after_spawning(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "count_spawn")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();
    client.create_queue(None).await.unwrap();

    // Spawn 3 tasks
    for i in 0..3 {
        client
            .spawn::<EchoTask>(EchoParams {
                message: format!("task {i}"),
            })
            .await
            .expect("Failed to spawn task");
    }

    let count = client
        .count_unclaimed_ready_tasks(None)
        .await
        .expect("Failed to count unclaimed tasks");
    assert_eq!(count, 3, "Should have 3 unclaimed tasks after spawning 3");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_count_unclaimed_decreases_after_claim(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "count_claim")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();
    client.create_queue(None).await.unwrap();

    // Spawn 3 tasks
    for i in 0..3 {
        client
            .spawn::<EchoTask>(EchoParams {
                message: format!("task {i}"),
            })
            .await
            .expect("Failed to spawn task");
    }

    assert_eq!(
        client.count_unclaimed_ready_tasks(None).await.unwrap(),
        3,
        "Should start with 3 unclaimed"
    );

    // Start a worker that will claim and complete tasks
    let worker = client
        .start_worker(WorkerOptions {
            concurrency: 3,
            poll_interval: Duration::from_millis(100),
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for the worker to process all tasks
    tokio::time::sleep(Duration::from_secs(2)).await;

    let count = client
        .count_unclaimed_ready_tasks(None)
        .await
        .expect("Failed to count unclaimed tasks");
    assert_eq!(
        count, 0,
        "Should have 0 unclaimed tasks after worker claims them"
    );

    worker.shutdown().await;

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_count_unclaimed_with_explicit_queue_name(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "default")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();

    // Create two queues
    client.create_queue(Some("queue_a")).await.unwrap();
    client.create_queue(Some("queue_b")).await.unwrap();

    // Spawn tasks into queue_a
    let client_a = create_client(pool.clone(), "queue_a")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();
    for i in 0..2 {
        client_a
            .spawn::<EchoTask>(EchoParams {
                message: format!("a-{i}"),
            })
            .await
            .unwrap();
    }

    // Spawn tasks into queue_b
    let client_b = create_client(pool.clone(), "queue_b")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();
    for i in 0..5 {
        client_b
            .spawn::<EchoTask>(EchoParams {
                message: format!("b-{i}"),
            })
            .await
            .unwrap();
    }

    // Count using explicit queue names
    let count_a = client
        .count_unclaimed_ready_tasks(Some("queue_a"))
        .await
        .unwrap();
    let count_b = client
        .count_unclaimed_ready_tasks(Some("queue_b"))
        .await
        .unwrap();

    assert_eq!(count_a, 2, "queue_a should have 2 unclaimed tasks");
    assert_eq!(count_b, 5, "queue_b should have 5 unclaimed tasks");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_count_unclaimed_excludes_future_tasks(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "count_future")
        .register::<EchoTask>()
        .unwrap()
        .build()
        .await
        .unwrap();
    client.create_queue(None).await.unwrap();

    // Spawn two tasks (both ready now)
    client
        .spawn::<EchoTask>(EchoParams {
            message: "ready now".to_string(),
        })
        .await
        .unwrap();
    let delayed = client
        .spawn::<EchoTask>(EchoParams {
            message: "will be delayed".to_string(),
        })
        .await
        .unwrap();

    // Push one run's available_at into the future via direct SQL
    sqlx::query(AssertSqlSafe(
        "UPDATE durable.r_count_future SET available_at = now() + interval '1 hour' WHERE task_id = $1".to_string()
    ))
    .bind(delayed.task_id)
    .execute(&pool)
    .await?;

    let count = client
        .count_unclaimed_ready_tasks(None)
        .await
        .expect("Failed to count unclaimed tasks");
    assert_eq!(
        count, 1,
        "Should only count the immediately-ready task, not the delayed one"
    );

    Ok(())
}
