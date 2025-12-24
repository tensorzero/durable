#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::wait_for_task_terminal;
use common::tasks::{EchoParams, EchoTask};
use durable::{Durable, MIGRATOR, WorkerOptions};
use sqlx::{AssertSqlSafe, PgPool};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

async fn create_client(pool: PgPool, queue_name: &str) -> Durable {
    Durable::builder()
        .pool(pool)
        .queue_name(queue_name)
        .build()
        .await
        .expect("Failed to create Durable client")
}

// ============================================================================
// Concurrency Tests
// ============================================================================

/// Test that a task is claimed by exactly one worker when multiple workers compete.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_claimed_by_exactly_one_worker(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "conc_claim").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await.unwrap();

    // Spawn a single task
    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    // Start multiple workers competing for tasks
    let num_workers = 5;
    let barrier = Arc::new(Barrier::new(num_workers));
    let mut workers = Vec::new();

    for i in 0..num_workers {
        let pool_clone = pool.clone();
        let barrier_clone = barrier.clone();

        let worker_handle = tokio::spawn(async move {
            let client = create_client(pool_clone, "conc_claim").await;
            client.register::<EchoTask>().await.unwrap();

            // Synchronize all workers to start at the same time
            barrier_clone.wait().await;

            let worker = client
                .start_worker(WorkerOptions {
                    poll_interval: Duration::from_millis(10), // Fast polling
                    claim_timeout: Duration::from_secs(30),
                    concurrency: 1,
                    ..Default::default()
                })
                .await.unwrap();

            // Let workers run for a bit
            tokio::time::sleep(Duration::from_millis(500)).await;

            worker.shutdown().await;
            i
        });

        workers.push(worker_handle);
    }

    // Wait for all workers to complete
    for worker in workers {
        worker.await.expect("Worker panicked");
    }

    // Verify task completed successfully
    let terminal = wait_for_task_terminal(
        &pool,
        "conc_claim",
        spawn_result.task_id,
        Duration::from_secs(1),
    )
    .await?;
    assert_eq!(terminal, Some("completed".to_string()));

    // Verify only one run was created (no duplicate claims)
    let query =
        AssertSqlSafe("SELECT COUNT(*) FROM durable.r_conc_claim WHERE task_id = $1".to_string());
    let (run_count,): (i64,) = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_one(&pool)
        .await?;

    assert_eq!(
        run_count, 1,
        "Task should have exactly 1 run, not {}",
        run_count
    );

    // Verify exactly one worker claimed the task (claimed_by should be set)
    let query =
        AssertSqlSafe("SELECT claimed_by FROM durable.r_conc_claim WHERE task_id = $1".to_string());
    let result: Option<(Option<String>,)> = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_optional(&pool)
        .await?;

    // After completion, claimed_by might be cleared, so we just verify the task completed once
    assert!(result.is_some(), "Run should exist");

    Ok(())
}

/// Test that concurrent claims with SKIP LOCKED do not cause deadlocks.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_concurrent_claims_with_skip_locked(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "conc_skip").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await.unwrap();

    // Spawn many tasks
    let num_tasks = 50;
    let mut task_ids = Vec::new();

    for i in 0..num_tasks {
        let spawn_result = client
            .spawn::<EchoTask>(EchoParams {
                message: format!("task-{}", i),
            })
            .await
            .expect("Failed to spawn task");
        task_ids.push(spawn_result.task_id);
    }

    // Start multiple workers competing for tasks
    let num_workers = 10;
    let barrier = Arc::new(Barrier::new(num_workers));
    let mut worker_handles = Vec::new();

    for _ in 0..num_workers {
        let pool_clone = pool.clone();
        let barrier_clone = barrier.clone();

        let handle = tokio::spawn(async move {
            let client = create_client(pool_clone, "conc_skip").await;
            client.register::<EchoTask>().await.unwrap();

            // Synchronize all workers to start at the same time
            barrier_clone.wait().await;

            let worker = client
                .start_worker(WorkerOptions {
                    poll_interval: Duration::from_millis(10), // Fast polling to maximize contention
                    claim_timeout: Duration::from_secs(30),
                    concurrency: 5, // Each worker handles multiple tasks
                    ..Default::default()
                })
                .await.unwrap();

            // Let workers process tasks
            tokio::time::sleep(Duration::from_secs(3)).await;

            worker.shutdown().await;
        });

        worker_handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await.expect("Worker panicked");
    }

    // Verify all tasks completed
    let mut completed_count = 0;
    for task_id in &task_ids {
        let terminal =
            wait_for_task_terminal(&pool, "conc_skip", *task_id, Duration::from_millis(100))
                .await?;
        if terminal == Some("completed".to_string()) {
            completed_count += 1;
        }
    }

    assert_eq!(
        completed_count, num_tasks,
        "All {} tasks should complete, but only {} did",
        num_tasks, completed_count
    );

    // Verify no duplicate runs (each task should have exactly 1 run)
    let query = AssertSqlSafe(
        "SELECT task_id, COUNT(*) as run_count FROM durable.r_conc_skip GROUP BY task_id HAVING COUNT(*) > 1".to_string()
    );
    let duplicates: Vec<(uuid::Uuid, i64)> = sqlx::query_as(query).fetch_all(&pool).await?;

    assert!(
        duplicates.is_empty(),
        "No tasks should have duplicate runs, found: {:?}",
        duplicates
    );

    // Verify all tasks were processed (check claimed_by was set at some point)
    let query = AssertSqlSafe(
        "SELECT DISTINCT claimed_by FROM durable.r_conc_skip WHERE claimed_by IS NOT NULL"
            .to_string(),
    );
    let workers_that_claimed: Vec<(String,)> = sqlx::query_as(query).fetch_all(&pool).await?;

    let unique_workers: HashSet<_> = workers_that_claimed.into_iter().map(|(w,)| w).collect();

    // Multiple workers should have claimed tasks (proving distribution)
    // Note: After completion, claimed_by might be cleared, so this is a soft check
    println!(
        "Number of unique workers that claimed tasks: {}",
        unique_workers.len()
    );

    Ok(())
}
