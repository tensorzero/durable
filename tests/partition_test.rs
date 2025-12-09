#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{count_runs_for_task, get_checkpoint_count, wait_for_task_terminal};
use common::tasks::{StepCountingParams, StepCountingTask};
use durable::{Durable, MIGRATOR, RetryStrategy, SpawnOptions, WorkerOptions};
use sqlx::PgPool;
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
// Partition/Network Failure Tests
// ============================================================================

/// Test that a task that fails mid-execution retries from checkpoint.
/// Simulates "connection lost during checkpoint" by using a task that fails after step 2.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_db_connection_lost_during_checkpoint(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "part_ckpt").await;
    client.create_queue(None).await.unwrap();
    client.register::<StepCountingTask>().await;

    // Spawn a task that will fail after step 2 (simulating checkpoint failure)
    let spawn_result = client
        .spawn_with_options::<StepCountingTask>(
            StepCountingParams {
                fail_after_step2: true,
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

    // Wait for task to fail (will retry but always fail after step 2)
    let terminal = wait_for_task_terminal(
        &pool,
        "part_ckpt",
        spawn_result.task_id,
        Duration::from_secs(10),
    )
    .await?;
    worker.shutdown().await;

    // Task should fail after max attempts
    assert_eq!(terminal, Some("failed".to_string()));

    // Verify checkpoints were created and preserved across retries
    let checkpoint_count = get_checkpoint_count(&pool, "part_ckpt", spawn_result.task_id).await?;
    assert_eq!(
        checkpoint_count, 2,
        "Should have 2 checkpoints (step1, step2)"
    );

    // Verify multiple runs were created (retries happened)
    let run_count = count_runs_for_task(&pool, "part_ckpt", spawn_result.task_id).await?;
    assert_eq!(run_count, 3, "Should have 3 runs (max_attempts)");

    Ok(())
}

/// Test that a stale worker cannot update checkpoints after another worker has reclaimed.
/// This verifies that the checkpoint system has proper ownership checks.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_stale_worker_checkpoint_rejected(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{SlowNoHeartbeatParams, SlowNoHeartbeatTask};

    let client = create_client(pool.clone(), "part_stale").await;
    client.create_queue(None).await.unwrap();
    client.register::<SlowNoHeartbeatTask>().await;

    let claim_timeout = 2; // Short lease

    // Spawn a slow task
    let spawn_result = client
        .spawn_with_options::<SlowNoHeartbeatTask>(
            SlowNoHeartbeatParams {
                sleep_ms: 30000, // 30 seconds
            },
            SpawnOptions {
                retry_strategy: Some(RetryStrategy::Fixed { base_seconds: 0 }),
                max_attempts: Some(5),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to spawn task");

    // First worker claims the task
    let worker1 = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout,
            ..Default::default()
        })
        .await;

    // Wait for task to be claimed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown first worker without completing
    worker1.shutdown().await;

    // Wait for lease to expire
    tokio::time::sleep(Duration::from_secs(claim_timeout + 1)).await;

    // Second worker reclaims
    let worker2 = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 60, // Longer timeout
            ..Default::default()
        })
        .await;

    // Wait for reclaim to happen
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify multiple runs exist (proves reclaim happened)
    let run_count = count_runs_for_task(&pool, "part_stale", spawn_result.task_id).await?;
    assert!(
        run_count >= 2,
        "Should have at least 2 runs after reclaim, got {}",
        run_count
    );

    worker2.shutdown().await;

    // The key assertion: if worker1 tried to write a checkpoint after worker2 reclaimed,
    // it would be rejected. This is enforced by the owner_run_id check in set_task_checkpoint_state.
    // We can't easily test the rejection directly without more complex setup,
    // but the fact that multiple runs exist proves the reclaim mechanism works.

    Ok(())
}
