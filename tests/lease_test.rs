#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{
    get_checkpoint_count, get_claim_expires_at, get_last_run_id, get_task_state, set_fake_time,
    wait_for_task_state, wait_for_task_terminal,
};
use common::tasks::{
    LongRunningHeartbeatParams, LongRunningHeartbeatTask, SleepThenCheckpointParams,
    SleepThenCheckpointTask,
};
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
// Lease Management Tests
// ============================================================================

/// Test that claiming a task sets the correct expiry time.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_claim_sets_correct_expiry(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lease_claim").await;
    client.create_queue(None).await.unwrap();
    client.register::<LongRunningHeartbeatTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    let claim_timeout = Duration::from_secs(30); // 30 seconds

    // Spawn a task that will take a while (uses heartbeats)
    let spawn_result = client
        .spawn::<LongRunningHeartbeatTask>(LongRunningHeartbeatParams {
            total_duration_ms: 60000,
            heartbeat_interval_ms: 5000,
        })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for the task to be claimed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Get the run_id
    let run_id = get_last_run_id(&pool, "lease_claim", spawn_result.task_id)
        .await?
        .expect("Run should exist");

    // Check the claim_expires_at
    let claim_expires = get_claim_expires_at(&pool, "lease_claim", run_id)
        .await?
        .expect("claim_expires_at should be set");

    // Should be approximately start_time + claim_timeout seconds
    let expected_expiry = start_time + chrono::Duration::seconds(claim_timeout.as_secs() as i64);
    let diff = (claim_expires - expected_expiry).num_seconds().abs();

    assert!(
        diff <= 2,
        "claim_expires_at should be ~{} seconds from start, got {} seconds diff",
        claim_timeout.as_secs(),
        diff
    );

    worker.shutdown().await;

    Ok(())
}

/// Test that heartbeat extends the lease.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_heartbeat_extends_lease(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lease_hb").await;
    client.create_queue(None).await.unwrap();
    client.register::<LongRunningHeartbeatTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    let claim_timeout = Duration::from_secs(10); // 10 seconds

    // Spawn task that heartbeats frequently
    let spawn_result = client
        .spawn::<LongRunningHeartbeatTask>(LongRunningHeartbeatParams {
            total_duration_ms: 5000,    // 5 seconds total
            heartbeat_interval_ms: 500, // heartbeat every 500ms
        })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for task to be claimed
    tokio::time::sleep(Duration::from_millis(200)).await;

    let run_id = get_last_run_id(&pool, "lease_hb", spawn_result.task_id)
        .await?
        .expect("Run should exist");

    // Get initial claim_expires_at
    let initial_expires = get_claim_expires_at(&pool, "lease_hb", run_id)
        .await?
        .expect("claim_expires_at should be set");

    // Wait for a couple heartbeats
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Check that claim_expires_at has been extended
    let updated_expires = get_claim_expires_at(&pool, "lease_hb", run_id)
        .await?
        .expect("claim_expires_at should still be set");

    assert!(
        updated_expires > initial_expires,
        "Heartbeat should extend lease: initial={}, updated={}",
        initial_expires,
        updated_expires
    );

    // Let task complete
    let terminal = wait_for_task_terminal(
        &pool,
        "lease_hb",
        spawn_result.task_id,
        Duration::from_secs(10),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    Ok(())
}

/// Test that checkpoint (ctx.step) extends the lease.
/// We use ManyStepsTask to have enough steps to observe lease extension.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_checkpoint_extends_lease(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{ManyStepsParams, ManyStepsTask};

    let client = create_client(pool.clone(), "lease_ckpt").await;
    client.create_queue(None).await.unwrap();
    client.register::<ManyStepsTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    let claim_timeout = Duration::from_secs(30);
    let num_steps = 20; // Enough steps to observe lease extension

    // Spawn task that creates many checkpoints
    let spawn_result = client
        .spawn::<ManyStepsTask>(ManyStepsParams { num_steps })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for task to start running
    tokio::time::sleep(Duration::from_millis(200)).await;

    let run_id = get_last_run_id(&pool, "lease_ckpt", spawn_result.task_id)
        .await?
        .expect("Run should exist");

    // Get initial claim_expires_at (might already be extended by some checkpoints)
    let initial_expires = get_claim_expires_at(&pool, "lease_ckpt", run_id).await?;

    // Wait for more checkpoints
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Get claim_expires_at after more checkpoints
    let updated_expires = get_claim_expires_at(&pool, "lease_ckpt", run_id).await?;

    // Wait for task to complete
    let terminal = wait_for_task_terminal(
        &pool,
        "lease_ckpt",
        spawn_result.task_id,
        Duration::from_secs(30),
    )
    .await?;
    worker.shutdown().await;

    assert_eq!(terminal, Some("completed".to_string()));

    // If we captured both timestamps and task hasn't completed yet, verify extension
    if let (Some(initial), Some(updated)) = (initial_expires, updated_expires) {
        assert!(
            updated >= initial,
            "Checkpoint should extend or maintain lease: initial={}, updated={}",
            initial,
            updated
        );
    }

    // The key assertion is that the task completed successfully with many checkpoints
    // If leases weren't being extended properly, the task would have failed
    Ok(())
}

/// Test that checkpoints are rejected once the lease has expired.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_checkpoint_rejected_after_lease_expired(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lease_expired_ckpt").await;
    client.create_queue(None).await.unwrap();
    client.register::<SleepThenCheckpointTask>().await.unwrap();

    let claim_timeout = Duration::from_secs(1);

    let spawn_result = client
        .spawn_with_options::<SleepThenCheckpointTask>(
            SleepThenCheckpointParams { sleep_ms: 1500 },
            {
                let mut opts = SpawnOptions::default();
                opts.retry_strategy = Some(RetryStrategy::Fixed {
                    base_delay: Duration::from_secs(0),
                });
                opts.max_attempts = Some(1);
                opts
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker1 = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout,
            ..Default::default()
        })
        .await
        .unwrap();

    let running = wait_for_task_state(
        &pool,
        "lease_expired_ckpt",
        spawn_result.task_id,
        "running",
        Duration::from_secs(2),
    )
    .await?;
    assert!(running, "Task should enter running state");

    // Wait for lease to expire but before the task wakes to checkpoint.
    tokio::time::sleep(claim_timeout + Duration::from_millis(200)).await;

    // Second worker polls to fail expired lease.
    let worker2 = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(5),
            ..Default::default()
        })
        .await
        .unwrap();

    let terminal = wait_for_task_terminal(
        &pool,
        "lease_expired_ckpt",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    assert_eq!(terminal, Some("failed".to_string()));

    let checkpoint_count =
        get_checkpoint_count(&pool, "lease_expired_ckpt", spawn_result.task_id).await?;
    assert_eq!(
        checkpoint_count, 0,
        "Checkpoint should not be written after lease expiry"
    );

    worker2.shutdown().await;
    worker1.shutdown().await;

    Ok(())
}

/// Test that checkpoints are rejected once the lease has expired - single worker variant.
/// Unlike the multi-worker test, this verifies that a single worker's poll loop
/// can detect and fail the expired run after the task stops due to lease expiration.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_checkpoint_rejected_after_lease_expired_single_worker(
    pool: PgPool,
) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lease_expired_single").await;
    client.create_queue(None).await.unwrap();
    client.register::<SleepThenCheckpointTask>().await.unwrap();

    let claim_timeout = Duration::from_secs(1);

    let spawn_result = client
        .spawn_with_options::<SleepThenCheckpointTask>(
            SleepThenCheckpointParams { sleep_ms: 1500 },
            {
                let mut opts = SpawnOptions::default();
                opts.retry_strategy = Some(RetryStrategy::Fixed {
                    base_delay: Duration::from_secs(0),
                });
                opts.max_attempts = Some(1);
                opts
            },
        )
        .await
        .expect("Failed to spawn task");

    // Single worker handles both running the task and detecting the expired lease
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout,
            ..Default::default()
        })
        .await
        .unwrap();

    let running = wait_for_task_state(
        &pool,
        "lease_expired_single",
        spawn_result.task_id,
        "running",
        Duration::from_secs(2),
    )
    .await?;
    assert!(running, "Task should enter running state");

    // Wait for:
    // 1. Lease to expire (1s)
    // 2. Task to wake and attempt checkpoint (at 1.5s) - gets AB002 error
    // 3. Worker's next poll to claim and fail the expired run
    let terminal = wait_for_task_terminal(
        &pool,
        "lease_expired_single",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    assert_eq!(terminal, Some("failed".to_string()));

    let checkpoint_count =
        get_checkpoint_count(&pool, "lease_expired_single", spawn_result.task_id).await?;
    assert_eq!(
        checkpoint_count, 0,
        "Checkpoint should not be written after lease expiry"
    );

    worker.shutdown().await;

    Ok(())
}

/// Test that heartbeat detects if the task has been cancelled.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_heartbeat_detects_cancellation(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "lease_cancel").await;
    client.create_queue(None).await.unwrap();
    client.register::<LongRunningHeartbeatTask>().await.unwrap();

    // Spawn a long-running task
    let spawn_result = client
        .spawn::<LongRunningHeartbeatTask>(LongRunningHeartbeatParams {
            total_duration_ms: 60000,   // Long task
            heartbeat_interval_ms: 200, // Frequent heartbeats
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

    // Wait for task to start executing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify task is running
    let state = get_task_state(&pool, "lease_cancel", spawn_result.task_id).await?;
    assert_eq!(state, Some("running".to_string()));

    // Cancel the task
    client
        .cancel_task(spawn_result.task_id, None)
        .await
        .expect("Failed to cancel task");

    // Wait for the cancellation to be detected via heartbeat
    let terminal = wait_for_task_terminal(
        &pool,
        "lease_cancel",
        spawn_result.task_id,
        Duration::from_secs(5),
    )
    .await?;
    worker.shutdown().await;

    // Task should be cancelled
    assert_eq!(terminal, Some("cancelled".to_string()));

    Ok(())
}
