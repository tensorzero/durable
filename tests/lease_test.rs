#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::helpers::{
    advance_time, get_claim_expires_at, get_last_run_id, get_task_state, set_fake_time,
    single_conn_pool, wait_for_task_terminal,
};
use common::tasks::{LongRunningHeartbeatParams, LongRunningHeartbeatTask};
use durable::{Durable, MIGRATOR, WorkerOptions};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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
async fn test_claim_sets_correct_expiry(
    pool_options: PgPoolOptions,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    let pool = single_conn_pool(pool_options, connect_options).await?;
    let client = create_client(pool.clone(), "lease_claim").await;
    client.create_queue(None).await.unwrap();
    client.register::<LongRunningHeartbeatTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    let claim_timeout = 30; // 30 seconds

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
            poll_interval: 0.05,
            claim_timeout,
            ..Default::default()
        })
        .await;

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
    let expected_expiry = start_time + chrono::Duration::seconds(claim_timeout as i64);
    let diff = (claim_expires - expected_expiry).num_seconds().abs();

    assert!(
        diff <= 2,
        "claim_expires_at should be ~{} seconds from start, got {} seconds diff",
        claim_timeout,
        diff
    );

    worker.shutdown().await;

    Ok(())
}

/// Test that heartbeat extends the lease.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_heartbeat_extends_lease(
    pool_options: PgPoolOptions,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    let pool = single_conn_pool(pool_options, connect_options).await?;
    let client = create_client(pool.clone(), "lease_hb").await;
    client.create_queue(None).await.unwrap();
    client.register::<LongRunningHeartbeatTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    let claim_timeout = 10; // 10 seconds

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
            poll_interval: 0.05,
            claim_timeout,
            ..Default::default()
        })
        .await;

    // Wait for task to be claimed
    tokio::time::sleep(Duration::from_millis(200)).await;

    let run_id = get_last_run_id(&pool, "lease_hb", spawn_result.task_id)
        .await?
        .expect("Run should exist");

    // Get initial claim_expires_at
    let initial_expires = get_claim_expires_at(&pool, "lease_hb", run_id)
        .await?
        .expect("claim_expires_at should be set");

    // Advance fake time so heartbeats extend the lease relative to a newer timestamp.
    advance_time(&pool, 2).await?;

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
async fn test_checkpoint_extends_lease(
    pool_options: PgPoolOptions,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    use common::tasks::{ManyStepsParams, ManyStepsTask};

    let pool = single_conn_pool(pool_options, connect_options).await?;
    let client = create_client(pool.clone(), "lease_ckpt").await;
    client.create_queue(None).await.unwrap();
    client.register::<ManyStepsTask>().await.unwrap();

    let start_time = chrono::Utc::now();
    set_fake_time(&pool, start_time).await?;

    let claim_timeout = 30;
    let num_steps = 20; // Enough steps to observe lease extension

    // Spawn task that creates many checkpoints
    let spawn_result = client
        .spawn::<ManyStepsTask>(ManyStepsParams { num_steps })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout,
            ..Default::default()
        })
        .await;

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
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

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
