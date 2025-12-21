//! Integration tests for telemetry (spans and metrics).
//!
//! These tests verify that spans are created and metrics are recorded during
//! actual task execution.
//!
//! Run with: `cargo test --features telemetry telemetry_test`

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "telemetry")]

mod common;

use common::helpers::wait_for_task_state;
use common::tasks::{EchoParams, EchoTask, FailingParams, FailingTask, MultiStepTask};
use durable::{Durable, MIGRATOR, WorkerOptions};
use metrics_util::CompositeKey;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
use ordered_float::OrderedFloat;
use sqlx::PgPool;
use std::sync::OnceLock;
use std::time::Duration;

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
// Metrics Helper Functions
// ============================================================================

fn find_counter_with_label(
    snapshot: Snapshot,
    name: &str,
    label_key: &str,
    label_value: &str,
) -> Option<(CompositeKey, u64)> {
    snapshot
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == label_key && l.value() == label_value)
        })
        .map(|(key, _, _, value)| {
            let count = match value {
                DebugValue::Counter(c) => c,
                _ => panic!("Expected counter"),
            };
            (key, count)
        })
}

fn find_gauge_with_label(
    snapshot: Snapshot,
    name: &str,
    label_key: &str,
    label_value: &str,
) -> Option<(CompositeKey, f64)> {
    snapshot
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == label_key && l.value() == label_value)
        })
        .map(|(key, _, _, value)| {
            let gauge_value = match value {
                DebugValue::Gauge(g) => g.0,
                _ => panic!("Expected gauge"),
            };
            (key, gauge_value)
        })
}

#[allow(dead_code)]
fn find_histogram(
    snapshot: Snapshot,
    name: &str,
) -> Option<(CompositeKey, Vec<OrderedFloat<f64>>)> {
    snapshot
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| key.key().name() == name)
        .map(|(key, _, _, value)| {
            let values = match value {
                DebugValue::Histogram(h) => h,
                _ => panic!("Expected histogram"),
            };
            (key, values)
        })
}

fn get_label<'a>(key: &'a CompositeKey, label_name: &str) -> Option<&'a str> {
    key.key()
        .labels()
        .find(|l| l.key() == label_name)
        .map(|l| l.value())
}

fn count_metrics_by_name(snapshot: Snapshot, name: &str) -> usize {
    snapshot
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .count()
}

// Global snapshotter for tests - recorder installed once
static GLOBAL_SNAPSHOTTER: OnceLock<metrics_util::debugging::Snapshotter> = OnceLock::new();

fn get_snapshotter() -> metrics_util::debugging::Snapshotter {
    GLOBAL_SNAPSHOTTER
        .get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            recorder.install().expect("Failed to install recorder");
            snapshotter
        })
        .clone()
}

// ============================================================================
// Metrics Integration Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_lifecycle_metrics(pool: PgPool) -> sqlx::Result<()> {
    let snapshotter = get_snapshotter();
    let queue_name = "metrics_lifecycle";

    let client = create_client(pool.clone(), queue_name).await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await.unwrap();

    // Spawn a task
    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    // Start worker
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await;

    // Wait for task to complete
    wait_for_task_state(
        &pool,
        queue_name,
        spawn_result.task_id,
        "completed",
        Duration::from_secs(10),
    )
    .await
    .expect("Task should complete");

    worker.shutdown().await;

    // Give a moment for metrics to flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify task spawned metric for this specific queue
    let snapshot = snapshotter.snapshot();
    let spawn_result =
        find_counter_with_label(snapshot, "durable_tasks_spawned_total", "queue", queue_name);
    assert!(
        spawn_result.is_some(),
        "Task spawned metric should exist for queue {}",
        queue_name
    );
    if let Some((key, count)) = spawn_result {
        assert!(count >= 1, "Task spawn count should be at least 1");
        assert_eq!(get_label(&key, "task_name"), Some("echo"));
    }

    // Verify task claimed metric exists for this queue
    let snapshot = snapshotter.snapshot();
    let claimed =
        find_counter_with_label(snapshot, "durable_tasks_claimed_total", "queue", queue_name);
    assert!(
        claimed.is_some(),
        "Task claimed metric should exist for queue {}",
        queue_name
    );

    // Verify task completed metric exists for this queue
    let snapshot = snapshotter.snapshot();
    let completed = find_counter_with_label(
        snapshot,
        "durable_tasks_completed_total",
        "queue",
        queue_name,
    );
    assert!(
        completed.is_some(),
        "Task completed metric should exist for queue {}",
        queue_name
    );

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_failure_metrics(pool: PgPool) -> sqlx::Result<()> {
    let snapshotter = get_snapshotter();

    // Get baseline
    let baseline = snapshotter.snapshot();
    let baseline_failed_count = count_metrics_by_name(baseline, "durable_tasks_failed_total");

    let client = create_client(pool.clone(), "metrics_failure").await;
    client.create_queue(None).await.unwrap();
    client.register::<FailingTask>().await.unwrap();

    // Spawn a task that will fail
    let spawn_result = client
        .spawn::<FailingTask>(FailingParams {
            error_message: "intentional failure".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await;

    // Wait for task to fail
    wait_for_task_state(
        &pool,
        "metrics_failure",
        spawn_result.task_id,
        "failed",
        Duration::from_secs(10),
    )
    .await
    .expect("Task should fail");

    worker.shutdown().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snapshot = snapshotter.snapshot();

    // Verify task failed metric increased
    let failed_count = count_metrics_by_name(snapshot, "durable_tasks_failed_total");
    assert!(
        failed_count > baseline_failed_count,
        "Task failed count should have increased"
    );

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_worker_gauge_metrics(pool: PgPool) -> sqlx::Result<()> {
    let snapshotter = get_snapshotter();
    let queue_name = "metrics_worker";

    let client = create_client(pool.clone(), queue_name).await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await.unwrap();

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await;

    // Give the worker time to set its active gauge
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check worker active gauge while running for this specific queue
    let snapshot = snapshotter.snapshot();
    let worker_active =
        find_gauge_with_label(snapshot, "durable_worker_active", "queue", queue_name);
    assert!(
        worker_active.is_some(),
        "Worker active gauge should be recorded for queue {}",
        queue_name
    );

    if let Some((_, value)) = worker_active {
        assert!(
            (value - 1.0).abs() < f64::EPSILON,
            "Worker should be active (value={})",
            value
        );
    }

    worker.shutdown().await;

    // After shutdown, worker should set gauge to 0
    tokio::time::sleep(Duration::from_millis(100)).await;

    let snapshot = snapshotter.snapshot();
    if let Some((_, value)) =
        find_gauge_with_label(snapshot, "durable_worker_active", "queue", queue_name)
    {
        // The gauge should be 0 after shutdown
        assert!(
            value.abs() < f64::EPSILON,
            "Worker gauge should be 0 after shutdown (value={})",
            value
        );
    }

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_checkpoint_metrics(pool: PgPool) -> sqlx::Result<()> {
    let snapshotter = get_snapshotter();

    // Get baseline
    let baseline = snapshotter.snapshot();
    let baseline_ckpt_count =
        count_metrics_by_name(baseline, "durable_checkpoint_duration_seconds");

    let client = create_client(pool.clone(), "metrics_checkpoint").await;
    client.create_queue(None).await.unwrap();
    client.register::<MultiStepTask>().await.unwrap();

    // MultiStepTask has steps which record checkpoint metrics
    let spawn_result = client
        .spawn::<MultiStepTask>(())
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await;

    wait_for_task_state(
        &pool,
        "metrics_checkpoint",
        spawn_result.task_id,
        "completed",
        Duration::from_secs(10),
    )
    .await
    .expect("Task should complete");

    worker.shutdown().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snapshot = snapshotter.snapshot();

    // Verify checkpoint duration histogram was recorded
    let checkpoint_count = count_metrics_by_name(snapshot, "durable_checkpoint_duration_seconds");
    assert!(
        checkpoint_count > baseline_ckpt_count,
        "Expected checkpoint duration metrics to increase (baseline: {}, current: {})",
        baseline_ckpt_count,
        checkpoint_count
    );

    Ok(())
}

// ============================================================================
// Execution Duration Histogram Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_execution_duration_metrics(pool: PgPool) -> sqlx::Result<()> {
    let snapshotter = get_snapshotter();

    // Get baseline
    let baseline = snapshotter.snapshot();
    let baseline_duration_count =
        count_metrics_by_name(baseline, "durable_task_execution_duration_seconds");

    let client = create_client(pool.clone(), "metrics_exec_dur").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await.unwrap();

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: Duration::from_millis(50),
            claim_timeout: Duration::from_secs(30),
            ..Default::default()
        })
        .await;

    wait_for_task_state(
        &pool,
        "metrics_exec_dur",
        spawn_result.task_id,
        "completed",
        Duration::from_secs(10),
    )
    .await
    .expect("Task should complete");

    worker.shutdown().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snapshot = snapshotter.snapshot();

    // Verify execution duration histogram was recorded
    let duration_count = count_metrics_by_name(snapshot, "durable_task_execution_duration_seconds");
    assert!(
        duration_count > baseline_duration_count,
        "Expected execution duration metrics to increase"
    );

    Ok(())
}
