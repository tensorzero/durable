#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::tasks::{
    EchoParams, EchoTask, FailingParams, FailingTask, UserErrorParams, UserErrorTask,
};
use durable::{Durable, MIGRATOR, RetryStrategy, SpawnOptions, TaskStatus, WorkerOptions};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_poll_completed_task(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_poll_completed")
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();
    durable.register::<EchoTask>().await.unwrap();

    let spawned = durable
        .spawn::<EchoTask>(EchoParams {
            message: "hello".to_string(),
        })
        .await
        .unwrap();

    // Initially pending
    let poll = durable
        .get_task_result(spawned.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.status, TaskStatus::Pending);
    assert!(poll.output.is_none());
    assert!(poll.error.is_none());

    // Start worker, wait for completion
    let worker = durable
        .start_worker(WorkerOptions::default())
        .await
        .unwrap();

    common::helpers::wait_for_task_state(
        &pool,
        "test_poll_completed",
        spawned.task_id,
        "completed",
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    // Should be completed with output
    let poll = durable
        .get_task_result(spawned.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.status, TaskStatus::Completed);
    assert!(poll.output.is_some());
    assert!(poll.error.is_none());

    worker.shutdown().await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_poll_nonexistent_task(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool)
        .queue_name("test_poll_nonexistent")
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let result = durable.get_task_result(Uuid::now_v7()).await.unwrap();
    assert!(result.is_none());
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_poll_failed_task(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_poll_failed")
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();
    durable.register::<FailingTask>().await.unwrap();

    // Spawn with max_attempts=1 and no retry so it fails immediately
    let spawned = durable
        .spawn_with_options::<FailingTask>(
            FailingParams {
                error_message: "test failure".to_string(),
            },
            {
                let mut opts = SpawnOptions::default();
                opts.max_attempts = Some(1);
                opts.retry_strategy = Some(RetryStrategy::None);
                opts
            },
        )
        .await
        .unwrap();

    let worker = durable
        .start_worker(WorkerOptions::default())
        .await
        .unwrap();

    common::helpers::wait_for_task_state(
        &pool,
        "test_poll_failed",
        spawned.task_id,
        "failed",
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    let poll = durable
        .get_task_result(spawned.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.status, TaskStatus::Failed);
    assert!(poll.output.is_none());

    let error = poll.error.unwrap();
    assert_eq!(error.name.as_deref(), Some("Step"));
    assert!(
        error
            .message
            .as_deref()
            .is_some_and(|m| m.contains("test failure"))
    );

    worker.shutdown().await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_poll_cancelled_task(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_poll_cancelled")
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();
    durable.register::<EchoTask>().await.unwrap();

    let spawned = durable
        .spawn::<EchoTask>(EchoParams {
            message: "will be cancelled".to_string(),
        })
        .await
        .unwrap();

    // Cancel before any worker picks it up
    durable.cancel_task(spawned.task_id, None).await.unwrap();

    let poll = durable
        .get_task_result(spawned.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.status, TaskStatus::Cancelled);
    assert!(poll.output.is_none());
    assert!(poll.error.is_none());
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_poll_failed_task_user_error(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_poll_user_error")
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();
    durable.register::<UserErrorTask>().await.unwrap();

    // UserErrorTask returns a non-retryable User error, so max_attempts doesn't matter
    // but set to 1 to be explicit
    let spawned = durable
        .spawn_with_options::<UserErrorTask>(
            UserErrorParams {
                error_message: "bad input".to_string(),
            },
            {
                let mut opts = SpawnOptions::default();
                opts.max_attempts = Some(1);
                opts.retry_strategy = Some(RetryStrategy::None);
                opts
            },
        )
        .await
        .unwrap();

    let worker = durable
        .start_worker(WorkerOptions::default())
        .await
        .unwrap();

    common::helpers::wait_for_task_state(
        &pool,
        "test_poll_user_error",
        spawned.task_id,
        "failed",
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    let poll = durable
        .get_task_result(spawned.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.status, TaskStatus::Failed);

    let error = poll.error.unwrap();
    assert_eq!(error.name.as_deref(), Some("User"));
    assert_eq!(error.message.as_deref(), Some("bad input"));

    // Verify raw field contains the structured error_data
    let raw = error.raw.unwrap();
    assert!(raw.get("error_data").is_some());

    worker.shutdown().await;
}
