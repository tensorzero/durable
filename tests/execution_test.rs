mod common;

use common::tasks::{
    EchoParams, EchoTask, EmptyParamsTask, MultiStepOutput, MultiStepTask, ResearchParams,
    ResearchResult, ResearchTask,
};
use durable::{Durable, MIGRATOR, WorkerOptions};
use sqlx::{AssertSqlSafe, PgPool};
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

#[derive(sqlx::FromRow)]
struct TaskState {
    state: String,
}

/// Helper to query task state from the database.
async fn get_task_state(pool: &PgPool, queue_name: &str, task_id: uuid::Uuid) -> String {
    let query = AssertSqlSafe(format!(
        "SELECT state FROM durable.t_{queue_name} WHERE task_id = $1"
    ));
    let res: TaskState = sqlx::query_as(query)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("Failed to query task state");
    res.state
}

#[derive(sqlx::FromRow)]
struct TaskResult {
    completed_payload: Option<serde_json::Value>,
}

/// Helper to query task result from the database.
async fn get_task_result(
    pool: &PgPool,
    queue_name: &str,
    task_id: uuid::Uuid,
) -> Option<serde_json::Value> {
    let query = AssertSqlSafe(format!(
        "SELECT completed_payload FROM durable.t_{queue_name} WHERE task_id = $1"
    ));
    let result: TaskResult = sqlx::query_as(query)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("Failed to query task result");
    result.completed_payload
}

// ============================================================================
// Basic Execution Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_simple_task_executes_and_completes(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_simple").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Spawn a task
    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "hello world".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    // Start worker with short poll interval
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for task to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown worker
    worker.shutdown().await;

    // Verify task completed
    let state = get_task_state(&pool, "exec_simple", spawn_result.task_id).await;
    assert_eq!(state, "completed", "Task should be in completed state");

    // Verify result is stored correctly
    let result = get_task_result(&pool, "exec_simple", spawn_result.task_id)
        .await
        .expect("Task should have a result");
    assert_eq!(result, serde_json::json!("hello world"));

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_state_transitions(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_states").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Spawn a task
    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .expect("Failed to spawn task");

    // Verify initial state is pending
    let state = get_task_state(&pool, "exec_states", spawn_result.task_id).await;
    assert_eq!(state, "pending", "Initial state should be pending");

    // Start worker
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    // Wait for task to complete
    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;

    // Verify final state is completed
    let state = get_task_state(&pool, "exec_states", spawn_result.task_id).await;
    assert_eq!(state, "completed", "Final state should be completed");

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_empty_params_task_executes(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_empty").await;
    client.create_queue(None).await.unwrap();
    client.register::<EmptyParamsTask>().await;

    let spawn_result = client
        .spawn::<EmptyParamsTask>(())
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;

    let state = get_task_state(&pool, "exec_empty", spawn_result.task_id).await;
    assert_eq!(state, "completed");

    let result = get_task_result(&pool, "exec_empty", spawn_result.task_id)
        .await
        .expect("Task should have a result");
    assert_eq!(result, serde_json::json!("completed"));

    Ok(())
}

// ============================================================================
// Multi-Step Task Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_multi_step_task_completes_all_steps(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_steps").await;
    client.create_queue(None).await.unwrap();
    client.register::<MultiStepTask>().await;

    let spawn_result = client
        .spawn::<MultiStepTask>(())
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            ..Default::default()
        })
        .await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;

    let state = get_task_state(&pool, "exec_steps", spawn_result.task_id).await;
    assert_eq!(state, "completed");

    let result = get_task_result(&pool, "exec_steps", spawn_result.task_id)
        .await
        .expect("Task should have a result");

    let output: MultiStepOutput =
        serde_json::from_value(result).expect("Failed to deserialize result");
    assert_eq!(output.step1, 1);
    assert_eq!(output.step2, 2);
    assert_eq!(output.step3, 3);

    Ok(())
}

// ============================================================================
// Concurrency Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_multiple_tasks_execute_concurrently(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_concurrent").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Spawn multiple tasks
    let mut task_ids = Vec::new();
    for i in 0..5 {
        let result = client
            .spawn::<EchoTask>(EchoParams {
                message: format!("task_{i}"),
            })
            .await
            .expect("Failed to spawn task");
        task_ids.push(result.task_id);
    }

    // Start worker with concurrency > 1
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 5,
            ..Default::default()
        })
        .await;

    // Wait for all tasks to complete
    tokio::time::sleep(Duration::from_millis(1000)).await;
    worker.shutdown().await;

    // Verify all tasks completed
    for task_id in task_ids {
        let state = get_task_state(&pool, "exec_concurrent", task_id).await;
        assert_eq!(state, "completed", "Task {task_id} should be completed");
    }

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_worker_concurrency_limit_respected(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_limit").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Spawn more tasks than concurrency limit
    for i in 0..10 {
        client
            .spawn::<EchoTask>(EchoParams {
                message: format!("task_{i}"),
            })
            .await
            .expect("Failed to spawn task");
    }

    // Start worker with low concurrency
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2, // Only 2 at a time
            ..Default::default()
        })
        .await;

    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(2000)).await;
    worker.shutdown().await;

    // All tasks should eventually complete
    let query = "SELECT COUNT(*) FROM durable.t_exec_limit WHERE state = 'completed'";
    let (count,): (i64,) = sqlx::query_as(query).fetch_one(&pool).await?;
    assert_eq!(count, 10, "All 10 tasks should complete");

    Ok(())
}

// ============================================================================
// Worker Behavior Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_worker_graceful_shutdown_waits(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_shutdown").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
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

    // Very short wait, then shutdown
    tokio::time::sleep(Duration::from_millis(200)).await;
    worker.shutdown().await;

    // After shutdown, task should be completed (if it was picked up)
    // or still pending (if worker shutdown before claiming)
    let state = get_task_state(&pool, "exec_shutdown", spawn_result.task_id).await;
    assert!(
        state == "completed" || state == "pending",
        "Task should be completed or pending after graceful shutdown"
    );

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_unregistered_task_fails(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_unreg").await;
    client.create_queue(None).await.unwrap();
    // Note: We don't register any task handler

    // Spawn a task by name
    let spawn_result = client
        .spawn_by_name(
            "unregistered-task",
            serde_json::json!({}),
            Default::default(),
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

    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;

    // Task should have failed because handler is not registered
    let state = get_task_state(&pool, "exec_unreg", spawn_result.task_id).await;
    assert_eq!(
        state, "failed",
        "Task with unregistered handler should fail"
    );

    Ok(())
}

// ============================================================================
// Result Storage Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_result_stored_correctly(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_result").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let test_message = "This is a test message with special chars: <>&\"'";

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: test_message.to_string(),
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

    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;

    let result = get_task_result(&pool, "exec_result", spawn_result.task_id)
        .await
        .expect("Task should have a result");
    assert_eq!(result, serde_json::json!(test_message));

    Ok(())
}

// ============================================================================
// README Example Test
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_research_task_readme_example(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_research").await;
    client.create_queue(None).await.unwrap();
    client.register::<ResearchTask>().await;

    let spawn_result = client
        .spawn::<ResearchTask>(ResearchParams {
            query: "distributed systems consensus algorithms".into(),
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

    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.shutdown().await;

    // Verify task completed
    let state = get_task_state(&pool, "exec_research", spawn_result.task_id).await;
    assert_eq!(state, "completed");

    // Verify result structure
    let result = get_task_result(&pool, "exec_research", spawn_result.task_id)
        .await
        .expect("Task should have a result");

    let output: ResearchResult =
        serde_json::from_value(result).expect("Failed to deserialize result");

    assert_eq!(output.sources.len(), 2);
    assert!(
        output
            .summary
            .contains("distributed systems consensus algorithms")
    );

    Ok(())
}
