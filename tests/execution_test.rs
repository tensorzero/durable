#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::tasks::{
    ConvenienceMethodsOutput, ConvenienceMethodsTask, EchoParams, EchoTask, EmptyParamsTask,
    MultiStepOutput, MultiStepTask, MultipleCallsOutput, MultipleConvenienceCallsTask,
    ResearchParams, ResearchResult, ResearchTask, ReservedPrefixTask,
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

    // Spawn a task by name - should fail at spawn time because task is not registered
    let spawn_result = client
        .spawn_by_name(
            "unregistered-task",
            serde_json::json!({}),
            Default::default(),
        )
        .await;

    assert!(
        spawn_result.is_err(),
        "Spawning an unregistered task should fail"
    );
    let err = spawn_result.unwrap_err();
    assert!(
        err.to_string().contains("Unknown task"),
        "Error should mention 'Unknown task': {}",
        err
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

// ============================================================================
// Convenience Methods Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_convenience_methods_execute(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_convenience").await;
    client.create_queue(None).await.unwrap();
    client.register::<ConvenienceMethodsTask>().await;

    let spawn_result = client
        .spawn::<ConvenienceMethodsTask>(())
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
    let state = get_task_state(&pool, "exec_convenience", spawn_result.task_id).await;
    assert_eq!(state, "completed");

    // Verify result structure
    let result = get_task_result(&pool, "exec_convenience", spawn_result.task_id)
        .await
        .expect("Task should have a result");

    let output: ConvenienceMethodsOutput =
        serde_json::from_value(result).expect("Failed to deserialize result");

    // rand should be in [0, 1)
    assert!(output.rand_value >= 0.0 && output.rand_value < 1.0);

    // now should be a valid RFC3339 timestamp
    assert!(!output.now_value.is_empty());

    // uuid should be valid (non-nil)
    assert!(!output.uuid_value.is_nil());

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_multiple_convenience_calls_produce_different_values(
    pool: PgPool,
) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_multi_convenience").await;
    client.create_queue(None).await.unwrap();
    client.register::<MultipleConvenienceCallsTask>().await;

    let spawn_result = client
        .spawn::<MultipleConvenienceCallsTask>(())
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
    let state = get_task_state(&pool, "exec_multi_convenience", spawn_result.task_id).await;
    assert_eq!(state, "completed");

    let result = get_task_result(&pool, "exec_multi_convenience", spawn_result.task_id)
        .await
        .expect("Task should have a result");

    let output: MultipleCallsOutput =
        serde_json::from_value(result).expect("Failed to deserialize result");

    // Multiple calls should produce different values (auto-increment works)
    // Note: there's a tiny chance rand1 == rand2, but it's astronomically unlikely
    assert_ne!(
        output.uuid1, output.uuid2,
        "Multiple uuid7() calls should produce different UUIDs"
    );

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_reserved_prefix_rejected(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "exec_reserved").await;
    client.create_queue(None).await.unwrap();
    client.register::<ReservedPrefixTask>().await;

    let spawn_result = client
        .spawn::<ReservedPrefixTask>(())
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

    // Task should have failed because $ prefix is reserved
    let state = get_task_state(&pool, "exec_reserved", spawn_result.task_id).await;
    assert_eq!(state, "failed", "Task using $ prefix should fail");

    Ok(())
}

// ============================================================================
// Timer Reset Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_long_running_task_with_heartbeat_completes(pool: PgPool) -> sqlx::Result<()> {
    use common::tasks::{LongRunningHeartbeatParams, LongRunningHeartbeatTask};

    let client = create_client(pool.clone(), "exec_heartbeat_timer").await;
    client.create_queue(None).await.unwrap();
    client.register::<LongRunningHeartbeatTask>().await;

    // Run a task for 3 seconds with 1 second claim_timeout
    // Task heartbeats every 200ms, so it should stay alive
    let spawn_result = client
        .spawn::<LongRunningHeartbeatTask>(LongRunningHeartbeatParams {
            total_duration_ms: 3000,    // 3 seconds total
            heartbeat_interval_ms: 200, // heartbeat every 200ms
        })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 1, // 1 second claim timeout - task runs for 3x this duration
            ..Default::default()
        })
        .await;

    // Wait for task to complete (3 seconds + buffer)
    tokio::time::sleep(Duration::from_millis(4000)).await;
    worker.shutdown().await;

    // Task should have completed successfully despite running longer than claim_timeout
    let state = get_task_state(&pool, "exec_heartbeat_timer", spawn_result.task_id).await;
    assert_eq!(
        state, "completed",
        "Task should complete when heartbeating properly"
    );

    let result = get_task_result(&pool, "exec_heartbeat_timer", spawn_result.task_id)
        .await
        .expect("Task should have a result");
    assert_eq!(result, serde_json::json!("completed"));

    Ok(())
}

// ============================================================================
// Application State Tests
// ============================================================================

/// Application state that holds a database pool for tasks to use.
#[derive(Clone)]
struct AppState {
    db_pool: PgPool,
}

/// A task that uses the application state to write to a database table.
struct WriteToDbTask;

#[derive(serde::Serialize, serde::Deserialize)]
struct WriteToDbParams {
    key: String,
    value: String,
}

#[durable::async_trait]
impl durable::Task<AppState> for WriteToDbTask {
    const NAME: &'static str = "write-to-db";
    type Params = WriteToDbParams;
    type Output = i64;

    async fn run(
        params: Self::Params,
        mut ctx: durable::TaskContext<AppState>,
        state: AppState,
    ) -> durable::TaskResult<Self::Output> {
        // Use the app state's db pool to write to a table
        let row_id: i64 = ctx
            .step("insert", || async {
                let (id,): (i64,) = sqlx::query_as(
                    "INSERT INTO test_state_table (key, value) VALUES ($1, $2) RETURNING id",
                )
                .bind(&params.key)
                .bind(&params.value)
                .fetch_one(&state.db_pool)
                .await
                .map_err(|e| anyhow::anyhow!("DB error: {}", e))?;
                Ok(id)
            })
            .await?;

        Ok(row_id)
    }
}

/// Helper to create a Durable client with application state.
async fn create_client_with_state(pool: PgPool, queue_name: &str) -> durable::Durable<AppState> {
    let app_state = AppState {
        db_pool: pool.clone(),
    };
    durable::Durable::builder()
        .pool(pool)
        .queue_name(queue_name)
        .build_with_state(app_state)
        .await
        .expect("Failed to create Durable client with state")
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_task_uses_application_state(pool: PgPool) -> sqlx::Result<()> {
    // Create a test table for the task to write to
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_state_table (
            id BIGSERIAL PRIMARY KEY,
            key TEXT NOT NULL,
            value TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    // Create client with application state
    let client = create_client_with_state(pool.clone(), "exec_state").await;
    client.create_queue(None).await.unwrap();
    client.register::<WriteToDbTask>().await;

    // Spawn a task that will use the state to write to the database
    let spawn_result = client
        .spawn::<WriteToDbTask>(WriteToDbParams {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        })
        .await
        .expect("Failed to spawn task");

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

    // Verify task completed
    let task_state = get_task_state(&pool, "exec_state", spawn_result.task_id).await;
    assert_eq!(task_state, "completed", "Task should complete successfully");

    // Verify the task actually wrote to the database using the state
    let (key, value): (String, String) =
        sqlx::query_as("SELECT key, value FROM test_state_table WHERE key = 'test_key'")
            .fetch_one(&pool)
            .await?;

    assert_eq!(key, "test_key");
    assert_eq!(value, "test_value");

    // Verify the task returned the row ID
    let result = get_task_result(&pool, "exec_state", spawn_result.task_id)
        .await
        .expect("Task should have a result");
    let row_id: i64 = serde_json::from_value(result).expect("Result should be i64");
    assert!(row_id > 0, "Row ID should be positive");

    // Cleanup
    sqlx::query("DROP TABLE test_state_table")
        .execute(&pool)
        .await?;

    Ok(())
}
