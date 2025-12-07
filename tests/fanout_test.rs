mod common;

use common::tasks::{
    DoubleTask, FailingChildTask, MultiSpawnOutput, MultiSpawnParams, MultiSpawnTask,
    SingleSpawnOutput, SingleSpawnParams, SingleSpawnTask, SlowChildTask, SpawnFailingChildTask,
    SpawnSlowChildParams, SpawnSlowChildTask,
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

#[derive(sqlx::FromRow)]
struct ParentTaskId {
    parent_task_id: Option<uuid::Uuid>,
}

/// Helper to query parent_task_id from the database.
async fn get_parent_task_id(
    pool: &PgPool,
    queue_name: &str,
    task_id: uuid::Uuid,
) -> Option<uuid::Uuid> {
    let query = AssertSqlSafe(format!(
        "SELECT parent_task_id FROM durable.t_{queue_name} WHERE task_id = $1"
    ));
    let result: ParentTaskId = sqlx::query_as(query)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("Failed to query parent_task_id");
    result.parent_task_id
}

// ============================================================================
// Basic Spawn/Join Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_single_child_and_join(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "fanout_single").await;
    client.create_queue(None).await.unwrap();
    client.register::<SingleSpawnTask>().await;
    client.register::<DoubleTask>().await;

    // Spawn parent task
    let spawn_result = client
        .spawn::<SingleSpawnTask>(SingleSpawnParams { child_value: 21 })
        .await
        .expect("Failed to spawn task");

    // Start worker with concurrency to handle both parent and child
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2,
            ..Default::default()
        })
        .await;

    // Wait for tasks to complete
    tokio::time::sleep(Duration::from_millis(2000)).await;
    worker.shutdown().await;

    // Verify parent task completed
    let state = get_task_state(&pool, "fanout_single", spawn_result.task_id).await;
    assert_eq!(state, "completed", "Parent task should be completed");

    // Verify result
    let result = get_task_result(&pool, "fanout_single", spawn_result.task_id)
        .await
        .expect("Task should have a result");

    let output: SingleSpawnOutput =
        serde_json::from_value(result).expect("Failed to deserialize result");
    assert_eq!(
        output.child_result, 42,
        "Child should have doubled 21 to 42"
    );

    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_spawn_multiple_children_and_join(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "fanout_multi").await;
    client.create_queue(None).await.unwrap();
    client.register::<MultiSpawnTask>().await;
    client.register::<DoubleTask>().await;

    // Spawn parent task with multiple values
    let spawn_result = client
        .spawn::<MultiSpawnTask>(MultiSpawnParams {
            values: vec![1, 2, 3, 4, 5],
        })
        .await
        .expect("Failed to spawn task");

    // Start worker with high concurrency
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 10,
            ..Default::default()
        })
        .await;

    // Wait for tasks to complete
    tokio::time::sleep(Duration::from_millis(3000)).await;
    worker.shutdown().await;

    // Verify parent task completed
    let state = get_task_state(&pool, "fanout_multi", spawn_result.task_id).await;
    assert_eq!(state, "completed", "Parent task should be completed");

    // Verify result
    let result = get_task_result(&pool, "fanout_multi", spawn_result.task_id)
        .await
        .expect("Task should have a result");

    let output: MultiSpawnOutput =
        serde_json::from_value(result).expect("Failed to deserialize result");
    assert_eq!(
        output.results,
        vec![2, 4, 6, 8, 10],
        "All values should be doubled"
    );

    Ok(())
}

// ============================================================================
// Parent-Child Relationship Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_child_has_parent_task_id(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "fanout_parent").await;
    client.create_queue(None).await.unwrap();
    client.register::<SingleSpawnTask>().await;
    client.register::<DoubleTask>().await;

    // Spawn parent task
    let spawn_result = client
        .spawn::<SingleSpawnTask>(SingleSpawnParams { child_value: 10 })
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2,
            ..Default::default()
        })
        .await;

    tokio::time::sleep(Duration::from_millis(2000)).await;
    worker.shutdown().await;

    // Find the child task (any task with parent_task_id set)
    let query = "SELECT task_id FROM durable.t_fanout_parent WHERE parent_task_id = $1";
    let child_ids: Vec<(uuid::Uuid,)> = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_all(&pool)
        .await?;

    assert_eq!(child_ids.len(), 1, "Should have exactly one child task");

    // Verify child's parent_task_id
    let child_parent = get_parent_task_id(&pool, "fanout_parent", child_ids[0].0).await;
    assert_eq!(
        child_parent,
        Some(spawn_result.task_id),
        "Child's parent_task_id should match parent"
    );

    // Verify parent has no parent (is root task)
    let parent_parent = get_parent_task_id(&pool, "fanout_parent", spawn_result.task_id).await;
    assert_eq!(
        parent_parent, None,
        "Parent task should not have a parent_task_id"
    );

    Ok(())
}

// ============================================================================
// Child Failure Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_child_failure_propagates_to_parent(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "fanout_fail").await;
    client.create_queue(None).await.unwrap();
    client.register::<SpawnFailingChildTask>().await;
    client.register::<FailingChildTask>().await;

    // Spawn parent task that will spawn a failing child
    // Use max_attempts=1 for both parent and child to avoid long retry waits
    let spawn_result = client
        .spawn_with_options::<SpawnFailingChildTask>(
            (),
            durable::SpawnOptions {
                max_attempts: Some(1),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to spawn task");

    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 4,
            ..Default::default()
        })
        .await;

    // Wait for tasks to complete - longer since child needs to fail first, then parent
    tokio::time::sleep(Duration::from_millis(5000)).await;
    worker.shutdown().await;

    // Parent should fail because child failed
    let state = get_task_state(&pool, "fanout_fail", spawn_result.task_id).await;
    assert_eq!(state, "failed", "Parent task should fail when child fails");

    Ok(())
}

// ============================================================================
// Cascade Cancellation Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_cascade_cancel_when_parent_cancelled(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "fanout_cancel").await;
    client.create_queue(None).await.unwrap();
    client.register::<SpawnSlowChildTask>().await;
    client.register::<SlowChildTask>().await;

    // Spawn parent task that will spawn a slow child (5 seconds)
    let spawn_result = client
        .spawn::<SpawnSlowChildTask>(SpawnSlowChildParams {
            child_sleep_ms: 5000,
        })
        .await
        .expect("Failed to spawn task");

    // Start worker to let parent spawn child
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2, // Process both parent and child
            ..Default::default()
        })
        .await;

    // Give time for parent to spawn child and child to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel the parent task while child is still running
    client
        .cancel_task(spawn_result.task_id, None)
        .await
        .unwrap();

    // Give time for cascade cancellation to propagate
    tokio::time::sleep(Duration::from_millis(200)).await;
    worker.shutdown().await;

    // Verify parent is cancelled
    let parent_state = get_task_state(&pool, "fanout_cancel", spawn_result.task_id).await;
    assert_eq!(parent_state, "cancelled", "Parent should be cancelled");

    // Find and verify all children are also cancelled
    let query = "SELECT state FROM durable.t_fanout_cancel WHERE parent_task_id = $1";
    let child_states: Vec<(String,)> = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_all(&pool)
        .await?;

    assert!(
        !child_states.is_empty(),
        "Should have at least one child task"
    );
    for (state,) in child_states {
        assert_eq!(
            state, "cancelled",
            "Child tasks should be cascade cancelled"
        );
    }

    Ok(())
}
