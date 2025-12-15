#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::tasks::{DoubleTask, EchoParams, EchoTask, SingleSpawnParams, SingleSpawnTask};
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

/// Helper to directly set task state for testing.
async fn set_task_state(pool: &PgPool, queue: &str, task_id: uuid::Uuid, state: &str) {
    let query = AssertSqlSafe(format!(
        "UPDATE durable.t_{} SET state = $2 WHERE task_id = $1",
        queue
    ));
    sqlx::query(query)
        .bind(task_id)
        .bind(state)
        .execute(pool)
        .await
        .expect("Failed to set task state");
}

// ============================================================================
// Non-existent Task Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_nonexistent_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_nonexistent").await;
    client.create_queue(None).await.unwrap();

    let result = client
        .is_task_tree_blocked(uuid::Uuid::now_v7(), None)
        .await
        .unwrap();

    assert_eq!(result, None, "Non-existent task should return None");
    Ok(())
}

// ============================================================================
// Single Task State Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_single_pending_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_pending").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .unwrap();

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(result, Some(false), "Pending task should not be blocked");
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_single_sleeping_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_sleeping").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .unwrap();

    // Manually set state to sleeping
    set_task_state(&pool, "blocked_sleeping", spawn_result.task_id, "sleeping").await;

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(result, Some(true), "Sleeping task should be blocked");
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_single_completed_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_completed").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .unwrap();

    // Manually set state to completed
    set_task_state(
        &pool,
        "blocked_completed",
        spawn_result.task_id,
        "completed",
    )
    .await;

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(result, Some(true), "Completed task should be blocked");
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_single_failed_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_failed").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .unwrap();

    // Manually set state to failed
    set_task_state(&pool, "blocked_failed", spawn_result.task_id, "failed").await;

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(result, Some(true), "Failed task should be blocked");
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_single_cancelled_task(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_cancelled").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .unwrap();

    // Cancel the task
    client
        .cancel_task(spawn_result.task_id, None)
        .await
        .unwrap();

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(result, Some(true), "Cancelled task should be blocked");
    Ok(())
}

// ============================================================================
// Parent-Child Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_parent_blocked_child_pending(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_parent_child").await;
    client.create_queue(None).await.unwrap();
    client.register::<SingleSpawnTask>().await;
    client.register::<DoubleTask>().await;

    // Spawn parent task
    let spawn_result = client
        .spawn::<SingleSpawnTask>(SingleSpawnParams { child_value: 21 })
        .await
        .unwrap();

    // Start worker briefly to let parent spawn child, then stop
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2,
            ..Default::default()
        })
        .await;

    // Give time for parent to spawn child
    tokio::time::sleep(Duration::from_millis(300)).await;
    worker.shutdown().await;

    // Set parent to sleeping (simulating waiting for child)
    set_task_state(
        &pool,
        "blocked_parent_child",
        spawn_result.task_id,
        "sleeping",
    )
    .await;

    // Find child task
    let query = "SELECT task_id FROM durable.t_blocked_parent_child WHERE parent_task_id = $1";
    let child_ids: Vec<(uuid::Uuid,)> = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_all(&pool)
        .await?;

    // Set child to pending
    if !child_ids.is_empty() {
        set_task_state(&pool, "blocked_parent_child", child_ids[0].0, "pending").await;
    }

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(false),
        "Tree with pending child should not be blocked"
    );
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_parent_blocked_child_blocked(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_both").await;
    client.create_queue(None).await.unwrap();
    client.register::<SingleSpawnTask>().await;
    client.register::<DoubleTask>().await;

    // Spawn parent task
    let spawn_result = client
        .spawn::<SingleSpawnTask>(SingleSpawnParams { child_value: 21 })
        .await
        .unwrap();

    // Start worker briefly to let parent spawn child
    let worker = client
        .start_worker(WorkerOptions {
            poll_interval: 0.05,
            claim_timeout: 30,
            concurrency: 2,
            ..Default::default()
        })
        .await;

    // Give time for parent to spawn child
    tokio::time::sleep(Duration::from_millis(300)).await;
    worker.shutdown().await;

    // Set parent to sleeping
    set_task_state(&pool, "blocked_both", spawn_result.task_id, "sleeping").await;

    // Find and set child to completed
    let query = "SELECT task_id FROM durable.t_blocked_both WHERE parent_task_id = $1";
    let child_ids: Vec<(uuid::Uuid,)> = sqlx::query_as(query)
        .bind(spawn_result.task_id)
        .fetch_all(&pool)
        .await?;

    if !child_ids.is_empty() {
        set_task_state(&pool, "blocked_both", child_ids[0].0, "completed").await;
    }

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(true),
        "Tree with all tasks blocked should be blocked"
    );
    Ok(())
}

// ============================================================================
// Deep Tree Tests (3 levels)
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_deep_tree_all_blocked(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_deep_all").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Create root task
    let root = client
        .spawn::<EchoTask>(EchoParams {
            message: "root".to_string(),
        })
        .await
        .unwrap();

    // Create level 1 children manually in DB
    let child1_id = uuid::Uuid::now_v7();
    let child2_id = uuid::Uuid::now_v7();

    let insert_child =
        "INSERT INTO durable.t_blocked_deep_all (task_id, task_name, params, parent_task_id, state)
         VALUES ($1, 'echo', '{}', $2, 'completed')";
    sqlx::query(AssertSqlSafe(insert_child))
        .bind(child1_id)
        .bind(root.task_id)
        .execute(&pool)
        .await?;

    sqlx::query(AssertSqlSafe(insert_child))
        .bind(child2_id)
        .bind(root.task_id)
        .execute(&pool)
        .await?;

    // Create level 2 grandchildren
    let grandchild1_id = uuid::Uuid::now_v7();
    let grandchild2_id = uuid::Uuid::now_v7();

    let insert_grandchild =
        "INSERT INTO durable.t_blocked_deep_all (task_id, task_name, params, parent_task_id, state)
         VALUES ($1, 'echo', '{}', $2, 'sleeping')";
    sqlx::query(AssertSqlSafe(insert_grandchild))
        .bind(grandchild1_id)
        .bind(child1_id)
        .execute(&pool)
        .await?;

    sqlx::query(AssertSqlSafe(insert_grandchild))
        .bind(grandchild2_id)
        .bind(child2_id)
        .execute(&pool)
        .await?;

    // Set root to sleeping
    set_task_state(&pool, "blocked_deep_all", root.task_id, "sleeping").await;

    let result = client
        .is_task_tree_blocked(root.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(true),
        "Deep tree with all blocked should be blocked"
    );
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_deep_tree_leaf_running(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_deep_leaf").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Create root task
    let root = client
        .spawn::<EchoTask>(EchoParams {
            message: "root".to_string(),
        })
        .await
        .unwrap();

    // Create level 1 child
    let child_id = uuid::Uuid::now_v7();
    let insert_child = "INSERT INTO durable.t_blocked_deep_leaf (task_id, task_name, params, parent_task_id, state)
         VALUES ($1, 'echo', '{}', $2, 'completed')";
    sqlx::query(AssertSqlSafe(insert_child))
        .bind(child_id)
        .bind(root.task_id)
        .execute(&pool)
        .await?;

    // Create level 2 grandchild that is running
    let grandchild_id = uuid::Uuid::now_v7();
    let insert_grandchild = "INSERT INTO durable.t_blocked_deep_leaf (task_id, task_name, params, parent_task_id, state)
         VALUES ($1, 'echo', '{}', $2, 'running')";
    sqlx::query(AssertSqlSafe(insert_grandchild))
        .bind(grandchild_id)
        .bind(child_id)
        .execute(&pool)
        .await?;

    // Set root to sleeping
    set_task_state(&pool, "blocked_deep_leaf", root.task_id, "sleeping").await;

    let result = client
        .is_task_tree_blocked(root.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(false),
        "Deep tree with running leaf should not be blocked"
    );
    Ok(())
}

// ============================================================================
// Wide Tree Tests (many siblings)
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_wide_tree_all_blocked(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_wide_all").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Create root task
    let root = client
        .spawn::<EchoTask>(EchoParams {
            message: "root".to_string(),
        })
        .await
        .unwrap();

    // Create many children, all blocked
    for i in 0..10 {
        let child_id = uuid::Uuid::now_v7();
        let state = match i % 4 {
            0 => "completed",
            1 => "failed",
            2 => "cancelled",
            _ => "sleeping",
        };
        let insert_query = format!(
            "INSERT INTO durable.t_blocked_wide_all (task_id, task_name, params, parent_task_id, state)
             VALUES ($1, 'echo', '{{}}', $2, '{}')",
            state
        );
        sqlx::query(AssertSqlSafe(insert_query))
            .bind(child_id)
            .bind(root.task_id)
            .execute(&pool)
            .await?;
    }

    // Set root to sleeping
    set_task_state(&pool, "blocked_wide_all", root.task_id, "sleeping").await;

    let result = client
        .is_task_tree_blocked(root.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(true),
        "Wide tree with all blocked should be blocked"
    );
    Ok(())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_wide_tree_one_pending(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_wide_one").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    // Create root task
    let root = client
        .spawn::<EchoTask>(EchoParams {
            message: "root".to_string(),
        })
        .await
        .unwrap();

    // Create many children, all blocked except one
    for i in 0..10 {
        let child_id = uuid::Uuid::now_v7();
        let state = if i == 5 { "pending" } else { "completed" };
        let insert_query = format!(
            "INSERT INTO durable.t_blocked_wide_one (task_id, task_name, params, parent_task_id, state)
             VALUES ($1, 'echo', '{{}}', $2, '{}')",
            state
        );
        sqlx::query(AssertSqlSafe(insert_query))
            .bind(child_id)
            .bind(root.task_id)
            .execute(&pool)
            .await?;
    }

    // Set root to sleeping
    set_task_state(&pool, "blocked_wide_one", root.task_id, "sleeping").await;

    let result = client
        .is_task_tree_blocked(root.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(false),
        "Wide tree with one pending should not be blocked"
    );
    Ok(())
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_is_blocked_task_with_no_children(pool: PgPool) -> sqlx::Result<()> {
    let client = create_client(pool.clone(), "blocked_no_children").await;
    client.create_queue(None).await.unwrap();
    client.register::<EchoTask>().await;

    let spawn_result = client
        .spawn::<EchoTask>(EchoParams {
            message: "test".to_string(),
        })
        .await
        .unwrap();

    // Set to sleeping (blocked, but no children)
    set_task_state(
        &pool,
        "blocked_no_children",
        spawn_result.task_id,
        "sleeping",
    )
    .await;

    let result = client
        .is_task_tree_blocked(spawn_result.task_id, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(true),
        "Task with no children and blocked state should be blocked"
    );
    Ok(())
}
