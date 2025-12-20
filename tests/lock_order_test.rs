#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::tasks::{EventWaitParams, EventWaitingTask};
use durable::{Durable, MIGRATOR};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use std::future::Future;
use std::time::Duration;
use uuid::Uuid;

async fn create_client(pool: PgPool, queue_name: &str) -> Durable {
    Durable::builder()
        .pool(pool)
        .queue_name(queue_name)
        .build()
        .await
        .expect("Failed to create Durable client")
}

/// Verifies that a function locks task rows before run rows.
///
/// Pattern:
/// 1. Lock task row in tx_task (simulating cancel_task or similar)
/// 2. Call function_under_test in separate tokio task (will block on task lock if correct)
/// 3. Try to lock run row with NOWAIT
/// 4. If run lock succeeds → correct (function tried task first, is waiting)
/// 5. If run lock fails with 55P03 → wrong (function tried run first, got it)
async fn assert_locks_task_before_run<F, Fut>(
    pool: &PgPool,
    queue: &str,
    task_id: Uuid,
    run_id: Uuid,
    function_name: &str,
    function_under_test: F,
) -> sqlx::Result<()>
where
    F: FnOnce(PgPool) -> Fut + Send + 'static,
    Fut: Future<Output = sqlx::Result<()>> + Send,
{
    let task_table = format!("durable.t_{queue}");
    let run_table = format!("durable.r_{queue}");

    // Step 1: Lock task row
    let mut tx_task = pool.begin().await?;
    sqlx::query(AssertSqlSafe(format!(
        "SELECT task_id FROM {task_table} WHERE task_id = $1 FOR UPDATE"
    )))
    .bind(task_id)
    .execute(&mut *tx_task)
    .await?;

    // Step 2: Call function in separate task
    let pool_clone = pool.clone();
    let handle = tokio::spawn(async move { function_under_test(pool_clone).await });

    // Step 3: Wait for function to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Step 4: Try to lock run row with NOWAIT
    let mut tx_run = pool.begin().await?;
    let lock_result = sqlx::query(AssertSqlSafe(format!(
        "SELECT run_id FROM {run_table} WHERE run_id = $1 FOR UPDATE NOWAIT"
    )))
    .bind(run_id)
    .execute(&mut *tx_run)
    .await;

    // Step 5: Check result
    match lock_result {
        Ok(_) => tx_run.commit().await?,
        Err(e) => {
            if let sqlx::Error::Database(db) = &e
                && db.code().as_deref() == Some("55P03")
            {
                panic!("{function_name} locked run before task; expected task -> run ordering");
            }
            return Err(e);
        }
    }

    // Release task lock so function can complete
    tx_task.commit().await?;

    let join_result = tokio::time::timeout(Duration::from_secs(2), handle).await;
    let result = join_result.unwrap_or_else(|_| panic!("{function_name} timed out"));
    result.unwrap_or_else(|e| panic!("{function_name} task panicked: {e}"))?;

    Ok(())
}

/// Helper to set up a task/run in running state for lock order tests.
async fn setup_running_task(pool: &PgPool, queue: &str) -> sqlx::Result<(Uuid, Uuid)> {
    let client = create_client(pool.clone(), queue).await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    let spawn_result = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: "never".to_string(),
            timeout_seconds: Some(60),
        })
        .await
        .expect("Failed to spawn task");

    let task_id = spawn_result.task_id;
    let run_id = spawn_result.run_id;

    let task_table = format!("durable.t_{queue}");
    let run_table = format!("durable.r_{queue}");

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {task_table} SET state = 'running' WHERE task_id = $1"
    )))
    .bind(task_id)
    .execute(pool)
    .await?;

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {run_table} SET state = 'running' WHERE run_id = $1"
    )))
    .bind(run_id)
    .execute(pool)
    .await?;

    Ok((task_id, run_id))
}

/// Ensure complete_run locks task rows before run rows to avoid deadlocks with cancellation.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_complete_run_locks_task_before_run(pool: PgPool) -> sqlx::Result<()> {
    let queue = "lock_order_complete";
    let (_, run_id) = setup_running_task(&pool, queue).await?;

    let task_table = format!("durable.t_{queue}");
    let task_id: Uuid = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT task_id FROM {task_table} LIMIT 1"
    )))
    .fetch_one(&pool)
    .await?;

    assert_locks_task_before_run(&pool, queue, task_id, run_id, "complete_run", {
        let queue = queue.to_string();
        move |pool| async move {
            let mut tx = pool.begin().await?;
            sqlx::query("SELECT durable.complete_run($1, $2, $3)")
                .bind(&queue)
                .bind(run_id)
                .bind(json!({"ok": true}))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    })
    .await
}

/// Ensure fail_run locks task rows before run rows to avoid deadlocks with cancellation.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_fail_run_locks_task_before_run(pool: PgPool) -> sqlx::Result<()> {
    let queue = "lock_order_fail";
    let (task_id, run_id) = setup_running_task(&pool, queue).await?;

    assert_locks_task_before_run(&pool, queue, task_id, run_id, "fail_run", {
        let queue = queue.to_string();
        move |pool| async move {
            let mut tx = pool.begin().await?;
            sqlx::query("SELECT durable.fail_run($1, $2, $3)")
                .bind(&queue)
                .bind(run_id)
                .bind(json!({"error": "test error"}))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    })
    .await
}

/// Ensure sleep_for locks task rows before run rows to avoid deadlocks with cancellation.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_sleep_for_locks_task_before_run(pool: PgPool) -> sqlx::Result<()> {
    let queue = "lock_order_sleep";
    let (task_id, run_id) = setup_running_task(&pool, queue).await?;

    assert_locks_task_before_run(&pool, queue, task_id, run_id, "sleep_for", {
        let queue = queue.to_string();
        move |pool| async move {
            let mut tx = pool.begin().await?;
            sqlx::query("SELECT durable.sleep_for($1, $2, $3, $4)")
                .bind(&queue)
                .bind(run_id)
                .bind("test_checkpoint")
                .bind(1000i64)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    })
    .await
}

/// Ensure await_event locks task rows before run rows to avoid deadlocks with cancellation.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_await_event_locks_task_before_run(pool: PgPool) -> sqlx::Result<()> {
    let queue = "lock_order_await";
    let (task_id, run_id) = setup_running_task(&pool, queue).await?;

    assert_locks_task_before_run(&pool, queue, task_id, run_id, "await_event", {
        let queue = queue.to_string();
        move |pool| async move {
            let mut tx = pool.begin().await?;
            sqlx::query("SELECT * FROM durable.await_event($1, $2, $3, $4, $5, $6)")
                .bind(&queue)
                .bind(task_id)
                .bind(run_id)
                .bind("test_step")
                .bind("test_event")
                .bind(60i32)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    })
    .await
}

/// Ensure emit_event locks task rows before run rows to avoid deadlocks with cancellation.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_emit_event_locks_task_before_run(pool: PgPool) -> sqlx::Result<()> {
    let queue = "lock_order_emit";
    let event_name = "test_event";
    let client = create_client(pool.clone(), queue).await;
    client.create_queue(None).await.unwrap();
    client.register::<EventWaitingTask>().await.unwrap();

    let spawn_result = client
        .spawn::<EventWaitingTask>(EventWaitParams {
            event_name: event_name.to_string(),
            timeout_seconds: Some(60),
        })
        .await
        .expect("Failed to spawn task");

    let task_id = spawn_result.task_id;
    let run_id = spawn_result.run_id;

    let task_table = format!("durable.t_{queue}");
    let run_table = format!("durable.r_{queue}");
    let wait_table = format!("durable.w_{queue}");

    // Set task and run to sleeping state (simulating await_event)
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {task_table} SET state = 'sleeping' WHERE task_id = $1"
    )))
    .bind(task_id)
    .execute(&pool)
    .await?;

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {run_table} SET state = 'sleeping' WHERE run_id = $1"
    )))
    .bind(run_id)
    .execute(&pool)
    .await?;

    // Insert a wait registration for the event
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {wait_table} (run_id, task_id, event_name, step_name, timeout_at) VALUES ($1, $2, $3, 'test_step', NULL)"
    )))
    .bind(run_id)
    .bind(task_id)
    .bind(event_name)
    .execute(&pool)
    .await?;

    // emit_event has special setup (sleeping + wait registration), so we use it directly
    assert_locks_task_before_run(&pool, queue, task_id, run_id, "emit_event", {
        let queue = queue.to_string();
        let event_name = event_name.to_string();
        move |pool| async move {
            let mut tx = pool.begin().await?;
            sqlx::query("SELECT durable.emit_event($1, $2, $3)")
                .bind(&queue)
                .bind(&event_name)
                .bind(json!({"data": "test"}))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    })
    .await
}
