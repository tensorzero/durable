#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::tasks::{EventWaitParams, EventWaitingTask};
use durable::{Durable, MIGRATOR};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use std::time::Duration;

async fn create_client(pool: PgPool, queue_name: &str) -> Durable {
    Durable::builder()
        .pool(pool)
        .queue_name(queue_name)
        .build()
        .await
        .expect("Failed to create Durable client")
}

/// Ensure complete_run locks task rows before run rows to avoid deadlocks with cancellation.
#[sqlx::test(migrator = "MIGRATOR")]
async fn test_complete_run_locks_task_before_run(pool: PgPool) -> sqlx::Result<()> {
    let queue = "lock_order";
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
    .execute(&pool)
    .await?;

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {run_table} SET state = 'running' WHERE run_id = $1"
    )))
    .bind(run_id)
    .execute(&pool)
    .await?;

    let mut tx_task = pool.begin().await?;
    sqlx::query(AssertSqlSafe(format!(
        "SELECT task_id FROM {task_table} WHERE task_id = $1 FOR UPDATE"
    )))
    .bind(task_id)
    .execute(&mut *tx_task)
    .await?;

    let pool_clone = pool.clone();
    let complete_handle = tokio::spawn(async move {
        let mut tx = pool_clone.begin().await?;
        sqlx::query("SELECT durable.complete_run($1, $2, $3)")
            .bind(queue)
            .bind(run_id)
            .bind(json!({"ok": true}))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok::<(), sqlx::Error>(())
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut tx_run = pool.begin().await?;
    let lock_result = sqlx::query(AssertSqlSafe(format!(
        "SELECT run_id FROM {run_table} WHERE run_id = $1 FOR UPDATE NOWAIT"
    )))
    .bind(run_id)
    .execute(&mut *tx_run)
    .await;

    match lock_result {
        Ok(_) => {
            tx_run.commit().await?;
        }
        Err(e) => {
            if let sqlx::Error::Database(db) = &e
                && db.code().as_deref() == Some("55P03")
            {
                panic!("complete_run locked run before task; expected task -> run ordering");
            }
            return Err(e);
        }
    }

    tx_task.commit().await?;

    let join_result = tokio::time::timeout(Duration::from_secs(2), complete_handle).await;
    let complete_result = join_result.expect("complete_run timed out");
    complete_result.expect("complete_run task panicked")?;

    Ok(())
}
