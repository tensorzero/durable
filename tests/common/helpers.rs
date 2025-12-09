use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgPool};
use std::time::Duration;
use uuid::Uuid;

/// Set fake time for deterministic testing.
/// Uses the durable.fake_now session variable.
#[allow(dead_code)]
pub async fn set_fake_time(pool: &PgPool, time: DateTime<Utc>) -> sqlx::Result<()> {
    let query = AssertSqlSafe(format!("SET durable.fake_now = '{}'", time.to_rfc3339()));
    sqlx::query(query).execute(pool).await?;
    Ok(())
}

/// Advance fake time by the given number of seconds.
#[allow(dead_code)]
pub async fn advance_time(pool: &PgPool, seconds: i64) -> sqlx::Result<()> {
    let current_time = current_time(pool).await?;
    let new_time = current_time + chrono::Duration::seconds(seconds);
    set_fake_time(pool, new_time).await
}

/// Clear fake time, returning to real time.
#[allow(dead_code)]
pub async fn clear_fake_time(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("RESET durable.fake_now").execute(pool).await?;
    Ok(())
}

/// Get the current time (respects fake_now if set).
#[allow(dead_code)]
pub async fn current_time(pool: &PgPool) -> sqlx::Result<DateTime<Utc>> {
    let (time,): (DateTime<Utc>,) = sqlx::query_as("SELECT durable.current_time()")
        .fetch_one(pool)
        .await?;
    Ok(time)
}

// ============================================================================
// Run inspection helpers
// ============================================================================

/// Count the number of runs for a given task.
#[allow(dead_code)]
pub async fn count_runs_for_task(pool: &PgPool, queue: &str, task_id: Uuid) -> sqlx::Result<i64> {
    let query = AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM durable.r_{} WHERE task_id = $1",
        queue
    ));
    let (count,): (i64,) = sqlx::query_as(query).bind(task_id).fetch_one(pool).await?;
    Ok(count)
}

/// Get the attempt number for a specific run.
#[allow(dead_code)]
pub async fn get_run_attempt(
    pool: &PgPool,
    queue: &str,
    run_id: Uuid,
) -> sqlx::Result<Option<i32>> {
    let query = AssertSqlSafe(format!(
        "SELECT attempt FROM durable.r_{} WHERE run_id = $1",
        queue
    ));
    let result: Option<(i32,)> = sqlx::query_as(query)
        .bind(run_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.map(|(a,)| a))
}

/// Get the claim_expires_at for a specific run.
#[allow(dead_code)]
pub async fn get_claim_expires_at(
    pool: &PgPool,
    queue: &str,
    run_id: Uuid,
) -> sqlx::Result<Option<DateTime<Utc>>> {
    let query = AssertSqlSafe(format!(
        "SELECT claim_expires_at FROM durable.r_{} WHERE run_id = $1",
        queue
    ));
    let result: Option<(Option<DateTime<Utc>>,)> = sqlx::query_as(query)
        .bind(run_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.and_then(|(t,)| t))
}

/// Get the state of a specific run.
#[allow(dead_code)]
pub async fn get_run_state(
    pool: &PgPool,
    queue: &str,
    run_id: Uuid,
) -> sqlx::Result<Option<String>> {
    let query = AssertSqlSafe(format!(
        "SELECT state FROM durable.r_{} WHERE run_id = $1",
        queue
    ));
    let result: Option<(String,)> = sqlx::query_as(query)
        .bind(run_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.map(|(s,)| s))
}

/// Get the state of a specific task.
#[allow(dead_code)]
pub async fn get_task_state(
    pool: &PgPool,
    queue: &str,
    task_id: Uuid,
) -> sqlx::Result<Option<String>> {
    let query = AssertSqlSafe(format!(
        "SELECT state FROM durable.t_{} WHERE task_id = $1",
        queue
    ));
    let result: Option<(String,)> = sqlx::query_as(query)
        .bind(task_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.map(|(s,)| s))
}

// ============================================================================
// Checkpoint inspection helpers
// ============================================================================

/// Count the number of checkpoints for a given task.
#[allow(dead_code)]
pub async fn get_checkpoint_count(pool: &PgPool, queue: &str, task_id: Uuid) -> sqlx::Result<i64> {
    let query = AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM durable.c_{} WHERE task_id = $1",
        queue
    ));
    let (count,): (i64,) = sqlx::query_as(query).bind(task_id).fetch_one(pool).await?;
    Ok(count)
}

/// Get checkpoint value by name for a task.
#[allow(dead_code)]
pub async fn get_checkpoint_value(
    pool: &PgPool,
    queue: &str,
    task_id: Uuid,
    checkpoint_name: &str,
) -> sqlx::Result<Option<serde_json::Value>> {
    let query = AssertSqlSafe(format!(
        "SELECT state FROM durable.c_{} WHERE task_id = $1 AND checkpoint_name = $2",
        queue
    ));
    let result: Option<(serde_json::Value,)> = sqlx::query_as(query)
        .bind(task_id)
        .bind(checkpoint_name)
        .fetch_optional(pool)
        .await?;
    Ok(result.map(|(s,)| s))
}

// ============================================================================
// Waiting helpers
// ============================================================================

/// Wait for a task to reach a specific state, with timeout.
#[allow(dead_code)]
pub async fn wait_for_task_state(
    pool: &PgPool,
    queue: &str,
    task_id: Uuid,
    target_state: &str,
    timeout: Duration,
) -> sqlx::Result<bool> {
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(50);

    while start.elapsed() < timeout {
        if let Some(state) = get_task_state(pool, queue, task_id).await?
            && state == target_state
        {
            return Ok(true);
        }
        tokio::time::sleep(poll_interval).await;
    }
    Ok(false)
}

/// Wait for a task to reach any terminal state (completed, failed, cancelled).
#[allow(dead_code)]
pub async fn wait_for_task_terminal(
    pool: &PgPool,
    queue: &str,
    task_id: Uuid,
    timeout: Duration,
) -> sqlx::Result<Option<String>> {
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(50);

    while start.elapsed() < timeout {
        if let Some(state) = get_task_state(pool, queue, task_id).await?
            && (state == "completed" || state == "failed" || state == "cancelled")
        {
            return Ok(Some(state));
        }
        tokio::time::sleep(poll_interval).await;
    }
    Ok(None)
}

/// Get the last run_id for a task.
#[allow(dead_code)]
pub async fn get_last_run_id(
    pool: &PgPool,
    queue: &str,
    task_id: Uuid,
) -> sqlx::Result<Option<Uuid>> {
    let query = AssertSqlSafe(format!(
        "SELECT last_attempt_run FROM durable.t_{} WHERE task_id = $1",
        queue
    ));
    let result: Option<(Option<Uuid>,)> = sqlx::query_as(query)
        .bind(task_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.and_then(|(r,)| r))
}
