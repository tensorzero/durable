use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgPool};

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
