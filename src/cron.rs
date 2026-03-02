use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::HashMap;

use crate::client::{Durable, validate_headers};
use crate::error::{DurableError, DurableResult};
use crate::types::SpawnOptions;

/// Options for creating a cron schedule.
#[derive(Debug, Clone)]
pub struct ScheduleOptions {
    /// The task name to spawn on each cron tick.
    pub task_name: String,
    /// Standard 5-field cron expression (minute hour day-of-month month day-of-week).
    pub cron_expression: String,
    /// Parameters to pass to the spawned task (serialized as JSON).
    pub params: JsonValue,
    /// Spawn options (max_attempts, retry_strategy, cancellation, headers).
    pub spawn_options: SpawnOptions,
    /// Arbitrary user-defined metadata for categorization/filtering.
    pub metadata: Option<HashMap<String, JsonValue>>,
}

/// Information about an existing cron schedule.
#[derive(Debug, Clone)]
pub struct ScheduleInfo {
    /// The schedule name (unique within the queue).
    pub name: String,
    /// The cron expression.
    pub cron_expression: String,
    /// The task name that gets spawned.
    pub task_name: String,
    /// The parameters passed to the spawned task.
    pub params: JsonValue,
    /// The serialized spawn options.
    pub spawn_options: JsonValue,
    /// User-defined metadata.
    pub metadata: HashMap<String, JsonValue>,
    /// The pg_cron job name.
    pub pgcron_job_name: String,
    /// When the schedule was created.
    pub created_at: DateTime<Utc>,
    /// When the schedule was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Filter for listing schedules.
#[derive(Debug, Clone, Default)]
pub struct ScheduleFilter {
    /// Filter by task name (exact match).
    pub task_name: Option<String>,
    /// Filter by metadata (JSONB `@>` containment).
    /// e.g. `{"team": "payments"}` matches schedules whose metadata contains that key-value.
    pub metadata: Option<HashMap<String, JsonValue>>,
}

/// Set up the pg_cron extension in the database.
///
/// Attempts to create the extension and verifies it is available.
/// Call this once during application startup before using cron scheduling.
///
/// # Errors
///
/// Returns [`DurableError::PgCronUnavailable`] if the extension cannot be created
/// or is not available.
pub async fn setup_pgcron(pool: &PgPool) -> DurableResult<()> {
    // Attempt to create the extension, ignoring errors (user may not have privileges)
    let _ = sqlx::query(
        "DO $$ BEGIN CREATE EXTENSION IF NOT EXISTS pg_cron; EXCEPTION WHEN OTHERS THEN NULL; END $$"
    )
    .execute(pool)
    .await;

    // Verify it exists
    let exists: (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')")
            .fetch_one(pool)
            .await?;

    if !exists.0 {
        return Err(DurableError::PgCronUnavailable {
            reason: "pg_cron extension is not installed and could not be created".to_string(),
        });
    }

    Ok(())
}

impl<State> Durable<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Create or update a cron schedule.
    ///
    /// If a schedule with the same name already exists in this queue, it will be updated
    /// (upsert semantics). The pg_cron job is created/updated and the schedule metadata
    /// is stored in `durable.cron_schedules`.
    ///
    /// # Arguments
    ///
    /// * `schedule_name` - Unique name for this schedule within the queue
    ///   (alphanumeric, hyphens, and underscores only).
    /// * `options` - Schedule configuration including task name, cron expression,
    ///   params, spawn options, and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The schedule name is invalid
    /// - Headers use the reserved `durable::` prefix
    /// - pg_cron is not available
    /// - The cron expression is rejected by pg_cron
    pub async fn create_schedule(
        &self,
        schedule_name: &str,
        options: ScheduleOptions,
    ) -> DurableResult<()> {
        // Validate inputs
        validate_schedule_name(schedule_name)?;
        validate_headers(&options.spawn_options.headers)?;

        // Check pg_cron availability early before doing any work
        let exists: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')")
                .fetch_one(self.pool())
                .await?;

        if !exists.0 {
            return Err(DurableError::PgCronUnavailable {
                reason: "pg_cron extension is not installed".to_string(),
            });
        }

        let pgcron_job_name = format!("durable::{}::{}", self.queue_name(), schedule_name);

        // Build spawn options with injected durable:: headers
        let mut spawn_options = options.spawn_options.clone();
        let headers = spawn_options.headers.get_or_insert_with(HashMap::new);
        headers.insert(
            "durable::scheduled_by".to_string(),
            JsonValue::String(schedule_name.to_string()),
        );
        headers.insert(
            "durable::cron".to_string(),
            JsonValue::String(options.cron_expression.clone()),
        );

        let max_attempts = spawn_options
            .max_attempts
            .unwrap_or(self.spawn_defaults().max_attempts);
        let spawn_options = SpawnOptions {
            retry_strategy: spawn_options
                .retry_strategy
                .or_else(|| self.spawn_defaults().retry_strategy.clone()),
            cancellation: spawn_options
                .cancellation
                .or_else(|| self.spawn_defaults().cancellation.clone()),
            ..spawn_options
        };
        let db_options = Self::serialize_spawn_options(&spawn_options, max_attempts)
            .map_err(DurableError::Serialization)?;

        // Build the SQL command that pg_cron will execute
        let spawn_sql = build_pgcron_spawn_sql(
            self.queue_name(),
            &options.task_name,
            &options.params,
            &db_options,
        )?;

        let metadata_value = match &options.metadata {
            Some(m) => serde_json::to_value(m).map_err(DurableError::Serialization)?,
            None => serde_json::json!({}),
        };

        // Execute in a transaction
        let mut tx = self.pool().begin().await?;

        // Schedule the pg_cron job (has built-in upsert semantics)
        sqlx::query("SELECT cron.schedule($1, $2, $3)")
            .bind(&pgcron_job_name)
            .bind(&options.cron_expression)
            .bind(&spawn_sql)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_pgcron_error(e, "create"))?;

        // Upsert into our schedule registry
        sqlx::query(
            "INSERT INTO durable.cron_schedules
                (schedule_name, queue_name, task_name, cron_expression, params, spawn_options, metadata, pgcron_job_name, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, durable.current_time(), durable.current_time())
             ON CONFLICT (queue_name, schedule_name)
             DO UPDATE SET
                task_name = EXCLUDED.task_name,
                cron_expression = EXCLUDED.cron_expression,
                params = EXCLUDED.params,
                spawn_options = EXCLUDED.spawn_options,
                metadata = EXCLUDED.metadata,
                pgcron_job_name = EXCLUDED.pgcron_job_name,
                updated_at = durable.current_time()"
        )
        .bind(schedule_name)
        .bind(self.queue_name())
        .bind(&options.task_name)
        .bind(&options.cron_expression)
        .bind(&options.params)
        .bind(&db_options)
        .bind(&metadata_value)
        .bind(&pgcron_job_name)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Delete a cron schedule.
    ///
    /// Removes the pg_cron job and the schedule registry entry. Any in-flight
    /// tasks that were already spawned by this schedule are not cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError::ScheduleNotFound`] if the schedule does not exist.
    pub async fn delete_schedule(&self, schedule_name: &str) -> DurableResult<()> {
        // Look up the pgcron_job_name
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT pgcron_job_name FROM durable.cron_schedules
             WHERE queue_name = $1 AND schedule_name = $2",
        )
        .bind(self.queue_name())
        .bind(schedule_name)
        .fetch_optional(self.pool())
        .await?;

        let (pgcron_job_name,) = row.ok_or_else(|| DurableError::ScheduleNotFound {
            schedule_name: schedule_name.to_string(),
            queue_name: self.queue_name().to_string(),
        })?;

        let mut tx = self.pool().begin().await?;

        // Look up the jobid from cron.job and unschedule it
        let job_row: Option<(i64,)> =
            sqlx::query_as("SELECT jobid FROM cron.job WHERE jobname = $1")
                .bind(&pgcron_job_name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| map_pgcron_error(e, "delete"))?;

        if let Some((jobid,)) = job_row {
            sqlx::query("SELECT cron.unschedule($1)")
                .bind(jobid)
                .execute(&mut *tx)
                .await
                .map_err(|e| map_pgcron_error(e, "delete"))?;
        }

        // Delete from our registry
        sqlx::query(
            "DELETE FROM durable.cron_schedules
             WHERE queue_name = $1 AND schedule_name = $2",
        )
        .bind(self.queue_name())
        .bind(schedule_name)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// List cron schedules, optionally filtered.
    ///
    /// Returns all schedules for this queue. If a filter is provided, results
    /// are narrowed by task name and/or metadata containment.
    ///
    /// This only queries the `durable.cron_schedules` table (no pg_cron queries),
    /// so it works even if pg_cron is not installed.
    pub async fn list_schedules(
        &self,
        filter: Option<ScheduleFilter>,
    ) -> DurableResult<Vec<ScheduleInfo>> {
        let filter = filter.unwrap_or_default();

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT schedule_name, cron_expression, task_name, params, spawn_options, metadata, pgcron_job_name, created_at, updated_at
             FROM durable.cron_schedules
             WHERE queue_name = ",
        );
        qb.push_bind(self.queue_name());

        if let Some(ref task_name) = filter.task_name {
            qb.push(" AND task_name = ").push_bind(task_name.clone());
        }

        if let Some(ref metadata) = filter.metadata {
            let metadata_json =
                serde_json::to_value(metadata).map_err(DurableError::Serialization)?;
            qb.push(" AND metadata @> ")
                .push_bind(metadata_json)
                .push("::jsonb");
        }

        qb.push(" ORDER BY schedule_name");

        let rows: Vec<ScheduleRow> = qb
            .build_query_as::<ScheduleRow>()
            .fetch_all(self.pool())
            .await?;

        rows.into_iter()
            .map(|row| {
                let metadata: HashMap<String, JsonValue> =
                    serde_json::from_value(row.metadata).unwrap_or_default();
                Ok(ScheduleInfo {
                    name: row.schedule_name,
                    cron_expression: row.cron_expression,
                    task_name: row.task_name,
                    params: row.params,
                    spawn_options: row.spawn_options,
                    metadata,
                    pgcron_job_name: row.pgcron_job_name,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                })
            })
            .collect()
    }
}

// --- Internal types ---

#[derive(Debug, sqlx::FromRow)]
struct ScheduleRow {
    schedule_name: String,
    cron_expression: String,
    task_name: String,
    params: JsonValue,
    spawn_options: JsonValue,
    metadata: JsonValue,
    pgcron_job_name: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

// --- Validation helpers ---

/// Validate a schedule name (alphanumeric, hyphens, underscores only; non-empty).
fn validate_schedule_name(name: &str) -> DurableResult<()> {
    if name.is_empty() {
        return Err(DurableError::InvalidScheduleName {
            name: String::new(),
            reason: "schedule name cannot be empty".to_string(),
        });
    }

    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(DurableError::InvalidScheduleName {
            name: name.to_string(),
            reason:
                "contains invalid characters (only alphanumeric, hyphens, and underscores allowed)"
                    .to_string(),
        });
    }

    Ok(())
}

// --- SQL escaping ---

/// Dollar-quote a string using `$durable$` as the delimiter.
/// Returns an error if the content contains `$durable`, which would break the delimiter.
fn pg_literal(s: &str) -> Result<String, DurableError> {
    if s.contains("$durable") {
        return Err(DurableError::InvalidConfiguration {
            reason: format!("string contains reserved delimiter sequence '$durable': {s}"),
        });
    }
    Ok(format!("$durable${s}$durable$"))
}

/// Build the SQL command that pg_cron will execute to spawn a task.
fn build_pgcron_spawn_sql(
    queue_name: &str,
    task_name: &str,
    params: &JsonValue,
    spawn_options: &JsonValue,
) -> Result<String, DurableError> {
    let params_str = params.to_string();
    let options_str = spawn_options.to_string();

    Ok(format!(
        "SELECT durable.spawn_task({}, {}, {}::jsonb, {}::jsonb)",
        pg_literal(queue_name)?,
        pg_literal(task_name)?,
        pg_literal(&params_str)?,
        pg_literal(&options_str)?,
    ))
}

/// Map pgcron-related SQL errors to more descriptive error messages.
fn map_pgcron_error(err: sqlx::Error, operation: &str) -> DurableError {
    let err_str = err.to_string();
    // Only classify as PgCronUnavailable when the schema or function is missing,
    // not when pg_cron rejects invalid input (e.g. a bad cron expression).
    if err_str.contains("schema \"cron\" does not exist")
        || err_str.contains("function cron.schedule")
        || err_str.contains("function cron.unschedule")
    {
        DurableError::PgCronUnavailable {
            reason: format!("pg_cron error during {operation}: {err_str}"),
        }
    } else {
        DurableError::Database(err)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    // --- pg_literal tests ---

    #[test]
    fn test_pg_literal_simple() {
        assert_eq!(pg_literal("hello").unwrap(), "$durable$hello$durable$");
    }

    #[test]
    fn test_pg_literal_with_single_quotes() {
        assert_eq!(
            pg_literal("it's a test").unwrap(),
            "$durable$it's a test$durable$"
        );
    }

    #[test]
    fn test_pg_literal_with_json() {
        let json = r#"{"key": "value"}"#;
        assert_eq!(
            pg_literal(json).unwrap(),
            format!("$durable${json}$durable$")
        );
    }

    #[test]
    fn test_pg_literal_rejects_delimiter() {
        assert!(pg_literal("contains $durable$ in it").is_err());
    }

    #[test]
    fn test_pg_literal_rejects_partial_delimiter() {
        assert!(pg_literal("test$durable").is_err());
        assert!(pg_literal("$durablefoo").is_err());
        assert!(pg_literal("mid$durablemid").is_err());
    }

    // --- Schedule name validation tests ---

    #[test]
    fn test_valid_schedule_names() {
        assert!(validate_schedule_name("my-schedule").is_ok());
        assert!(validate_schedule_name("task_1").is_ok());
        assert!(validate_schedule_name("DailyReport").is_ok());
        assert!(validate_schedule_name("a").is_ok());
        assert!(validate_schedule_name("test-123_abc").is_ok());
    }

    #[test]
    fn test_invalid_schedule_name_empty() {
        let err = validate_schedule_name("").unwrap_err();
        assert!(matches!(err, DurableError::InvalidScheduleName { .. }));
    }

    #[test]
    fn test_invalid_schedule_name_spaces() {
        let err = validate_schedule_name("my schedule").unwrap_err();
        assert!(matches!(err, DurableError::InvalidScheduleName { .. }));
    }

    #[test]
    fn test_invalid_schedule_name_semicolons() {
        let err = validate_schedule_name("drop;table").unwrap_err();
        assert!(matches!(err, DurableError::InvalidScheduleName { .. }));
    }

    #[test]
    fn test_invalid_schedule_name_special_chars() {
        let err = validate_schedule_name("name@here").unwrap_err();
        assert!(matches!(err, DurableError::InvalidScheduleName { .. }));
        let err = validate_schedule_name("name.here").unwrap_err();
        assert!(matches!(err, DurableError::InvalidScheduleName { .. }));
        let err = validate_schedule_name("name/here").unwrap_err();
        assert!(matches!(err, DurableError::InvalidScheduleName { .. }));
    }

    // --- build_pgcron_spawn_sql tests ---

    #[test]
    fn test_build_pgcron_spawn_sql() {
        let params = serde_json::json!({"key": "value"});
        let options = serde_json::json!({"max_attempts": 3});
        let sql = build_pgcron_spawn_sql("my_queue", "my_task", &params, &options).unwrap();
        assert!(sql.contains("durable.spawn_task"));
        assert!(sql.contains("my_queue"));
        assert!(sql.contains("my_task"));
        assert!(sql.contains("::jsonb"));
    }

    // --- map_pgcron_error tests ---

    #[test]
    fn test_map_pgcron_error_schema_missing() {
        let err = sqlx::Error::Protocol(
            r#"error returned from database: schema "cron" does not exist"#.to_string(),
        );
        let result = map_pgcron_error(err, "create");
        assert!(
            matches!(result, DurableError::PgCronUnavailable { .. }),
            "expected PgCronUnavailable, got: {result:?}"
        );
    }

    #[test]
    fn test_map_pgcron_error_function_schedule_missing() {
        let err = sqlx::Error::Protocol(
            "error returned from database: function cron.schedule(unknown, unknown, unknown) does not exist".to_string(),
        );
        let result = map_pgcron_error(err, "create");
        assert!(
            matches!(result, DurableError::PgCronUnavailable { .. }),
            "expected PgCronUnavailable, got: {result:?}"
        );
    }

    #[test]
    fn test_map_pgcron_error_function_unschedule_missing() {
        let err = sqlx::Error::Protocol(
            "error returned from database: function cron.unschedule(bigint) does not exist"
                .to_string(),
        );
        let result = map_pgcron_error(err, "delete");
        assert!(
            matches!(result, DurableError::PgCronUnavailable { .. }),
            "expected PgCronUnavailable, got: {result:?}"
        );
    }

    #[test]
    fn test_map_pgcron_error_invalid_cron_expression_not_misclassified() {
        // pg_cron rejects bad cron expressions with an error containing "cron"
        let err = sqlx::Error::Protocol(
            "error returned from database: invalid cron expression".to_string(),
        );
        let result = map_pgcron_error(err, "create");
        assert!(
            matches!(result, DurableError::Database(_)),
            "expected Database error, got: {result:?}"
        );
    }

    #[test]
    fn test_map_pgcron_error_unrelated_error() {
        let err = sqlx::Error::Protocol("some other database error".to_string());
        let result = map_pgcron_error(err, "create");
        assert!(
            matches!(result, DurableError::Database(_)),
            "expected Database error, got: {result:?}"
        );
    }
}
