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
    /// - The cron expression is invalid
    /// - Headers use the reserved `durable::` prefix
    /// - pg_cron is not available
    pub async fn create_schedule(
        &self,
        schedule_name: &str,
        options: ScheduleOptions,
    ) -> DurableResult<()> {
        // Validate inputs
        validate_schedule_name(schedule_name)?;
        validate_cron_expression(&options.cron_expression)?;
        validate_headers(&options.spawn_options.headers)?;

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
        );

        let metadata_value = match &options.metadata {
            Some(m) => serde_json::to_value(m).map_err(DurableError::Serialization)?,
            None => serde_json::json!({}),
        };

        // Check pg_cron availability
        let exists: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')")
                .fetch_one(self.pool())
                .await?;

        if !exists.0 {
            return Err(DurableError::PgCronUnavailable {
                reason: "pg_cron extension is not installed".to_string(),
            });
        }

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
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
             ON CONFLICT (queue_name, schedule_name)
             DO UPDATE SET
                task_name = EXCLUDED.task_name,
                cron_expression = EXCLUDED.cron_expression,
                params = EXCLUDED.params,
                spawn_options = EXCLUDED.spawn_options,
                metadata = EXCLUDED.metadata,
                pgcron_job_name = EXCLUDED.pgcron_job_name,
                updated_at = now()"
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

/// Validate a 5-field standard cron expression.
fn validate_cron_expression(expr: &str) -> DurableResult<()> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(DurableError::InvalidCronExpression {
            expression: expr.to_string(),
            reason: format!("expected 5 fields, got {}", fields.len()),
        });
    }

    let field_names = ["minute", "hour", "day-of-month", "month", "day-of-week"];
    let field_ranges: [(u32, u32); 5] = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)];

    for (i, field) in fields.iter().enumerate() {
        validate_cron_field(
            expr,
            field,
            field_names[i],
            field_ranges[i].0,
            field_ranges[i].1,
        )?;
    }

    Ok(())
}

fn validate_cron_field(
    expr: &str,
    field: &str,
    name: &str,
    min: u32,
    max: u32,
) -> DurableResult<()> {
    if field.is_empty() {
        return Err(DurableError::InvalidCronExpression {
            expression: expr.to_string(),
            reason: format!("{name} field is empty"),
        });
    }

    // Split by comma for lists (e.g., "1,15,30")
    for part in field.split(',') {
        validate_cron_part(expr, part, name, min, max)?;
    }

    Ok(())
}

fn validate_cron_part(expr: &str, part: &str, name: &str, min: u32, max: u32) -> DurableResult<()> {
    // Handle step values (e.g., "*/5" or "1-30/5")
    let (range_part, _step) = if let Some((range, step_str)) = part.split_once('/') {
        let step: u32 = step_str
            .parse()
            .map_err(|_| DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("invalid step value `{step_str}` in {name} field"),
            })?;
        if step == 0 {
            return Err(DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("step value cannot be 0 in {name} field"),
            });
        }
        (range, Some(step))
    } else {
        (part, None)
    };

    // Handle wildcard
    if range_part == "*" {
        return Ok(());
    }

    // Handle range (e.g., "1-30")
    if let Some((start_str, end_str)) = range_part.split_once('-') {
        let start: u32 = start_str
            .parse()
            .map_err(|_| DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("invalid range start `{start_str}` in {name} field"),
            })?;
        let end: u32 = end_str
            .parse()
            .map_err(|_| DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("invalid range end `{end_str}` in {name} field"),
            })?;

        if start < min || start > max {
            return Err(DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("{name} range start {start} out of range {min}-{max}"),
            });
        }
        if end < min || end > max {
            return Err(DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("{name} range end {end} out of range {min}-{max}"),
            });
        }
        if start > end {
            return Err(DurableError::InvalidCronExpression {
                expression: expr.to_string(),
                reason: format!("{name} range start {start} is greater than end {end}"),
            });
        }

        return Ok(());
    }

    // Handle single value
    let value: u32 = range_part
        .parse()
        .map_err(|_| DurableError::InvalidCronExpression {
            expression: expr.to_string(),
            reason: format!("invalid value `{range_part}` in {name} field"),
        })?;

    if value < min || value > max {
        return Err(DurableError::InvalidCronExpression {
            expression: expr.to_string(),
            reason: format!("{name} value {value} out of range {min}-{max}"),
        });
    }

    Ok(())
}

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
/// Falls back to escaped single quotes if the content contains `$durable$`.
fn pg_literal(s: &str) -> String {
    if !s.contains("$durable$") {
        format!("$durable${s}$durable$")
    } else {
        // Fallback: single-quote escaping (double up any single quotes)
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// Build the SQL command that pg_cron will execute to spawn a task.
fn build_pgcron_spawn_sql(
    queue_name: &str,
    task_name: &str,
    params: &JsonValue,
    spawn_options: &JsonValue,
) -> String {
    let params_str = params.to_string();
    let options_str = spawn_options.to_string();

    format!(
        "SELECT durable.spawn_task({}, {}, {}::jsonb, {}::jsonb)",
        pg_literal(queue_name),
        pg_literal(task_name),
        pg_literal(&params_str),
        pg_literal(&options_str),
    )
}

/// Map pgcron-related SQL errors to more descriptive error messages.
fn map_pgcron_error(err: sqlx::Error, operation: &str) -> DurableError {
    let err_str = err.to_string();
    if err_str.contains("cron") || err_str.contains("pg_cron") {
        DurableError::PgCronUnavailable {
            reason: format!("pg_cron error during {operation}: {err_str}"),
        }
    } else {
        DurableError::Database(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Cron expression validation tests ---

    #[test]
    fn test_valid_cron_every_minute() {
        assert!(validate_cron_expression("* * * * *").is_ok());
    }

    #[test]
    fn test_valid_cron_every_5_minutes() {
        assert!(validate_cron_expression("*/5 * * * *").is_ok());
    }

    #[test]
    fn test_valid_cron_range_with_step() {
        assert!(validate_cron_expression("0-30/5 * * * *").is_ok());
    }

    #[test]
    fn test_valid_cron_list() {
        assert!(validate_cron_expression("1,15,30 * * * *").is_ok());
    }

    #[test]
    fn test_valid_cron_specific_time() {
        assert!(validate_cron_expression("0 9 * * 1").is_ok());
    }

    #[test]
    fn test_valid_cron_complex() {
        assert!(validate_cron_expression("30 2 1,15 * 0-5").is_ok());
    }

    #[test]
    fn test_valid_cron_weekday_7() {
        // 7 is valid for day-of-week (Sunday in some implementations)
        assert!(validate_cron_expression("0 0 * * 7").is_ok());
    }

    #[test]
    fn test_invalid_cron_too_few_fields() {
        let result = validate_cron_expression("* * *");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DurableError::InvalidCronExpression { reason, .. } => {
                assert!(reason.contains("expected 5 fields"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_invalid_cron_too_many_fields() {
        let result = validate_cron_expression("* * * * * *");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DurableError::InvalidCronExpression { reason, .. } => {
                assert!(reason.contains("expected 5 fields"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_invalid_cron_6_field_seconds() {
        let result = validate_cron_expression("0 */5 * * * *");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_out_of_range_minute() {
        let result = validate_cron_expression("60 * * * *");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DurableError::InvalidCronExpression { reason, .. } => {
                assert!(reason.contains("out of range"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_invalid_cron_out_of_range_hour() {
        let result = validate_cron_expression("0 24 * * *");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_out_of_range_day() {
        let result = validate_cron_expression("0 0 32 * *");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_out_of_range_month() {
        let result = validate_cron_expression("0 0 * 13 *");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_out_of_range_weekday() {
        let result = validate_cron_expression("0 0 * * 8");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_empty() {
        let result = validate_cron_expression("");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_letters() {
        let result = validate_cron_expression("abc * * * *");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_zero_step() {
        let result = validate_cron_expression("*/0 * * * *");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DurableError::InvalidCronExpression { reason, .. } => {
                assert!(reason.contains("step value cannot be 0"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_invalid_cron_reversed_range() {
        let result = validate_cron_expression("30-10 * * * *");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DurableError::InvalidCronExpression { reason, .. } => {
                assert!(reason.contains("greater than end"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    // --- pg_literal tests ---

    #[test]
    fn test_pg_literal_simple() {
        assert_eq!(pg_literal("hello"), "$durable$hello$durable$");
    }

    #[test]
    fn test_pg_literal_with_single_quotes() {
        assert_eq!(pg_literal("it's a test"), "$durable$it's a test$durable$");
    }

    #[test]
    fn test_pg_literal_with_json() {
        let json = r#"{"key": "value"}"#;
        assert_eq!(pg_literal(json), format!("$durable${json}$durable$"));
    }

    #[test]
    fn test_pg_literal_fallback_on_delimiter_conflict() {
        let content = "contains $durable$ in it";
        assert_eq!(pg_literal(content), "'contains $durable$ in it'");
    }

    #[test]
    fn test_pg_literal_fallback_escapes_quotes() {
        let content = "has $durable$ and 'quotes'";
        assert_eq!(pg_literal(content), "'has $durable$ and ''quotes'''");
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
        let sql = build_pgcron_spawn_sql("my_queue", "my_task", &params, &options);
        assert!(sql.contains("durable.spawn_task"));
        assert!(sql.contains("my_queue"));
        assert!(sql.contains("my_task"));
        assert!(sql.contains("::jsonb"));
    }
}
