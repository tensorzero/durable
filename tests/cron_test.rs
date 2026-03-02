#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use durable::{
    Durable, DurableError, MIGRATOR, ScheduleFilter, ScheduleOptions, SpawnOptions, setup_pgcron,
};
use serde_json::json;
use sqlx::migrate::MigrateDatabase;
use sqlx::{AssertSqlSafe, PgPool, Row};
use std::collections::HashMap;

/// Connect to the real `test` database (where pg_cron lives) and run migrations.
/// `sqlx::test` creates temporary databases where pg_cron cannot be installed,
/// so cron tests must use the real database instead.
async fn setup_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Ensure the database exists
    if !sqlx::Postgres::database_exists(&url).await.unwrap() {
        sqlx::Postgres::create_database(&url).await.unwrap();
    }

    let pool = PgPool::connect(&url).await.expect("connect to test db");
    MIGRATOR.run(&pool).await.expect("run migrations");
    setup_pgcron(&pool).await.expect("setup pg_cron");
    pool
}

/// Clean up a queue after a test (removes cron schedules, pg_cron jobs, and queue tables).
async fn cleanup_queue(pool: &PgPool, queue_name: &str) {
    sqlx::query("SELECT durable.drop_queue($1)")
        .bind(queue_name)
        .execute(pool)
        .await
        .expect("cleanup queue");
}

#[tokio::test]
async fn test_setup_pgcron() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&url).await.expect("connect to test db");
    MIGRATOR.run(&pool).await.expect("run migrations");
    setup_pgcron(&pool).await.unwrap();
}

#[tokio::test]
async fn test_create_and_list_schedule() {
    let pool = setup_pool().await;
    let queue = "test_cron_create_list";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let mut metadata = HashMap::new();
    metadata.insert("team".to_string(), json!("payments"));
    metadata.insert("env".to_string(), json!("production"));

    let options = ScheduleOptions {
        task_name: "process-payments".to_string(),
        cron_expression: "*/5 * * * *".to_string(),
        params: json!({"batch_size": 100}),
        spawn_options: SpawnOptions::default(),
        metadata: Some(metadata),
    };

    durable
        .create_schedule("payment-schedule", options)
        .await
        .unwrap();

    let schedules = durable.list_schedules(None).await.unwrap();
    assert_eq!(schedules.len(), 1);

    let schedule = &schedules[0];
    assert_eq!(schedule.name, "payment-schedule");
    assert_eq!(schedule.cron_expression, "*/5 * * * *");
    assert_eq!(schedule.task_name, "process-payments");
    assert_eq!(schedule.params, json!({"batch_size": 100}));
    assert_eq!(schedule.metadata.get("team"), Some(&json!("payments")));
    assert_eq!(schedule.metadata.get("env"), Some(&json!("production")));
    assert_eq!(
        schedule.pgcron_job_name,
        "durable::test_cron_create_list::payment-schedule"
    );

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_create_schedule_upsert() {
    let pool = setup_pool().await;
    let queue = "test_cron_upsert";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    // Create initial schedule
    let options = ScheduleOptions {
        task_name: "task-v1".to_string(),
        cron_expression: "*/5 * * * *".to_string(),
        params: json!({"version": 1}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable
        .create_schedule("my-schedule", options)
        .await
        .unwrap();

    // Update with same name
    let options2 = ScheduleOptions {
        task_name: "task-v2".to_string(),
        cron_expression: "*/10 * * * *".to_string(),
        params: json!({"version": 2}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable
        .create_schedule("my-schedule", options2)
        .await
        .unwrap();

    // Should still be just 1 schedule, but updated
    let schedules = durable.list_schedules(None).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].task_name, "task-v2");
    assert_eq!(schedules[0].cron_expression, "*/10 * * * *");
    assert_eq!(schedules[0].params, json!({"version": 2}));

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_delete_schedule() {
    let pool = setup_pool().await;
    let queue = "test_cron_delete";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let options = ScheduleOptions {
        task_name: "cleanup-task".to_string(),
        cron_expression: "0 0 * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable
        .create_schedule("daily-cleanup", options)
        .await
        .unwrap();

    // Verify it exists
    let schedules = durable.list_schedules(None).await.unwrap();
    assert_eq!(schedules.len(), 1);

    // Delete it
    durable.delete_schedule("daily-cleanup").await.unwrap();

    // Verify it's gone
    let schedules = durable.list_schedules(None).await.unwrap();
    assert_eq!(schedules.len(), 0);

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_delete_nonexistent_schedule() {
    let pool = setup_pool().await;
    let queue = "test_cron_delete_missing";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let result = durable.delete_schedule("nonexistent").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DurableError::ScheduleNotFound {
            schedule_name,
            queue_name,
        } => {
            assert_eq!(schedule_name, "nonexistent");
            assert_eq!(queue_name, "test_cron_delete_missing");
        }
        other => panic!("expected ScheduleNotFound, got: {other:?}"),
    }

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_create_schedule_invalid_cron() {
    let pool = setup_pool().await;
    let queue = "test_cron_invalid";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let options = ScheduleOptions {
        task_name: "my-task".to_string(),
        cron_expression: "invalid cron".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };

    // pg_cron validates the expression and the transaction should fail.
    // Crucially, this should be a Database error, NOT PgCronUnavailable.
    let result = durable.create_schedule("bad-cron", options).await;
    let err = result.unwrap_err();
    assert!(
        !matches!(err, DurableError::PgCronUnavailable { .. }),
        "invalid cron expression should not be classified as PgCronUnavailable, got: {err:?}"
    );

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_schedule_injects_metadata_headers() {
    let pool = setup_pool().await;
    let queue = "test_cron_headers";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let options = ScheduleOptions {
        task_name: "header-task".to_string(),
        cron_expression: "0 0 * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable
        .create_schedule("header-test", options)
        .await
        .unwrap();

    // Verify the spawn_options in the registry contain the durable:: headers
    let schedules = durable.list_schedules(None).await.unwrap();
    assert_eq!(schedules.len(), 1);

    let spawn_opts = &schedules[0].spawn_options;
    let headers = spawn_opts.get("headers").expect("should have headers");
    assert_eq!(
        headers.get("durable::scheduled_by"),
        Some(&json!("header-test"))
    );
    assert_eq!(headers.get("durable::cron"), Some(&json!("0 0 * * *")));

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_list_schedules_filter_by_metadata() {
    let pool = setup_pool().await;
    let queue = "test_cron_filter_meta";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    // Create schedules with different metadata
    let mut meta_payments = HashMap::new();
    meta_payments.insert("team".to_string(), json!("payments"));

    let mut meta_billing = HashMap::new();
    meta_billing.insert("team".to_string(), json!("billing"));

    let options1 = ScheduleOptions {
        task_name: "task-a".to_string(),
        cron_expression: "*/5 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: Some(meta_payments.clone()),
    };
    durable
        .create_schedule("schedule-a", options1)
        .await
        .unwrap();

    let options2 = ScheduleOptions {
        task_name: "task-b".to_string(),
        cron_expression: "*/10 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: Some(meta_billing),
    };
    durable
        .create_schedule("schedule-b", options2)
        .await
        .unwrap();

    // Filter by payments team
    let filter = ScheduleFilter {
        metadata: Some(meta_payments),
        ..Default::default()
    };
    let schedules = durable.list_schedules(Some(filter)).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "schedule-a");

    // No filter returns both
    let all = durable.list_schedules(None).await.unwrap();
    assert_eq!(all.len(), 2);

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_list_schedules_filter_by_task_name() {
    let pool = setup_pool().await;
    let queue = "test_cron_filter_task";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let options1 = ScheduleOptions {
        task_name: "process-orders".to_string(),
        cron_expression: "*/5 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable.create_schedule("orders-1", options1).await.unwrap();

    let options2 = ScheduleOptions {
        task_name: "process-orders".to_string(),
        cron_expression: "*/15 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable.create_schedule("orders-2", options2).await.unwrap();

    let options3 = ScheduleOptions {
        task_name: "send-reports".to_string(),
        cron_expression: "0 9 * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable.create_schedule("reports", options3).await.unwrap();

    // Filter by task name
    let filter = ScheduleFilter {
        task_name: Some("process-orders".to_string()),
        ..Default::default()
    };
    let schedules = durable.list_schedules(Some(filter)).await.unwrap();
    assert_eq!(schedules.len(), 2);
    assert!(schedules.iter().all(|s| s.task_name == "process-orders"));

    // Filter by different task name
    let filter = ScheduleFilter {
        task_name: Some("send-reports".to_string()),
        ..Default::default()
    };
    let schedules = durable.list_schedules(Some(filter)).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "reports");

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_list_schedules_combined_filter() {
    let pool = setup_pool().await;
    let queue = "test_cron_filter_combo";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    let mut meta_payments = HashMap::new();
    meta_payments.insert("team".to_string(), json!("payments"));

    let mut meta_billing = HashMap::new();
    meta_billing.insert("team".to_string(), json!("billing"));

    // Same task, different metadata
    let options1 = ScheduleOptions {
        task_name: "process-data".to_string(),
        cron_expression: "*/5 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: Some(meta_payments.clone()),
    };
    durable
        .create_schedule("data-payments", options1)
        .await
        .unwrap();

    let options2 = ScheduleOptions {
        task_name: "process-data".to_string(),
        cron_expression: "*/10 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: Some(meta_billing.clone()),
    };
    durable
        .create_schedule("data-billing", options2)
        .await
        .unwrap();

    // Different task, same metadata
    let options3 = ScheduleOptions {
        task_name: "send-alerts".to_string(),
        cron_expression: "0 * * * *".to_string(),
        params: json!({}),
        spawn_options: SpawnOptions::default(),
        metadata: Some(meta_payments.clone()),
    };
    durable
        .create_schedule("alerts-payments", options3)
        .await
        .unwrap();

    // Filter by task + metadata
    let filter = ScheduleFilter {
        task_name: Some("process-data".to_string()),
        metadata: Some(meta_payments),
    };
    let schedules = durable.list_schedules(Some(filter)).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "data-payments");

    cleanup_queue(&pool, queue).await;
}

#[tokio::test]
async fn test_pgcron_job_actually_spawns_task() {
    let pool = setup_pool().await;
    let queue = "test_cron_fires";

    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name(queue)
        .build()
        .await
        .unwrap();

    durable.create_queue(None).await.unwrap();

    // Schedule a job that fires every 2 seconds
    let options = ScheduleOptions {
        task_name: "cron-ping".to_string(),
        cron_expression: "2 seconds".to_string(),
        params: json!({"source": "cron"}),
        spawn_options: SpawnOptions::default(),
        metadata: None,
    };
    durable
        .create_schedule("fast-schedule", options)
        .await
        .unwrap();

    // Poll the task table until pg_cron spawns at least one task (up to 3s timeout)
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(3);
    let mut count: i64 = 0;
    while start.elapsed() < timeout {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let row: (i64,) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM durable.t_{queue}"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        count = row.0;
        if count > 0 {
            break;
        }
    }

    assert!(count > 0, "pg_cron should have spawned at least one task");

    // Verify the spawned task has the right task_name and params
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT task_name, params FROM durable.t_{queue} LIMIT 1"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();

    let task_name: &str = row.get("task_name");
    let params: serde_json::Value = row.get("params");
    assert_eq!(task_name, "cron-ping");
    assert_eq!(params, json!({"source": "cron"}));

    cleanup_queue(&pool, queue).await;
}
