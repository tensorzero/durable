#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use durable::{
    Durable, DurableError, MIGRATOR, ScheduleFilter, ScheduleOptions, SpawnOptions, setup_pgcron,
};
use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_setup_pgcron(pool: PgPool) {
    setup_pgcron(&pool).await.unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_and_list_schedule(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_create_list")
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

    // Cleanup
    durable.delete_schedule("payment-schedule").await.unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_schedule_upsert(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_upsert")
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

    // Cleanup
    durable.delete_schedule("my-schedule").await.unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_delete_schedule(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_delete")
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
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_delete_nonexistent_schedule(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_delete_missing")
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
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_create_schedule_invalid_cron(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_invalid")
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

    // pg_cron validates the expression and the transaction should fail
    let result = durable.create_schedule("bad-cron", options).await;
    assert!(result.is_err());
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_schedule_injects_metadata_headers(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_headers")
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

    // Cleanup
    durable.delete_schedule("header-test").await.unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_list_schedules_filter_by_metadata(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_filter_meta")
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

    // Cleanup
    durable.delete_schedule("schedule-a").await.unwrap();
    durable.delete_schedule("schedule-b").await.unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_list_schedules_filter_by_task_name(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_filter_task")
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

    // Cleanup
    durable.delete_schedule("orders-1").await.unwrap();
    durable.delete_schedule("orders-2").await.unwrap();
    durable.delete_schedule("reports").await.unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn test_list_schedules_combined_filter(pool: PgPool) {
    let durable = Durable::builder()
        .pool(pool.clone())
        .queue_name("test_cron_filter_combo")
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

    // Cleanup
    durable.delete_schedule("data-payments").await.unwrap();
    durable.delete_schedule("data-billing").await.unwrap();
    durable.delete_schedule("alerts-payments").await.unwrap();
}
