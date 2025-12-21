#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sqlx::AssertSqlSafe;
use std::time::Duration;

mod common;
use common::setup::{BenchContext, bench_worker_options, wait_for_tasks_complete};
use common::tasks::{
    LargePayloadBenchTask, LargePayloadParams, MultiStepBenchTask, MultiStepParams,
};

/// Benchmark: step() overhead for cache miss (first execution)
fn bench_step_cache_miss(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("step_cache_miss");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);

    // Test with different step counts
    for num_steps in [10u32, 50, 100] {
        group.throughput(Throughput::Elements(num_steps as u64));
        group.bench_with_input(
            BenchmarkId::new("steps", num_steps),
            &num_steps,
            |b, &num_steps| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total_time = Duration::ZERO;

                        for _ in 0..iters {
                            let ctx = BenchContext::new().await;
                            ctx.client.register::<MultiStepBenchTask>().await.unwrap();

                            ctx.client
                                .spawn::<MultiStepBenchTask>(MultiStepParams { num_steps })
                                .await
                                .unwrap();

                            let start = std::time::Instant::now();
                            let worker = ctx
                                .client
                                .start_worker(bench_worker_options(1, Duration::from_secs(120)))
                                .await;

                            wait_for_tasks_complete(&ctx.pool, &ctx.queue_name, 1, 60).await;
                            total_time += start.elapsed();

                            worker.shutdown().await;
                            ctx.cleanup().await;
                        }

                        total_time
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: step() overhead for cache hit (replay scenario)
/// This simulates re-execution of a task where checkpoints already exist.
fn bench_step_cache_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("step_cache_hit");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);

    for num_steps in [10u32, 50, 100] {
        group.throughput(Throughput::Elements(num_steps as u64));
        group.bench_with_input(
            BenchmarkId::new("steps", num_steps),
            &num_steps,
            |b, &num_steps| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total_time = Duration::ZERO;

                        for _ in 0..iters {
                            let ctx = BenchContext::new().await;
                            ctx.client.register::<MultiStepBenchTask>().await.unwrap();

                            // First run to populate checkpoints
                            let spawn_result = ctx
                                .client
                                .spawn::<MultiStepBenchTask>(MultiStepParams { num_steps })
                                .await
                                .unwrap();

                            let worker = ctx
                                .client
                                .start_worker(bench_worker_options(1, Duration::from_secs(120)))
                                .await;

                            wait_for_tasks_complete(&ctx.pool, &ctx.queue_name, 1, 60).await;
                            worker.shutdown().await;

                            // Reset task to pending to force re-execution with cached checkpoints
                            let reset_task_query = AssertSqlSafe(format!(
                                "UPDATE durable.t_{} SET state = 'pending', attempts = 0, last_attempt_run = NULL WHERE task_id = $1",
                                &ctx.queue_name
                            ));
                            sqlx::query(reset_task_query)
                                .bind(spawn_result.task_id)
                                .execute(&ctx.pool)
                                .await
                                .unwrap();

                            // Create new run for the task
                            let create_run_query = AssertSqlSafe(format!(
                                "INSERT INTO durable.r_{} (run_id, task_id, attempt, state, available_at) VALUES (gen_random_uuid(), $1, 1, 'pending', NOW())",
                                &ctx.queue_name
                            ));
                            sqlx::query(create_run_query)
                                .bind(spawn_result.task_id)
                                .execute(&ctx.pool)
                                .await
                                .unwrap();

                            // Measure cache-hit replay time
                            let start = std::time::Instant::now();
                            let worker = ctx
                                .client
                                .start_worker(bench_worker_options(1, Duration::from_secs(120)))
                                .await;

                            wait_for_tasks_complete(&ctx.pool, &ctx.queue_name, 1, 60).await;
                            total_time += start.elapsed();

                            worker.shutdown().await;
                            ctx.cleanup().await;
                        }

                        total_time
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Large payload checkpoint persistence
fn bench_large_payload_checkpoint(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("large_payload_checkpoint");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    // Test with different payload sizes: 1KB, 100KB, 1MB
    for size_kb in [1u64, 100, 1000] {
        let payload_size = (size_kb * 1024) as usize;

        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_with_input(
            BenchmarkId::new("size_kb", size_kb),
            &payload_size,
            |b, &payload_size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total_time = Duration::ZERO;

                        for _ in 0..iters {
                            let ctx = BenchContext::new().await;
                            ctx.client
                                .register::<LargePayloadBenchTask>()
                                .await
                                .unwrap();

                            ctx.client
                                .spawn::<LargePayloadBenchTask>(LargePayloadParams { payload_size })
                                .await
                                .unwrap();

                            let start = std::time::Instant::now();
                            let worker = ctx
                                .client
                                .start_worker(bench_worker_options(1, Duration::from_secs(120)))
                                .await;

                            wait_for_tasks_complete(&ctx.pool, &ctx.queue_name, 1, 120).await;
                            total_time += start.elapsed();

                            worker.shutdown().await;
                            ctx.cleanup().await;
                        }

                        total_time
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_step_cache_miss,
    bench_step_cache_hit,
    bench_large_payload_checkpoint
);
criterion_main!(benches);
