#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;

mod common;
use common::setup::{BenchContext, bench_worker_options, wait_for_tasks_complete};
use common::tasks::{NoOpTask, QuickParams, QuickTask};

/// Benchmark: Spawn latency (how long to enqueue a single task)
fn bench_spawn_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("spawn_latency");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("single_spawn", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = BenchContext::new().await;
                ctx.client.register::<NoOpTask>().await;

                let start = std::time::Instant::now();
                for _ in 0..iters {
                    ctx.client.spawn::<NoOpTask>(()).await.unwrap();
                }
                let elapsed = start.elapsed();

                ctx.cleanup().await;
                elapsed
            })
        });
    });

    group.finish();
}

/// Benchmark: Task throughput with varying worker counts
fn bench_task_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("task_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    // Test with different worker concurrency levels
    for concurrency in [1, 2, 4, 8] {
        let num_tasks: u32 = 100;

        group.throughput(Throughput::Elements(num_tasks as u64));
        group.bench_with_input(
            BenchmarkId::new("workers", concurrency),
            &(num_tasks, concurrency),
            |b, &(num_tasks, concurrency)| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total_time = Duration::ZERO;

                        for _ in 0..iters {
                            let ctx = BenchContext::new().await;
                            ctx.client.register::<QuickTask>().await;

                            // Spawn all tasks first
                            for i in 0..num_tasks {
                                ctx.client
                                    .spawn::<QuickTask>(QuickParams { task_num: i })
                                    .await
                                    .unwrap();
                            }

                            // Start worker and measure completion time
                            let start = std::time::Instant::now();
                            let worker = ctx
                                .client
                                .start_worker(bench_worker_options(concurrency, 60))
                                .await;

                            wait_for_tasks_complete(
                                &ctx.pool,
                                &ctx.queue_name,
                                num_tasks as usize,
                                60,
                            )
                            .await;

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

/// Benchmark: End-to-end task completion time (spawn to completed)
fn bench_e2e_completion(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("e2e_completion");
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("single_task_roundtrip", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = BenchContext::new().await;
                ctx.client.register::<NoOpTask>().await;

                let worker = ctx.client.start_worker(bench_worker_options(1, 60)).await;

                let start = std::time::Instant::now();
                for i in 0..iters {
                    ctx.client.spawn::<NoOpTask>(()).await.unwrap();
                    wait_for_tasks_complete(&ctx.pool, &ctx.queue_name, (i + 1) as usize, 30).await;
                }
                let elapsed = start.elapsed();

                worker.shutdown().await;
                ctx.cleanup().await;
                elapsed
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_spawn_latency,
    bench_task_throughput,
    bench_e2e_completion
);
criterion_main!(benches);
