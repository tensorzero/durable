#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

mod common;
use common::setup::{BenchContext, bench_worker_options, wait_for_tasks_complete};
use common::tasks::{QuickParams, QuickTask};

/// Benchmark: Multiple workers competing for claims
fn bench_concurrent_claims(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_claims");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    let num_tasks: u32 = 200;

    // Test with different worker counts
    for num_workers in [2usize, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("workers", num_workers),
            &num_workers,
            |b, &num_workers| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total_time = Duration::ZERO;

                        for _ in 0..iters {
                            let ctx = BenchContext::new().await;
                            ctx.client.register::<QuickTask>().await.unwrap();

                            // Spawn all tasks
                            for i in 0..num_tasks {
                                ctx.client
                                    .spawn::<QuickTask>(QuickParams { task_num: i })
                                    .await
                                    .unwrap();
                            }

                            // Create barrier for synchronized start
                            let barrier = Arc::new(Barrier::new(num_workers));
                            let mut handles = Vec::new();

                            let start = std::time::Instant::now();

                            // Spawn multiple worker processes
                            for _ in 0..num_workers {
                                let client = ctx.new_client().await;
                                client.register::<QuickTask>().await.unwrap();
                                let barrier = barrier.clone();

                                let handle = tokio::spawn(async move {
                                    // Sync all workers to start together
                                    barrier.wait().await;

                                    let worker = client
                                        .start_worker(bench_worker_options(
                                            1,
                                            Duration::from_secs(60),
                                        ))
                                        .await;

                                    // Wait a bit then shutdown
                                    tokio::time::sleep(Duration::from_secs(15)).await;
                                    worker.shutdown().await;
                                });

                                handles.push(handle);
                            }

                            // Wait for all tasks to complete
                            wait_for_tasks_complete(
                                &ctx.pool,
                                &ctx.queue_name,
                                num_tasks as usize,
                                30,
                            )
                            .await;

                            total_time += start.elapsed();

                            // Shutdown all workers
                            for handle in handles {
                                let _ = handle.await;
                            }

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

/// Benchmark: Claim latency distribution under contention
fn bench_claim_latency_distribution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("claim_latency");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);

    // Single worker baseline vs high contention
    for (scenario, num_workers) in [("baseline", 1usize), ("contention", 8)] {
        let num_tasks: u32 = 50;

        group.bench_with_input(
            BenchmarkId::new("scenario", scenario),
            &num_workers,
            |b, &num_workers| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total_time = Duration::ZERO;

                        for _ in 0..iters {
                            let ctx = BenchContext::new().await;
                            ctx.client.register::<QuickTask>().await.unwrap();

                            // Spawn tasks
                            for i in 0..num_tasks {
                                ctx.client
                                    .spawn::<QuickTask>(QuickParams { task_num: i })
                                    .await
                                    .unwrap();
                            }

                            let barrier = Arc::new(Barrier::new(num_workers));
                            let mut handles = Vec::new();

                            let start = std::time::Instant::now();

                            for _ in 0..num_workers {
                                let client = ctx.new_client().await;
                                client.register::<QuickTask>().await.unwrap();
                                let barrier = barrier.clone();

                                let handle = tokio::spawn(async move {
                                    barrier.wait().await;

                                    let worker = client
                                        .start_worker(bench_worker_options(
                                            4,
                                            Duration::from_secs(60),
                                        ))
                                        .await;

                                    tokio::time::sleep(Duration::from_secs(20)).await;
                                    worker.shutdown().await;
                                });

                                handles.push(handle);
                            }

                            wait_for_tasks_complete(
                                &ctx.pool,
                                &ctx.queue_name,
                                num_tasks as usize,
                                30,
                            )
                            .await;

                            total_time += start.elapsed();

                            for handle in handles {
                                let _ = handle.await;
                            }

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
    bench_concurrent_claims,
    bench_claim_latency_distribution
);
criterion_main!(benches);
