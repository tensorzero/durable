# Benchmarks

Performance benchmarks for the durable crate using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Prerequisites

Start PostgreSQL:

```bash
docker compose up -d
```

## Running Benchmarks

```bash
# Run all benchmarks
DATABASE_URL="postgres://postgres:postgres@localhost:5436/test" cargo bench

# Run specific benchmark suite
cargo bench --bench throughput
cargo bench --bench checkpoint
cargo bench --bench concurrency
```

## Comparing Performance

```bash
# Save a baseline
cargo bench -- --save-baseline main

# Make changes, then compare
cargo bench -- --baseline main
```

## Benchmark Suites

### throughput.rs

Measures task processing performance.

| Benchmark | Description |
|-----------|-------------|
| `spawn_latency/single_spawn` | Time to enqueue a single task |
| `task_throughput/workers/{1,2,4,8}` | Tasks/second with varying worker concurrency |
| `e2e_completion/single_task_roundtrip` | Full spawn-to-completion latency |

### checkpoint.rs

Measures checkpoint (step) overhead.

| Benchmark | Description |
|-----------|-------------|
| `step_cache_miss/steps/{10,50,100}` | First execution with N checkpoint steps |
| `step_cache_hit/steps/{10,50,100}` | Replay execution (checkpoints already cached) |
| `large_payload_checkpoint/size_kb/{1,100,1000}` | Checkpoint persistence for 1KB/100KB/1MB payloads |

### concurrency.rs

Measures multi-worker contention behavior.

| Benchmark | Description |
|-----------|-------------|
| `concurrent_claims/workers/{2,4,8,16}` | Task completion time with N competing workers |
| `claim_latency/scenario/{baseline,contention}` | Single worker vs 8 workers claiming from same queue |

## Output

Criterion generates HTML reports in `target/criterion/`. Open `target/criterion/report/index.html` to view results with graphs and statistical analysis.
