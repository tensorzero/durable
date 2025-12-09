# durable

A Rust SDK for building durable, fault-tolerant workflows using PostgreSQL.
This project is derived from [absurd](https://github.com/earendil-works/absurd).
It is experimental software to be used in TensorZero.
Use at your own risk.

## Overview

`durable` enables you to write long-running tasks that can:

- **Checkpoint progress** - Steps are persisted, so tasks resume where they left off after crashes
- **Sleep and wait** - Suspend execution for durations or until specific times
- **Await events** - Pause until external events arrive (with optional timeouts)
- **Retry on failure** - Configurable retry strategies with exponential backoff
- **Scale horizontally** - Multiple workers can process tasks concurrently

Unlike exception-based durable execution systems (Python, TypeScript), this SDK uses Rust's `Result` type for suspension control flow, making it idiomatic and type-safe.

## Why Durable Execution?

Traditional background job systems execute tasks once and hope for the best. Durable execution is different - it provides **guaranteed progress** even when things go wrong:

- **Crash recovery** - If your process dies mid-workflow, tasks resume exactly where they left off. No lost progress, no duplicate work.
- **Long-running workflows** - Execute workflows that span hours or days. Sleep for a week waiting for a subscription to renew, then continue.
- **External event coordination** - Wait for webhooks, human approvals, or other services. The task suspends until the event arrives.
- **Reliable retries** - Transient failures (network issues, rate limits) are automatically retried with configurable backoff.
- **Exactly-once semantics** - Checkpointed steps don't re-execute on retry. Combined with idempotency keys, achieve exactly-once side effects.

Use durable execution when your workflow is too important to fail silently, too long to hold in memory, or too complex for simple retries.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
durable = "0.1"
```

## Quick Start

```rust
use durable::{Durable, Task, TaskContext, TaskResult, WorkerOptions, async_trait};
use serde::{Deserialize, Serialize};

// Define your task parameters and output
#[derive(Serialize, Deserialize)]
struct ResearchParams {
    query: String,
}

#[derive(Serialize, Deserialize)]
struct ResearchResult {
    summary: String,
    sources: Vec<String>,
}

// Implement the Task trait
struct ResearchTask;

#[async_trait]
impl Task for ResearchTask {
    const NAME: &'static str = "research";
    type Params = ResearchParams;
    type Output = ResearchResult;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Phase 1: Find relevant sources (checkpointed)
        // If the task crashes after this step, it won't re-run on retry
        let sources: Vec<String> = ctx.step("find-sources", || async {
            // Search logic here...
            Ok(vec![
                "https://example.com/article1".into(),
                "https://example.com/article2".into(),
            ])
        }).await?;

        // Phase 2: Analyze the sources (checkpointed)
        let analysis: String = ctx.step("analyze", || async {
            // Analysis logic here...
            Ok("Key findings from sources...".into())
        }).await?;

        // Phase 3: Generate summary (checkpointed)
        let summary: String = ctx.step("summarize", || async {
            // Summarization logic here...
            Ok(format!("Research summary for '{}': {}", params.query, analysis))
        }).await?;

        Ok(ResearchResult { summary, sources })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create the client
    let client = Durable::builder()
        .database_url("postgres://localhost/myapp")
        .queue_name("research")
        .build()
        .await?;

    // Register your task
    client.register::<ResearchTask>().await;

    // Spawn a task
    let result = client.spawn::<ResearchTask>(ResearchParams {
        query: "distributed systems consensus algorithms".into(),
    }).await?;

    println!("Spawned task: {}", result.task_id);

    // Start a worker to process tasks
    let worker = client.start_worker(WorkerOptions::default()).await;

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    worker.shutdown().await;

    Ok(())
}
```

## Core Concepts

### Tasks

Tasks are defined by implementing the [`Task`] trait:

```rust
#[async_trait]
impl Task for MyTask {
    const NAME: &'static str = "my-task";  // Unique identifier
    type Params = MyParams;                 // Input (JSON-serializable)
    type Output = MyOutput;                 // Output (JSON-serializable)

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Your task logic here
    }
}
```

### TaskContext

The [`TaskContext`] provides methods for durable execution:

- **`step(name, closure)`** - Execute a checkpointed operation. If the step completed in a previous run, returns the cached result.
- **`spawn::<T>(name, params, options)`** - Spawn a subtask and return a handle.
- **`join(name, handle)`** - Wait for a subtask to complete and get its result.
- **`sleep_for(name, duration)`** - Suspend the task for a duration.
- **`await_event(name, timeout)`** - Wait for an external event.
- **`emit_event(name, payload)`** - Emit an event to wake waiting tasks.
- **`heartbeat(duration)`** - Extend the task lease for long operations.

### Checkpointing

Steps provide "at-least-once" execution. To achieve "exactly-once" semantics for side effects, use the `task_id` as an idempotency key:

```rust
ctx.step("charge-payment", || async {
    let idempotency_key = format!("{}:charge", ctx.task_id);
    stripe::charge(amount, &idempotency_key).await
}).await?;
```

### Events

Tasks can wait for and emit events:

```rust
// In one task: wait for an event
let shipment: ShipmentEvent = ctx.await_event(
    &format!("packed:{}", order_id),
    Some(Duration::from_secs(7 * 24 * 3600)), // 7 day timeout
).await?;

// From another task or service: emit the event
client.emit_event(
    &format!("packed:{}", order_id),
    &ShipmentEvent { tracking: "1Z999".into() },
    None,
).await?;
```

### Subtasks (Spawn/Join)

Tasks can spawn subtasks and wait for their results using `spawn()` and `join()`:

```rust
async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
    // Spawn subtasks (runs on same queue)
    let h1 = ctx.spawn::<ProcessItem>("item-1", Item { id: 1 }, Default::default()).await?;
    let h2 = ctx.spawn::<ProcessItem>("item-2", Item { id: 2 }, SpawnOptions {
        max_attempts: Some(3),
        ..Default::default()
    }).await?;

    // Do local work while subtasks run...
    let local = ctx.step("local-work", || async { Ok(compute()) }).await?;

    // Wait for subtask results
    let r1: ItemResult = ctx.join("item-1", h1).await?;
    let r2: ItemResult = ctx.join("item-2", h2).await?;

    Ok(Output { local, children: vec![r1, r2] })
}
```

**Key behaviors:**

- **Checkpointed** - Spawns and joins are cached. If the parent retries, it gets the same subtask handles and results.
- **Cascade cancellation** - When a parent fails or is cancelled, all its subtasks are automatically cancelled.
- **Error propagation** - If a subtask fails, `join()` returns an error that the parent can handle.
- **Same queue** - Subtasks run on the same queue as their parent.

### Event-Based Coordination

For coordination between independent tasks (not parent-child), use events:

```rust
// Task A: Waits for a signal from Task B
let approval: ApprovalPayload = ctx.await_event(
    &format!("approved:{}", request_id),
    Some(Duration::from_secs(24 * 3600)), // 24 hour timeout
).await?;

// Task B (or external service): Sends the signal
client.emit_event(
    &format!("approved:{}", request_id),
    &ApprovalPayload { approved_by: "admin".into() },
    None,
).await?;
```

### Transactional Spawning

You can atomically enqueue a task as part of a larger database transaction. This ensures that either both your write and the task spawn succeed, or neither does:

```rust
let mut tx = client.pool().begin().await?;

// Your application write
sqlx::query("INSERT INTO orders (id, status) VALUES ($1, $2)")
    .bind(order_id)
    .bind("pending")
    .execute(&mut *tx)
    .await?;

// Enqueue task in the same transaction
client.spawn_with::<ProcessOrder, _>(&mut *tx, ProcessOrderParams { order_id }).await?;

tx.commit().await?;
// Both succeed or both fail - atomic
```

This is useful when you need to guarantee that a task is only enqueued if related data was successfully persisted. The `_with` variants accept any SQLx executor:

- `spawn_with(executor, params)` - Spawn with default options
- `spawn_with_options_with(executor, params, options)` - Spawn with custom options
- `spawn_by_name_with(executor, task_name, params, options)` - Dynamic spawn by name

## API Overview

### Client

| Type | Description |
|------|-------------|
| [`Durable`] | Main client for spawning tasks and managing queues |
| [`DurableBuilder`] | Builder for configuring the client |
| [`Worker`] | Background worker that processes tasks |

### Task Definition

| Type | Description |
|------|-------------|
| [`Task`] | Trait for defining task types |
| [`TaskContext`] | Context passed to task execution |
| [`TaskResult<T>`] | Result type alias for task returns |
| [`TaskError`] | Error type with control flow signals |
| [`TaskHandle<T>`] | Handle to a spawned subtask (returned by `ctx.spawn()`) |

### Configuration

| Type | Description |
|------|-------------|
| [`SpawnOptions`] | Options for spawning tasks (retries, headers, queue) |
| [`WorkerOptions`] | Options for worker configuration (concurrency, timeouts) |
| [`RetryStrategy`] | Retry behavior: `None`, `Fixed`, or `Exponential` |
| [`CancellationPolicy`] | Auto-cancel tasks based on delay or duration |

### Results

| Type | Description |
|------|-------------|
| [`SpawnResult`] | Returned when spawning a task (task_id, run_id, attempt) |
| [`ControlFlow`] | Signals for suspension and cancellation |

## Environment Variables

- `DURABLE_DATABASE_URL` - Default PostgreSQL connection string (if not provided to builder)

## License

See LICENSE file.
