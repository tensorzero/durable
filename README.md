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
- **Cron scheduling** - Run tasks on recurring schedules via pg_cron integration

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
use durable::{Durable, MIGRATOR, Task, TaskContext, TaskResult, WorkerOptions, async_trait};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

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
#[derive(Default)]
struct ResearchTask;

#[async_trait]
impl Task<()> for ResearchTask {
    fn name(&self) -> Cow<'static, str> { Cow::Borrowed("research") }
    type Params = ResearchParams;
    type Output = ResearchResult;

    async fn run(&self, params: Self::Params, mut ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
        // Phase 1: Find relevant sources (checkpointed)
        // If the task crashes after this step, it won't re-run on retry
        let sources: Vec<String> = ctx.step("find-sources", (), |_, _| async {
            // Search logic here...
            Ok(vec![
                "https://example.com/article1".into(),
                "https://example.com/article2".into(),
            ])
        }).await?;

        // Phase 2: Analyze the sources (checkpointed)
        let analysis: String = ctx.step("analyze", (), |_, _| async {
            // Analysis logic here...
            Ok("Key findings from sources...".into())
        }).await?;

        // Phase 3: Generate summary (checkpointed)
        // Note: step takes a fn pointer, so captured variables must be passed via params
        let summary: String = ctx.step(
            "summarize",
            (params, analysis),
            |(params, analysis), _| async move {
                Ok(format!("Research summary for '{}': {}", params.query, analysis))
            },
        ).await?;

        Ok(ResearchResult { summary, sources })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create the client (register tasks on the builder)
    let client = Durable::builder()
        .database_url("postgres://localhost/myapp")
        .queue_name("research")
        .register::<ResearchTask>()?
        .build()
        .await?;

    // Run migrations (idempotent - safe to call on every startup)
    MIGRATOR.run(client.pool()).await?;

    // Create the queue (idempotent - safe to call on every startup)
    client.create_queue(None).await?;

    // Spawn a task
    let result = client.spawn::<ResearchTask>(ResearchParams {
        query: "distributed systems consensus algorithms".into(),
    }).await?;

    println!("Spawned task: {}", result.task_id);

    // Start a worker to process tasks
    let worker = client.start_worker(WorkerOptions::default()).await?;

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    worker.shutdown().await;

    Ok(())
}
```

## Core Concepts

### Tasks

Tasks are defined by implementing the [`Task`] trait. The `State` type parameter allows passing application state (e.g., HTTP clients, database pools) to your tasks. Use `()` if you don't need state.

```rust
#[derive(Default)]
struct MyTask;

#[async_trait]
impl Task<()> for MyTask {
    fn name(&self) -> Cow<'static, str> { Cow::Borrowed("my-task") }  // Unique identifier
    type Params = MyParams;                                            // Input (JSON-serializable)
    type Output = MyOutput;                                            // Output (JSON-serializable)

    async fn run(&self, params: Self::Params, mut ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
        // Your task logic here
    }
}
```

### Application State

Tasks can receive shared application state (like HTTP clients or database pools) via the generic `State` parameter:

```rust
#[derive(Clone)]
struct AppState {
    http_client: reqwest::Client,
}

#[derive(Default)]
struct FetchTask;

#[async_trait]
impl Task<AppState> for FetchTask {
    fn name(&self) -> Cow<'static, str> { Cow::Borrowed("fetch") }
    type Params = String;
    type Output = String;

    async fn run(&self, url: Self::Params, mut ctx: TaskContext, state: AppState) -> TaskResult<Self::Output> {
        ctx.step("fetch", url, |url, _| async move {
            state.http_client.get(&url).send().await
                .map_err(|e| anyhow::anyhow!("HTTP error: {}", e))?
                .text().await
                .map_err(|e| anyhow::anyhow!("HTTP error: {}", e))
        }).await
    }
}

// Build client with state
let app_state = AppState { http_client: reqwest::Client::new() };
let client = Durable::builder()
    .database_url("postgres://localhost/myapp")
    .register::<FetchTask>()?
    .build_with_state(app_state)
    .await?;
```

### User Errors

Return user errors with structured data using `TaskError::user()`:

```rust
// With structured data (message extracted from "message" field if present)
Err(TaskError::user(json!({"message": "Not found", "code": 404})))

// With any serializable type
Err(TaskError::user(MyError { code: 404, details: "..." }))

// Simple string message
Err(TaskError::user_message("Something went wrong"))
```

The error data is serialized to JSON and stored in the database for debugging and analysis.

### TaskContext

The [`TaskContext`] provides methods for durable execution:

- **`step(name, params, f)`** - Execute a checkpointed operation. The `f` is a `fn(P, StepState<State>) -> Future` (function pointer, not a closure that captures). If the step completed in a previous run with the same name and params hash, returns the cached result.
- **`spawn::<T>(name, params, options)`** - Spawn a subtask and return a handle.
- **`spawn_by_name(name, task_name, params, options)`** - Spawn a subtask by task name (dynamic version).
- **`join(handle)`** - Wait for a subtask to complete and get its result.
- **`sleep_for(name, duration)`** - Suspend the task for a duration.
- **`await_event(name, timeout)`** - Wait for an external event.
- **`emit_event(name, payload)`** - Emit an event to wake waiting tasks.
- **`heartbeat(duration)`** - Extend the task lease for long operations. Takes `Option<Duration>`.
- **`heartbeat_handle()`** - Get a cloneable `HeartbeatHandle` for use inside step closures.
- **`rand()`** - Generate a durable random value in [0, 1). Checkpointed.
- **`now()`** - Get the current time as a durable checkpoint.
- **`uuid7()`** - Generate a durable UUIDv7. Checkpointed.

### Checkpointing

Steps provide "at-least-once" execution. To achieve "exactly-once" semantics for side effects, use the `task_id` as an idempotency key:

```rust
ctx.step("charge-payment", ctx.task_id, |task_id, _step_state| async move {
    let idempotency_key = format!("{}:charge", task_id);
    stripe::charge(amount, &idempotency_key).await
}).await?;
```

The step closure receives `(params, StepState<State>)`. Since `step` takes a function pointer (`fn`), the closure cannot capture variables from the surrounding scope — pass any needed data through the `params` argument.

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
async fn run(&self, params: Self::Params, mut ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
    // Spawn subtasks (runs on same queue)
    let h1 = ctx.spawn::<ProcessItem>("item-1", Item { id: 1 }, Default::default()).await?;
    let h2 = ctx.spawn::<ProcessItem>("item-2", Item { id: 2 }, SpawnOptions {
        max_attempts: Some(3),
        ..Default::default()
    }).await?;

    // Do local work while subtasks run...
    let local = ctx.step("local-work", (), |_, _| async { Ok(compute()) }).await?;

    // Wait for subtask results
    let r1: ItemResult = ctx.join(h1).await?;
    let r2: ItemResult = ctx.join(h2).await?;

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

### Cron Scheduling

Schedule tasks to run on a recurring basis using [pg_cron](https://github.com/citusdata/pg_cron). Durable manages the pg_cron jobs and maintains a registry for discovery and filtering.

**Setup** - Enable pg_cron once at startup:

```rust
use durable::setup_pgcron;

// Enable the pg_cron extension (requires superuser or appropriate privileges)
setup_pgcron(client.pool()).await?;
```

**Create a schedule:**

```rust
use durable::ScheduleOptions;

let options = ScheduleOptions {
    task_name: "process-payments".to_string(),
    cron_expression: "*/5 * * * *".to_string(), // Every 5 minutes
    params: json!({"batch_size": 100}),
    spawn_options: SpawnOptions::default(),
    metadata: Some(HashMap::from([
        ("team".to_string(), json!("payments")),
        ("env".to_string(), json!("production")),
    ])),
};

// Creates or updates the schedule (upsert semantics)
client.create_schedule("payment-schedule", options).await?;
```

**List and filter schedules:**

```rust
use durable::ScheduleFilter;

// List all schedules on this queue
let all = client.list_schedules(None).await?;

// Filter by task name
let filter = ScheduleFilter {
    task_name: Some("process-payments".to_string()),
    ..Default::default()
};
let filtered = client.list_schedules(Some(filter)).await?;

// Filter by metadata (JSONB containment)
let filter = ScheduleFilter {
    metadata: Some(HashMap::from([("team".to_string(), json!("payments"))])),
    ..Default::default()
};
let filtered = client.list_schedules(Some(filter)).await?;
```

**Delete a schedule:**

```rust
// Removes the schedule and its pg_cron job. In-flight tasks are not cancelled.
client.delete_schedule("payment-schedule").await?;
```

**Key behaviors:**

- **pg_cron integration** - Schedules are backed by PostgreSQL's pg_cron extension. At each tick, pg_cron inserts a task into the queue via `durable.spawn_task()`, and workers pick it up normally.
- **Upsert semantics** - Calling `create_schedule` with an existing name updates the schedule in place.
- **Origin tracking** - Scheduled tasks automatically receive `durable::scheduled_by` and `durable::cron` headers, so tasks can identify how they were spawned.
- **Metadata filtering** - Attach arbitrary JSON metadata to schedules and filter with JSONB containment queries.
- **Queue cleanup** - Dropping a queue automatically unschedules all its cron jobs.

### Polling Task Results

You can poll for the result of a spawned task without running a worker in the same process:

```rust
use durable::TaskStatus;

let spawned = client.spawn::<MyTask>(params).await?;

// Poll for the result
let result = client.get_task_result(spawned.task_id).await?;

if let Some(poll) = result {
    match poll.status {
        TaskStatus::Completed => println!("Output: {:?}", poll.output),
        TaskStatus::Failed => println!("Error: {:?}", poll.error),
        TaskStatus::Pending | TaskStatus::Running | TaskStatus::Sleeping => {
            println!("Still in progress...")
        }
        TaskStatus::Cancelled => println!("Task was cancelled"),
    }
}
```

### Task Cancellation

Cancel a running or pending task:

```rust
client.cancel_task(task_id, None).await?;
```

When a task is cancelled, all its subtasks are automatically cancelled as well.

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
- `emit_event_with(executor, event_name, payload, queue_name)` - Emit event transactionally

## API Overview

### Client

| Type | Description |
|------|-------------|
| [`Durable`] | Main client for spawning tasks and managing queues |
| [`DurableBuilder`] | Builder for configuring the client (register tasks, set defaults) |
| [`Worker`] | Background worker that processes tasks |

### Task Definition

| Type | Description |
|------|-------------|
| [`Task<State>`] | Trait for defining task types (generic over application state) |
| [`TaskContext`] | Context passed to task execution |
| [`TaskResult<T>`] | Result type alias for task returns |
| [`TaskError`] | Error type with control flow signals and user errors |
| [`TaskError::user()`] | Helper to create user errors with JSON data |
| [`TaskError::user_message()`] | Helper to create string user errors |
| [`TaskHandle<T>`] | Handle to a spawned subtask (returned by `ctx.spawn()`) |

### Configuration

| Type | Description |
|------|-------------|
| [`SpawnOptions`] | Options for spawning tasks (retries, headers, cancellation) |
| [`WorkerOptions`] | Options for worker configuration (concurrency, timeouts, poll interval) |
| [`RetryStrategy`] | Retry behavior: `None`, `Fixed`, or `Exponential` |
| [`CancellationPolicy`] | Auto-cancel tasks based on pending or running time |

### Cron Scheduling

| Type | Description |
|------|-------------|
| [`ScheduleOptions`] | Options for creating a cron schedule (task, expression, params, metadata) |
| [`ScheduleInfo`] | Information about an existing schedule |
| [`ScheduleFilter`] | Filter for listing schedules (by task name or metadata) |
| [`setup_pgcron()`] | Initialize the pg_cron extension |

### Results & Errors

| Type | Description |
|------|-------------|
| [`SpawnResult`] | Returned when spawning a task (task_id, run_id, attempt) |
| [`TaskPollResult`] | Result of polling a task (status, output, error) |
| [`TaskStatus`] | Task state: `Pending`, `Running`, `Sleeping`, `Completed`, `Failed`, `Cancelled` |
| [`ControlFlow`] | Signals for suspension and cancellation |
| [`DurableError`] | Error type for client operations |
| [`DurableResult<T>`] | Result type alias for client operations |

### Heartbeat & Step State

| Type | Description |
|------|-------------|
| [`HeartbeatHandle`] | Cloneable handle for extending task leases inside step closures |
| [`StepState<State>`] | State passed to step closures (provides access to app state and heartbeat) |

## Queue Management

```rust
// Create a queue (idempotent)
client.create_queue(None).await?;

// List all queues
let queues = client.list_queues().await?;

// Count unclaimed ready tasks (useful for autoscaling)
let count = client.count_unclaimed_ready_tasks(None).await?;

// Drop a queue (also unschedules its cron jobs)
client.drop_queue(None).await?;
```

## Environment Variables

- `DURABLE_DATABASE_URL` - Default PostgreSQL connection string (if not provided to builder)

## Benchmarks

Performance benchmarks run automatically on every push to `main` using [Criterion](https://github.com/bheisler/criterion.rs). Results are published to GitHub Pages:

**[View Benchmark Results](https://tensorzero.github.io/durable/dev/bench/)**

To run benchmarks locally:

```bash
cargo bench
```

## License

See LICENSE file.
