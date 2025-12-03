# durable

A Rust SDK for building durable, fault-tolerant workflows using PostgreSQL.

## Overview

`durable` enables you to write long-running tasks that can:

- **Checkpoint progress** - Steps are persisted, so tasks resume where they left off after crashes
- **Sleep and wait** - Suspend execution for durations or until specific times
- **Await events** - Pause until external events arrive (with optional timeouts)
- **Retry on failure** - Configurable retry strategies with exponential backoff
- **Scale horizontally** - Multiple workers can process tasks concurrently

Unlike exception-based durable execution systems (Python, TypeScript), this SDK uses Rust's `Result` type for suspension control flow, making it idiomatic and type-safe.

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
struct SendEmailParams {
    to: String,
    subject: String,
    body: String,
}

#[derive(Serialize, Deserialize)]
struct SendEmailResult {
    message_id: String,
}

// Implement the Task trait
struct SendEmailTask;

#[async_trait]
impl Task for SendEmailTask {
    const NAME: &'static str = "send-email";
    type Params = SendEmailParams;
    type Output = SendEmailResult;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // This step is checkpointed - if the task crashes after sending,
        // it won't send again on retry
        let message_id = ctx.step("send", || async {
            // Your email sending logic here
            Ok("msg-123".to_string())
        }).await?;

        Ok(SendEmailResult { message_id })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create the client
    let client = Durable::builder()
        .database_url("postgres://localhost/myapp")
        .queue_name("emails")
        .build()
        .await?;

    // Register your task
    client.register::<SendEmailTask>().await;

    // Spawn a task
    let result = client.spawn::<SendEmailTask>(SendEmailParams {
        to: "user@example.com".into(),
        subject: "Hello".into(),
        body: "World".into(),
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
- **`sleep_for(name, duration)`** - Suspend the task for a duration.
- **`sleep_until(name, datetime)`** - Suspend until a specific time.
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
