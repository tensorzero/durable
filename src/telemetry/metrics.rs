//! Metrics definitions and recording helpers for the durable execution system.
//!
//! All metrics are prefixed with `durable_` and use Prometheus naming conventions.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

// Metric name constants
pub const TASKS_SPAWNED_TOTAL: &str = "durable_tasks_spawned_total";
pub const TASKS_CLAIMED_TOTAL: &str = "durable_tasks_claimed_total";
pub const TASKS_COMPLETED_TOTAL: &str = "durable_tasks_completed_total";
pub const TASKS_FAILED_TOTAL: &str = "durable_tasks_failed_total";
pub const EVENTS_EMITTED_TOTAL: &str = "durable_events_emitted_total";

pub const WORKER_CONCURRENT_TASKS: &str = "durable_worker_concurrent_tasks";
pub const WORKER_ACTIVE: &str = "durable_worker_active";

pub const TASK_EXECUTION_DURATION: &str = "durable_task_execution_duration_seconds";
pub const TASK_CLAIM_DURATION: &str = "durable_task_claim_duration_seconds";
pub const CHECKPOINT_DURATION: &str = "durable_checkpoint_duration_seconds";

/// Register all metric descriptions. Called once during telemetry initialization.
pub fn register_metrics() {
    // Counters
    describe_counter!(TASKS_SPAWNED_TOTAL, "Total number of tasks spawned");
    describe_counter!(
        TASKS_CLAIMED_TOTAL,
        "Total number of tasks claimed by workers"
    );
    describe_counter!(
        TASKS_COMPLETED_TOTAL,
        "Total number of tasks that completed successfully"
    );
    describe_counter!(TASKS_FAILED_TOTAL, "Total number of tasks that failed");
    describe_counter!(EVENTS_EMITTED_TOTAL, "Total number of events emitted");

    // Gauges
    describe_gauge!(
        WORKER_CONCURRENT_TASKS,
        "Number of tasks currently being executed by this worker"
    );
    describe_gauge!(
        WORKER_ACTIVE,
        "Whether the worker is active (1) or shut down (0)"
    );

    // Histograms
    describe_histogram!(
        TASK_EXECUTION_DURATION,
        "Duration of task execution in seconds"
    );
    describe_histogram!(
        TASK_CLAIM_DURATION,
        "Duration of task claim operation in seconds"
    );
    describe_histogram!(
        CHECKPOINT_DURATION,
        "Duration of checkpoint persistence in seconds"
    );
}

// Helper functions for recording metrics

/// Record a task spawn event
pub fn record_task_spawned(queue: &str, task_name: &str) {
    counter!(TASKS_SPAWNED_TOTAL, "queue" => queue.to_string(), "task_name" => task_name.to_string())
        .increment(1);
}

/// Record a task claim event
pub fn record_task_claimed(queue: &str) {
    counter!(TASKS_CLAIMED_TOTAL, "queue" => queue.to_string()).increment(1);
}

/// Record a successful task completion
pub fn record_task_completed(queue: &str, task_name: &str) {
    counter!(TASKS_COMPLETED_TOTAL, "queue" => queue.to_string(), "task_name" => task_name.to_string())
        .increment(1);
}

/// Record a task failure
pub fn record_task_failed(queue: &str, task_name: &str, error_type: &str) {
    counter!(TASKS_FAILED_TOTAL, "queue" => queue.to_string(), "task_name" => task_name.to_string(), "error_type" => error_type.to_string())
        .increment(1);
}

/// Record an event emission
pub fn record_event_emitted(queue: &str, event_name: &str) {
    counter!(EVENTS_EMITTED_TOTAL, "queue" => queue.to_string(), "event_name" => event_name.to_string())
        .increment(1);
}

/// Set the current number of concurrent tasks for a worker
pub fn set_worker_concurrent_tasks(queue: &str, worker_id: &str, count: usize) {
    gauge!(WORKER_CONCURRENT_TASKS, "queue" => queue.to_string(), "worker_id" => worker_id.to_string())
        .set(count as f64);
}

/// Set whether a worker is active
pub fn set_worker_active(queue: &str, worker_id: &str, active: bool) {
    gauge!(WORKER_ACTIVE, "queue" => queue.to_string(), "worker_id" => worker_id.to_string())
        .set(if active { 1.0 } else { 0.0 });
}

/// Record task execution duration
pub fn record_task_execution_duration(
    queue: &str,
    task_name: &str,
    outcome: &str,
    duration_secs: f64,
) {
    histogram!(TASK_EXECUTION_DURATION, "queue" => queue.to_string(), "task_name" => task_name.to_string(), "outcome" => outcome.to_string())
        .record(duration_secs);
}

/// Record task claim duration
pub fn record_task_claim_duration(queue: &str, duration_secs: f64) {
    histogram!(TASK_CLAIM_DURATION, "queue" => queue.to_string()).record(duration_secs);
}

/// Record checkpoint persistence duration
pub fn record_checkpoint_duration(queue: &str, task_name: &str, duration_secs: f64) {
    histogram!(CHECKPOINT_DURATION, "queue" => queue.to_string(), "task_name" => task_name.to_string())
        .record(duration_secs);
}
