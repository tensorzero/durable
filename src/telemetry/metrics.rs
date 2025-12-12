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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
    use ordered_float::OrderedFloat;

    fn find_counter(snapshot: Snapshot, name: &str) -> Option<(CompositeKey, u64)> {
        snapshot
            .into_vec()
            .into_iter()
            .find(|(key, _, _, _)| key.key().name() == name)
            .map(|(key, _, _, value)| {
                let count = match value {
                    DebugValue::Counter(c) => c,
                    _ => panic!("Expected counter"),
                };
                (key, count)
            })
    }

    fn find_gauge(snapshot: Snapshot, name: &str) -> Option<(CompositeKey, f64)> {
        snapshot
            .into_vec()
            .into_iter()
            .find(|(key, _, _, _)| key.key().name() == name)
            .map(|(key, _, _, value)| {
                let gauge_value = match value {
                    DebugValue::Gauge(g) => g.0,
                    _ => panic!("Expected gauge"),
                };
                (key, gauge_value)
            })
    }

    fn find_histogram(
        snapshot: Snapshot,
        name: &str,
    ) -> Option<(CompositeKey, Vec<OrderedFloat<f64>>)> {
        snapshot
            .into_vec()
            .into_iter()
            .find(|(key, _, _, _)| key.key().name() == name)
            .map(|(key, _, _, value)| {
                let values = match value {
                    DebugValue::Histogram(h) => h,
                    _ => panic!("Expected histogram"),
                };
                (key, values)
            })
    }

    fn get_label<'a>(key: &'a CompositeKey, label_name: &str) -> Option<&'a str> {
        key.key()
            .labels()
            .find(|l| l.key() == label_name)
            .map(|l| l.value())
    }

    #[test]
    fn test_record_task_spawned() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_task_spawned("test_queue", "MyTask");
        });

        let snapshot = snapshotter.snapshot();
        let (key, count) = find_counter(snapshot, TASKS_SPAWNED_TOTAL).unwrap();
        assert_eq!(count, 1);
        assert_eq!(get_label(&key, "queue"), Some("test_queue"));
        assert_eq!(get_label(&key, "task_name"), Some("MyTask"));
    }

    #[test]
    fn test_record_task_claimed() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_task_claimed("claim_queue");
        });

        let snapshot = snapshotter.snapshot();
        let (key, count) = find_counter(snapshot, TASKS_CLAIMED_TOTAL).unwrap();
        assert_eq!(count, 1);
        assert_eq!(get_label(&key, "queue"), Some("claim_queue"));
    }

    #[test]
    fn test_record_task_completed() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_task_completed("complete_queue", "CompletedTask");
        });

        let snapshot = snapshotter.snapshot();
        let (key, count) = find_counter(snapshot, TASKS_COMPLETED_TOTAL).unwrap();
        assert_eq!(count, 1);
        assert_eq!(get_label(&key, "queue"), Some("complete_queue"));
        assert_eq!(get_label(&key, "task_name"), Some("CompletedTask"));
    }

    #[test]
    fn test_record_task_failed() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_task_failed("fail_queue", "FailedTask", "timeout");
        });

        let snapshot = snapshotter.snapshot();
        let (key, count) = find_counter(snapshot, TASKS_FAILED_TOTAL).unwrap();
        assert_eq!(count, 1);
        assert_eq!(get_label(&key, "queue"), Some("fail_queue"));
        assert_eq!(get_label(&key, "task_name"), Some("FailedTask"));
        assert_eq!(get_label(&key, "error_type"), Some("timeout"));
    }

    #[test]
    fn test_record_event_emitted() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_event_emitted("event_queue", "user_created");
        });

        let snapshot = snapshotter.snapshot();
        let (key, count) = find_counter(snapshot, EVENTS_EMITTED_TOTAL).unwrap();
        assert_eq!(count, 1);
        assert_eq!(get_label(&key, "queue"), Some("event_queue"));
        assert_eq!(get_label(&key, "event_name"), Some("user_created"));
    }

    #[test]
    fn test_set_worker_concurrent_tasks() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            set_worker_concurrent_tasks("worker_queue", "worker-1", 5);
        });

        let snapshot = snapshotter.snapshot();
        let (key, value) = find_gauge(snapshot, WORKER_CONCURRENT_TASKS).unwrap();
        assert!((value - 5.0).abs() < f64::EPSILON);
        assert_eq!(get_label(&key, "queue"), Some("worker_queue"));
        assert_eq!(get_label(&key, "worker_id"), Some("worker-1"));
    }

    #[test]
    fn test_set_worker_active() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            set_worker_active("active_queue", "worker-2", true);
        });

        let snapshot = snapshotter.snapshot();
        let (key, value) = find_gauge(snapshot, WORKER_ACTIVE).unwrap();
        assert!((value - 1.0).abs() < f64::EPSILON);
        assert_eq!(get_label(&key, "queue"), Some("active_queue"));
        assert_eq!(get_label(&key, "worker_id"), Some("worker-2"));

        with_local_recorder(&recorder, || {
            set_worker_active("active_queue", "worker-2", false);
        });

        let snapshot = snapshotter.snapshot();
        let (_, value) = find_gauge(snapshot, WORKER_ACTIVE).unwrap();
        assert!(value.abs() < f64::EPSILON);
    }

    #[test]
    fn test_record_task_execution_duration() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_task_execution_duration("exec_queue", "ExecTask", "completed", 1.5);
        });

        let snapshot = snapshotter.snapshot();
        let (key, values) = find_histogram(snapshot, TASK_EXECUTION_DURATION).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], OrderedFloat(1.5));
        assert_eq!(get_label(&key, "queue"), Some("exec_queue"));
        assert_eq!(get_label(&key, "task_name"), Some("ExecTask"));
        assert_eq!(get_label(&key, "outcome"), Some("completed"));
    }

    #[test]
    fn test_record_task_claim_duration() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_task_claim_duration("claim_dur_queue", 0.25);
        });

        let snapshot = snapshotter.snapshot();
        let (key, values) = find_histogram(snapshot, TASK_CLAIM_DURATION).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], OrderedFloat(0.25));
        assert_eq!(get_label(&key, "queue"), Some("claim_dur_queue"));
    }

    #[test]
    fn test_record_checkpoint_duration() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            record_checkpoint_duration("ckpt_queue", "CkptTask", 0.1);
        });

        let snapshot = snapshotter.snapshot();
        let (key, values) = find_histogram(snapshot, CHECKPOINT_DURATION).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], OrderedFloat(0.1));
        assert_eq!(get_label(&key, "queue"), Some("ckpt_queue"));
        assert_eq!(get_label(&key, "task_name"), Some("CkptTask"));
    }
}
