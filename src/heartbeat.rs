use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{TaskError, TaskResult};
use crate::worker::LeaseExtender;

/// Trait for extending task leases during long-running operations.
///
/// Implementations allow code running inside a `step()` closure to send heartbeats
/// back to the durable framework, preventing the worker from considering the task
/// dead during long-running operations.
///
/// Two implementations are provided:
/// - [`HeartbeatHandle`] — extends leases via the database (used in durable workers)
/// - [`NoopHeartbeater`] — does nothing (used in tests and non-durable contexts)
#[async_trait]
pub trait Heartbeater: Send + Sync {
    /// Extend the task's lease.
    ///
    /// # Arguments
    /// * `duration` - Extension duration. If `None`, uses the original claim timeout.
    ///   Must be at least 1 second when `Some`.
    async fn heartbeat(&self, duration: Option<Duration>) -> TaskResult<()>;
}

/// Real heartbeat handle that extends leases via the database.
///
/// Created from a [`TaskContext`](crate::TaskContext) via
/// [`heartbeat_handle()`](crate::TaskContext::heartbeat_handle) and can be
/// passed into step closures or other contexts that need to extend the task lease.
#[derive(Clone)]
pub struct HeartbeatHandle {
    pool: sqlx::PgPool,
    queue_name: String,
    run_id: Uuid,
    claim_timeout: Duration,
    lease_extender: LeaseExtender,
}

impl HeartbeatHandle {
    pub(crate) fn new(
        pool: sqlx::PgPool,
        queue_name: String,
        run_id: Uuid,
        claim_timeout: Duration,
        lease_extender: LeaseExtender,
    ) -> Self {
        Self {
            pool,
            queue_name,
            run_id,
            claim_timeout,
            lease_extender,
        }
    }
}

#[async_trait]
impl Heartbeater for HeartbeatHandle {
    async fn heartbeat(&self, duration: Option<Duration>) -> TaskResult<()> {
        let extend_by = duration.unwrap_or(self.claim_timeout);

        if extend_by < Duration::from_secs(1) {
            return Err(TaskError::Validation {
                message: "heartbeat duration must be at least 1 second".to_string(),
            });
        }

        let query = "SELECT durable.extend_claim($1, $2, $3)";
        sqlx::query(query)
            .bind(&self.queue_name)
            .bind(self.run_id)
            .bind(extend_by.as_secs() as i32)
            .execute(&self.pool)
            .await
            .map_err(TaskError::from_sqlx_error)?;

        // Notify worker that lease was extended so it can reset timers
        self.lease_extender.notify(extend_by);

        Ok(())
    }
}

/// No-op heartbeater for testing and non-durable contexts.
///
/// All heartbeat calls succeed immediately without any side effects.
#[derive(Clone, Default)]
pub struct NoopHeartbeater;

#[async_trait]
impl Heartbeater for NoopHeartbeater {
    async fn heartbeat(&self, _duration: Option<Duration>) -> TaskResult<()> {
        Ok(())
    }
}

/// State provided to `step()` closures, wrapping the user's application state
/// alongside a heartbeater for extending the task lease.
///
/// This is passed as the second argument to every `step()` closure, making
/// heartbeating available without the consumer needing to thread it manually.
///
/// # Example
///
/// ```ignore
/// ctx.step("long-operation", params, |params, step_state| async move {
///     for item in &params.items {
///         process(item, &step_state.state).await?;
///         // Extend lease during long-running work
///         let _ = step_state.heartbeater.heartbeat(None).await;
///     }
///     Ok(result)
/// }).await?;
/// ```
///
/// For testing step closures in isolation, construct with [`NoopHeartbeater`]:
///
/// ```ignore
/// let step_state = StepState {
///     state: my_test_state,
///     heartbeater: Arc::new(NoopHeartbeater),
/// };
/// ```
pub struct StepState<State> {
    /// The user's application state.
    pub state: State,
    /// Handle for extending the task lease during long-running operations.
    pub heartbeater: Arc<dyn Heartbeater>,
}
