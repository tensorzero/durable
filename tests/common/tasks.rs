use durable::{Task, TaskContext, TaskError, TaskResult, async_trait};
use serde::{Deserialize, Serialize};

// ============================================================================
// EchoTask - Simple task that returns input
// ============================================================================

pub struct EchoTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoParams {
    pub message: String,
}

#[async_trait]
impl Task for EchoTask {
    const NAME: &'static str = "echo";
    type Params = EchoParams;
    type Output = String;

    async fn run(params: Self::Params, _ctx: TaskContext) -> TaskResult<Self::Output> {
        Ok(params.message)
    }
}

// ============================================================================
// FailingTask - Task that always fails
// ============================================================================

pub struct FailingTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailingParams {
    pub error_message: String,
}

#[async_trait]
impl Task for FailingTask {
    const NAME: &'static str = "failing";
    type Params = FailingParams;
    type Output = ();

    async fn run(params: Self::Params, _ctx: TaskContext) -> TaskResult<Self::Output> {
        Err(TaskError::Failed(anyhow::anyhow!(
            "{}",
            params.error_message
        )))
    }
}

// ============================================================================
// MultiStepTask - Task with multiple checkpointed steps
// ============================================================================

pub struct MultiStepTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiStepOutput {
    pub step1: i32,
    pub step2: i32,
    pub step3: i32,
}

#[async_trait]
impl Task for MultiStepTask {
    const NAME: &'static str = "multi-step";
    type Params = ();
    type Output = MultiStepOutput;

    async fn run(_params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        let step1: i32 = ctx.step("step1", || async { Ok(1) }).await?;
        let step2: i32 = ctx.step("step2", || async { Ok(2) }).await?;
        let step3: i32 = ctx.step("step3", || async { Ok(3) }).await?;
        Ok(MultiStepOutput {
            step1,
            step2,
            step3,
        })
    }
}

// ============================================================================
// SleepingTask - Task that sleeps for a duration
// ============================================================================

pub struct SleepingTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleepParams {
    pub seconds: u64,
}

#[async_trait]
impl Task for SleepingTask {
    const NAME: &'static str = "sleeping";
    type Params = SleepParams;
    type Output = String;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        ctx.sleep_for("wait", std::time::Duration::from_secs(params.seconds))
            .await?;
        Ok("woke up".to_string())
    }
}

// ============================================================================
// EventWaitingTask - Task that waits for an event
// ============================================================================

pub struct EventWaitingTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventWaitParams {
    pub event_name: String,
    pub timeout_seconds: Option<u64>,
}

#[async_trait]
impl Task for EventWaitingTask {
    const NAME: &'static str = "event-waiting";
    type Params = EventWaitParams;
    type Output = serde_json::Value;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        let timeout = params.timeout_seconds.map(std::time::Duration::from_secs);
        let payload: serde_json::Value = ctx.await_event(&params.event_name, timeout).await?;
        Ok(payload)
    }
}

// ============================================================================
// CountingParams - Parameters for counting retry attempts
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountingParams {
    pub fail_until_attempt: u32,
}

// ============================================================================
// StepCountingTask - Tracks how many times each step executes
// ============================================================================

pub struct StepCountingTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepCountingParams {
    /// If true, fail after step2
    pub fail_after_step2: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepCountingOutput {
    pub step1_value: String,
    pub step2_value: String,
    pub step3_value: String,
}

#[async_trait]
impl Task for StepCountingTask {
    const NAME: &'static str = "step-counting";
    type Params = StepCountingParams;
    type Output = StepCountingOutput;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Each step returns a unique value that indicates it ran
        let step1_value: String = ctx
            .step("step1", || async { Ok("step1_executed".to_string()) })
            .await?;

        let step2_value: String = ctx
            .step("step2", || async { Ok("step2_executed".to_string()) })
            .await?;

        if params.fail_after_step2 {
            return Err(TaskError::Failed(anyhow::anyhow!(
                "Intentional failure after step2"
            )));
        }

        let step3_value: String = ctx
            .step("step3", || async { Ok("step3_executed".to_string()) })
            .await?;

        Ok(StepCountingOutput {
            step1_value,
            step2_value,
            step3_value,
        })
    }
}

// ============================================================================
// EmptyParamsTask - Task with empty params (edge case)
// ============================================================================

pub struct EmptyParamsTask;

#[async_trait]
impl Task for EmptyParamsTask {
    const NAME: &'static str = "empty-params";
    type Params = ();
    type Output = String;

    async fn run(_params: Self::Params, _ctx: TaskContext) -> TaskResult<Self::Output> {
        Ok("completed".to_string())
    }
}

// ============================================================================
// HeartbeatTask - Task that uses heartbeat for long operations
// ============================================================================

pub struct HeartbeatTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatParams {
    pub iterations: u32,
}

#[async_trait]
impl Task for HeartbeatTask {
    const NAME: &'static str = "heartbeat";
    type Params = HeartbeatParams;
    type Output = u32;

    async fn run(params: Self::Params, ctx: TaskContext) -> TaskResult<Self::Output> {
        for _i in 0..params.iterations {
            // Simulate work
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            // Extend lease
            ctx.heartbeat(None).await?;
        }
        Ok(params.iterations)
    }
}
