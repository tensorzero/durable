use durable::{Task, TaskContext, TaskResult, async_trait};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

// ============================================================================
// NoOpTask - Minimal task for baseline throughput measurement
// ============================================================================

#[allow(dead_code)]
pub struct NoOpTask;

#[async_trait]
impl Task<()> for NoOpTask {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("bench-noop")
    }
    type Params = ();
    type Output = ();
    type Error = std::convert::Infallible;

    async fn run(_params: Self::Params, _ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
        Ok(())
    }
}

// ============================================================================
// QuickTask - Fast task for claim benchmarks
// ============================================================================

#[allow(dead_code)]
pub struct QuickTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickParams {
    pub task_num: u32,
}

#[async_trait]
impl Task<()> for QuickTask {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("bench-quick")
    }
    type Params = QuickParams;
    type Output = u32;
    type Error = std::convert::Infallible;

    async fn run(params: Self::Params, _ctx: TaskContext, _state: ()) -> TaskResult<Self::Output> {
        Ok(params.task_num)
    }
}

// ============================================================================
// MultiStepBenchTask - Task with configurable number of checkpoint steps
// ============================================================================

#[allow(dead_code)]
pub struct MultiStepBenchTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiStepParams {
    pub num_steps: u32,
}

#[async_trait]
impl Task<()> for MultiStepBenchTask {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("bench-multi-step")
    }
    type Params = MultiStepParams;
    type Output = u32;
    type Error = std::convert::Infallible;

    async fn run(
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        for i in 0..params.num_steps {
            let _: u32 = ctx
                .step(&format!("step-{}", i), i, |i, _| async move { Ok(i) })
                .await?;
        }
        Ok(params.num_steps)
    }
}

// ============================================================================
// LargePayloadBenchTask - Task that checkpoints a large payload
// ============================================================================

#[allow(dead_code)]
pub struct LargePayloadBenchTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LargePayloadParams {
    pub payload_size: usize,
}

#[async_trait]
impl Task<()> for LargePayloadBenchTask {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("bench-large-payload")
    }
    type Params = LargePayloadParams;
    type Output = usize;
    type Error = std::convert::Infallible;

    async fn run(
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let payload = "x".repeat(params.payload_size);
        let _: String = ctx
            .step(
                "large-step",
                payload,
                |payload, _| async move { Ok(payload) },
            )
            .await?;
        Ok(params.payload_size)
    }
}
