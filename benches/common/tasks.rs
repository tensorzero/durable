use durable::{SpawnOptions, Task, TaskContext, TaskHandle, TaskResult, async_trait};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

// ============================================================================
// NoOpTask - Minimal task for baseline throughput measurement
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct NoOpTask;

#[async_trait]
impl Task<()> for NoOpTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-noop")
    }
    type Params = ();
    type Output = ();

    async fn run(
        &self,
        _params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Ok(())
    }
}

// ============================================================================
// QuickTask - Fast task for claim benchmarks
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct QuickTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickParams {
    pub task_num: u32,
}

#[async_trait]
impl Task<()> for QuickTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-quick")
    }
    type Params = QuickParams;
    type Output = u32;

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Ok(params.task_num)
    }
}

// ============================================================================
// MultiStepBenchTask - Task with configurable number of checkpoint steps
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct MultiStepBenchTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiStepParams {
    pub num_steps: u32,
}

#[async_trait]
impl Task<()> for MultiStepBenchTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-multi-step")
    }
    type Params = MultiStepParams;
    type Output = u32;

    async fn run(
        &self,
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
#[derive(Default)]
pub struct LargePayloadBenchTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LargePayloadParams {
    pub payload_size: usize,
}

#[async_trait]
impl Task<()> for LargePayloadBenchTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-large-payload")
    }
    type Params = LargePayloadParams;
    type Output = usize;

    async fn run(
        &self,
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

// ============================================================================
// Hierarchical Tasks - Parent -> Child -> Grandchild for stress testing
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentParams {
    pub num_children: u32,
    pub grandchildren_per_child: u32,
}

#[allow(dead_code)]
#[derive(Default)]
pub struct ParentTask;

#[async_trait]
impl Task<()> for ParentTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-parent")
    }
    type Params = ParentParams;
    type Output = u32;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let mut handles = Vec::new();
        for i in 0..params.num_children {
            let handle: TaskHandle<u32> = ctx
                .spawn::<ChildTask>(
                    &format!("child-{}", i),
                    ChildParams {
                        num_grandchildren: params.grandchildren_per_child,
                    },
                    SpawnOptions::default(),
                )
                .await?;
            handles.push(handle);
        }

        let mut total = 0u32;
        for handle in handles {
            total += ctx.join(handle).await?;
        }
        Ok(total)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildParams {
    pub num_grandchildren: u32,
}

#[allow(dead_code)]
#[derive(Default)]
pub struct ChildTask;

#[async_trait]
impl Task<()> for ChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-child")
    }
    type Params = ChildParams;
    type Output = u32;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let mut handles = Vec::new();
        for i in 0..params.num_grandchildren {
            let handle: TaskHandle<()> = ctx
                .spawn::<GrandchildTask>(&format!("grandchild-{}", i), (), SpawnOptions::default())
                .await?;
            handles.push(handle);
        }

        for handle in handles {
            ctx.join(handle).await?;
        }
        Ok(params.num_grandchildren)
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct GrandchildTask;

#[async_trait]
impl Task<()> for GrandchildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bench-grandchild")
    }
    type Params = ();
    type Output = ();

    async fn run(
        &self,
        _params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Ok(())
    }
}
