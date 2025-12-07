use durable::{SpawnOptions, Task, TaskContext, TaskError, TaskHandle, TaskResult, async_trait};
use serde::{Deserialize, Serialize};

// ============================================================================
// ResearchTask - Example from README demonstrating multi-step checkpointing
// ============================================================================

pub struct ResearchTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchParams {
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchResult {
    pub summary: String,
    pub sources: Vec<String>,
}

#[async_trait]
impl Task for ResearchTask {
    const NAME: &'static str = "research";
    type Params = ResearchParams;
    type Output = ResearchResult;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Phase 1: Find relevant sources (checkpointed)
        let sources: Vec<String> = ctx
            .step("find-sources", || async {
                Ok(vec![
                    "https://example.com/article1".into(),
                    "https://example.com/article2".into(),
                ])
            })
            .await?;

        // Phase 2: Analyze the sources (checkpointed)
        let analysis: String = ctx
            .step("analyze", || async {
                Ok("Key findings from sources...".into())
            })
            .await?;

        // Phase 3: Generate summary (checkpointed)
        let summary: String = ctx
            .step("summarize", || async {
                Ok(format!(
                    "Research summary for '{}': {}",
                    params.query, analysis
                ))
            })
            .await?;

        Ok(ResearchResult { summary, sources })
    }
}

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

// ============================================================================
// ConvenienceMethodsTask - Task that uses rand(), now(), and uuid7()
// ============================================================================

pub struct ConvenienceMethodsTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConvenienceMethodsOutput {
    pub rand_value: f64,
    pub now_value: String,
    pub uuid_value: uuid::Uuid,
}

#[async_trait]
impl Task for ConvenienceMethodsTask {
    const NAME: &'static str = "convenience-methods";
    type Params = ();
    type Output = ConvenienceMethodsOutput;

    async fn run(_params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        let rand_value = ctx.rand().await?;
        let now_value = ctx.now().await?;
        let uuid_value = ctx.uuid7().await?;

        Ok(ConvenienceMethodsOutput {
            rand_value,
            now_value: now_value.to_rfc3339(),
            uuid_value,
        })
    }
}

// ============================================================================
// MultipleConvenienceCallsTask - Tests multiple calls produce different values
// ============================================================================

pub struct MultipleConvenienceCallsTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleCallsOutput {
    pub rand1: f64,
    pub rand2: f64,
    pub uuid1: uuid::Uuid,
    pub uuid2: uuid::Uuid,
}

#[async_trait]
impl Task for MultipleConvenienceCallsTask {
    const NAME: &'static str = "multiple-convenience-calls";
    type Params = ();
    type Output = MultipleCallsOutput;

    async fn run(_params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        let rand1 = ctx.rand().await?;
        let rand2 = ctx.rand().await?;
        let uuid1 = ctx.uuid7().await?;
        let uuid2 = ctx.uuid7().await?;

        Ok(MultipleCallsOutput {
            rand1,
            rand2,
            uuid1,
            uuid2,
        })
    }
}

// ============================================================================
// ReservedPrefixTask - Tests that $ prefix is rejected
// ============================================================================

pub struct ReservedPrefixTask;

#[async_trait]
impl Task for ReservedPrefixTask {
    const NAME: &'static str = "reserved-prefix";
    type Params = ();
    type Output = ();

    async fn run(_params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // This should fail because $ is reserved
        let _: String = ctx
            .step("$bad-name", || async { Ok("test".into()) })
            .await?;
        Ok(())
    }
}

// ============================================================================
// Child tasks for spawn/join testing
// ============================================================================

/// Simple child task that doubles a number
pub struct DoubleTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoubleParams {
    pub value: i32,
}

#[async_trait]
impl Task for DoubleTask {
    const NAME: &'static str = "double";
    type Params = DoubleParams;
    type Output = i32;

    async fn run(params: Self::Params, _ctx: TaskContext) -> TaskResult<Self::Output> {
        Ok(params.value * 2)
    }
}

/// Child task that always fails
pub struct FailingChildTask;

#[async_trait]
impl Task for FailingChildTask {
    const NAME: &'static str = "failing-child";
    type Params = ();
    type Output = ();

    async fn run(_params: Self::Params, _ctx: TaskContext) -> TaskResult<Self::Output> {
        Err(TaskError::Failed(anyhow::anyhow!(
            "Child task failed intentionally"
        )))
    }
}

// ============================================================================
// Parent tasks for spawn/join testing
// ============================================================================

/// Parent task that spawns a single child and joins it
pub struct SingleSpawnTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleSpawnParams {
    pub child_value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleSpawnOutput {
    pub child_result: i32,
}

#[async_trait]
impl Task for SingleSpawnTask {
    const NAME: &'static str = "single-spawn";
    type Params = SingleSpawnParams;
    type Output = SingleSpawnOutput;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Spawn child task
        let handle: TaskHandle<i32> = ctx
            .spawn::<DoubleTask>(
                "child",
                DoubleParams {
                    value: params.child_value,
                },
                Default::default(),
            )
            .await?;

        // Join and get result
        let child_result: i32 = ctx.join("child", handle).await?;

        Ok(SingleSpawnOutput { child_result })
    }
}

/// Parent task that spawns multiple children and joins them
pub struct MultiSpawnTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSpawnParams {
    pub values: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSpawnOutput {
    pub results: Vec<i32>,
}

#[async_trait]
impl Task for MultiSpawnTask {
    const NAME: &'static str = "multi-spawn";
    type Params = MultiSpawnParams;
    type Output = MultiSpawnOutput;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Spawn all children
        let mut handles = Vec::new();
        for (i, value) in params.values.iter().enumerate() {
            let handle: TaskHandle<i32> = ctx
                .spawn::<DoubleTask>(
                    &format!("child-{i}"),
                    DoubleParams { value: *value },
                    Default::default(),
                )
                .await?;
            handles.push(handle);
        }

        // Join all children (in order)
        let mut results = Vec::new();
        for (i, handle) in handles.into_iter().enumerate() {
            let result: i32 = ctx.join(&format!("child-{i}"), handle).await?;
            results.push(result);
        }

        Ok(MultiSpawnOutput { results })
    }
}

/// Parent task that spawns a failing child
pub struct SpawnFailingChildTask;

#[async_trait]
impl Task for SpawnFailingChildTask {
    const NAME: &'static str = "spawn-failing-child";
    type Params = ();
    type Output = ();

    async fn run(_params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Spawn with max_attempts=1 so child fails immediately without retries
        let handle: TaskHandle<()> = ctx
            .spawn::<FailingChildTask>(
                "child",
                (),
                SpawnOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                },
            )
            .await?;
        // This should fail because child fails
        ctx.join("child", handle).await?;
        Ok(())
    }
}

/// Slow child task (for testing cancellation)
pub struct SlowChildTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowChildParams {
    pub sleep_ms: u64,
}

#[async_trait]
impl Task for SlowChildTask {
    const NAME: &'static str = "slow-child";
    type Params = SlowChildParams;
    type Output = String;

    async fn run(params: Self::Params, _ctx: TaskContext) -> TaskResult<Self::Output> {
        tokio::time::sleep(std::time::Duration::from_millis(params.sleep_ms)).await;
        Ok("done".to_string())
    }
}

/// Parent task that spawns a slow child (for testing cancellation)
pub struct SpawnSlowChildTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnSlowChildParams {
    pub child_sleep_ms: u64,
}

#[async_trait]
impl Task for SpawnSlowChildTask {
    const NAME: &'static str = "spawn-slow-child";
    type Params = SpawnSlowChildParams;
    type Output = String;

    async fn run(params: Self::Params, mut ctx: TaskContext) -> TaskResult<Self::Output> {
        // Spawn a slow child
        let handle: TaskHandle<String> = ctx
            .spawn::<SlowChildTask>(
                "slow-child",
                SlowChildParams {
                    sleep_ms: params.child_sleep_ms,
                },
                Default::default(),
            )
            .await?;

        // Join (this will wait for the slow child)
        let result = ctx.join("slow-child", handle).await?;
        Ok(result)
    }
}
