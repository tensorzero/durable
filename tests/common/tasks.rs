use durable::{SpawnOptions, Task, TaskContext, TaskError, TaskHandle, TaskResult, async_trait};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

// ============================================================================
// ResearchTask - Example from README demonstrating multi-step checkpointing
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct ResearchTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchParams {
    pub query: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchResult {
    pub summary: String,
    pub sources: Vec<String>,
}

#[async_trait]
impl Task<()> for ResearchTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("research")
    }
    type Params = ResearchParams;
    type Output = ResearchResult;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Phase 1: Find relevant sources (checkpointed)
        let sources: Vec<String> = ctx
            .step("find-sources", (), |_, _| async {
                Ok(vec![
                    "https://example.com/article1".into(),
                    "https://example.com/article2".into(),
                ])
            })
            .await?;

        // Phase 2: Analyze the sources (checkpointed)
        let analysis: String = ctx
            .step("analyze", (), |_, _| async {
                Ok("Key findings from sources...".into())
            })
            .await?;

        // Phase 3: Generate summary (checkpointed)
        let summary: String = ctx
            .step(
                "summarize",
                (params, analysis),
                |(params, analysis), _| async move {
                    Ok(format!(
                        "Research summary for '{}': {}",
                        params.query, analysis
                    ))
                },
            )
            .await?;

        Ok(ResearchResult { summary, sources })
    }
}

// ============================================================================
// EchoTask - Simple task that returns input
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct EchoTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoParams {
    pub message: String,
}

#[async_trait]
impl Task<()> for EchoTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("echo")
    }
    type Params = EchoParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Ok(params.message)
    }
}

// ============================================================================
// FailingTask - Task that always fails
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct FailingTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailingParams {
    pub error_message: String,
}

#[async_trait]
impl Task<()> for FailingTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("failing")
    }
    type Params = FailingParams;
    type Output = ();

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Err(TaskError::user_message(params.error_message.to_string()))
    }
}

// ============================================================================
// MultiStepTask - Task with multiple checkpointed steps
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct MultiStepTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiStepOutput {
    pub step1: i32,
    pub step2: i32,
    pub step3: i32,
}

#[async_trait]
impl Task<()> for MultiStepTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("multi-step")
    }
    type Params = ();
    type Output = MultiStepOutput;

    async fn run(
        &self,
        _params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let step1: i32 = ctx.step("step1", (), |_, _| async { Ok(1) }).await?;
        let step2: i32 = ctx.step("step2", (), |_, _| async { Ok(2) }).await?;
        let step3: i32 = ctx.step("step3", (), |_, _| async { Ok(3) }).await?;
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

#[allow(dead_code)]
#[derive(Default)]
pub struct SleepingTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleepParams {
    pub seconds: u64,
}

#[async_trait]
impl Task<()> for SleepingTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("sleeping")
    }
    type Params = SleepParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        ctx.sleep_for("wait", std::time::Duration::from_secs(params.seconds))
            .await?;
        Ok("woke up".to_string())
    }
}

// ============================================================================
// EventWaitingTask - Task that waits for an event
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct EventWaitingTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventWaitParams {
    pub event_name: String,
    pub timeout_seconds: Option<u64>,
}

#[async_trait]
impl Task<()> for EventWaitingTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("event-waiting")
    }
    type Params = EventWaitParams;
    type Output = serde_json::Value;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let timeout = params.timeout_seconds.map(std::time::Duration::from_secs);
        let payload: serde_json::Value = ctx.await_event(&params.event_name, timeout).await?;
        Ok(payload)
    }
}

// ============================================================================
// CountingParams - Parameters for counting retry attempts
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountingParams {
    pub fail_until_attempt: u32,
}

// ============================================================================
// StepCountingTask - Tracks how many times each step executes
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct StepCountingTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepCountingParams {
    /// If true, fail after step2
    pub fail_after_step2: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepCountingOutput {
    pub step1_value: String,
    pub step2_value: String,
    pub step3_value: String,
}

#[async_trait]
impl Task<()> for StepCountingTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-counting")
    }
    type Params = StepCountingParams;
    type Output = StepCountingOutput;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Each step returns a unique value that indicates it ran
        let step1_value: String = ctx
            .step("step1", (), |_, _| async {
                Ok("step1_executed".to_string())
            })
            .await?;

        let step2_value: String = ctx
            .step("step2", (), |_, _| async {
                Ok("step2_executed".to_string())
            })
            .await?;

        if params.fail_after_step2 {
            return Err(TaskError::user_message(
                "Intentional failure after step2".to_string(),
            ));
        }

        let step3_value: String = ctx
            .step("step3", (), |_, _| async {
                Ok("step3_executed".to_string())
            })
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

#[allow(dead_code)]
#[derive(Default)]
pub struct EmptyParamsTask;

#[async_trait]
impl Task<()> for EmptyParamsTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("empty-params")
    }
    type Params = ();
    type Output = String;

    async fn run(
        &self,
        _params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Ok("completed".to_string())
    }
}

// ============================================================================
// HeartbeatTask - Task that uses heartbeat for long operations
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct HeartbeatTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatParams {
    pub iterations: u32,
}

#[async_trait]
impl Task<()> for HeartbeatTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("heartbeat")
    }
    type Params = HeartbeatParams;
    type Output = u32;

    async fn run(
        &self,
        params: Self::Params,
        ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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

#[allow(dead_code)]
#[derive(Default)]
pub struct ConvenienceMethodsTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConvenienceMethodsOutput {
    pub rand_value: f64,
    pub now_value: String,
    pub uuid_value: uuid::Uuid,
}

#[async_trait]
impl Task<()> for ConvenienceMethodsTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("convenience-methods")
    }
    type Params = ();
    type Output = ConvenienceMethodsOutput;

    async fn run(
        &self,
        _params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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

#[allow(dead_code)]
#[derive(Default)]
pub struct MultipleConvenienceCallsTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleCallsOutput {
    pub rand1: f64,
    pub rand2: f64,
    pub uuid1: uuid::Uuid,
    pub uuid2: uuid::Uuid,
}

#[async_trait]
impl Task<()> for MultipleConvenienceCallsTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("multiple-convenience-calls")
    }
    type Params = ();
    type Output = MultipleCallsOutput;

    async fn run(
        &self,
        _params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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

#[allow(dead_code)]
#[derive(Default)]
pub struct ReservedPrefixTask;

#[async_trait]
impl Task<()> for ReservedPrefixTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("reserved-prefix")
    }
    type Params = ();
    type Output = ();

    async fn run(
        &self,
        _params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // This should fail because $ is reserved
        let _: String = ctx
            .step("$bad-name", (), |_, _| async { Ok("test".into()) })
            .await?;
        Ok(())
    }
}

// ============================================================================
// Child tasks for spawn/join testing
// ============================================================================

/// Simple child task that doubles a number
#[allow(dead_code)]
#[derive(Default)]
pub struct DoubleTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoubleParams {
    pub value: i32,
}

#[async_trait]
impl Task<()> for DoubleTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("double")
    }
    type Params = DoubleParams;
    type Output = i32;

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Ok(params.value * 2)
    }
}

/// Child task that always fails
#[allow(dead_code)]
#[derive(Default)]
pub struct FailingChildTask;

#[async_trait]
impl Task<()> for FailingChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("failing-child")
    }
    type Params = ();
    type Output = ();

    async fn run(
        &self,
        _params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        Err(TaskError::user_message(
            "Child task failed intentionally".to_string(),
        ))
    }
}

// ============================================================================
// Parent tasks for spawn/join testing
// ============================================================================

/// Parent task that spawns a single child and joins it
#[allow(dead_code)]
#[derive(Default)]
pub struct SingleSpawnTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleSpawnParams {
    pub child_value: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleSpawnOutput {
    pub child_result: i32,
}

#[async_trait]
impl Task<()> for SingleSpawnTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("single-spawn")
    }
    type Params = SingleSpawnParams;
    type Output = SingleSpawnOutput;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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
        let child_result: i32 = ctx.join(handle).await?;

        Ok(SingleSpawnOutput { child_result })
    }
}

/// Parent task that spawns multiple children and joins them
#[allow(dead_code)]
#[derive(Default)]
pub struct MultiSpawnTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSpawnParams {
    pub values: Vec<i32>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSpawnOutput {
    pub results: Vec<i32>,
}

#[async_trait]
impl Task<()> for MultiSpawnTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("multi-spawn")
    }
    type Params = MultiSpawnParams;
    type Output = MultiSpawnOutput;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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
        for handle in handles {
            let result: i32 = ctx.join(handle).await?;
            results.push(result);
        }

        Ok(MultiSpawnOutput { results })
    }
}

/// Parent task that spawns a failing child
#[allow(dead_code)]
#[derive(Default)]
pub struct SpawnFailingChildTask;

#[async_trait]
impl Task<()> for SpawnFailingChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("spawn-failing-child")
    }
    type Params = ();
    type Output = ();

    async fn run(
        &self,
        _params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Spawn with max_attempts=1 so child fails immediately without retries
        let handle: TaskHandle<()> = ctx
            .spawn::<FailingChildTask>("child", (), {
                let mut opts = SpawnOptions::default();
                opts.max_attempts = Some(1);
                opts
            })
            .await?;
        // This should fail because child fails
        ctx.join(handle).await?;
        Ok(())
    }
}

// ============================================================================
// LongRunningHeartbeatTask - Task that runs longer than claim_timeout but heartbeats
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct LongRunningHeartbeatTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LongRunningHeartbeatParams {
    /// Total time to run in milliseconds
    pub total_duration_ms: u64,
    /// Interval between heartbeats in milliseconds
    pub heartbeat_interval_ms: u64,
}

#[async_trait]
impl Task<()> for LongRunningHeartbeatTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("long-running-heartbeat")
    }
    type Params = LongRunningHeartbeatParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let start = std::time::Instant::now();
        let total_duration = std::time::Duration::from_millis(params.total_duration_ms);
        let heartbeat_interval = std::time::Duration::from_millis(params.heartbeat_interval_ms);

        while start.elapsed() < total_duration {
            tokio::time::sleep(heartbeat_interval).await;
            ctx.heartbeat(None).await?;
        }

        Ok("completed".to_string())
    }
}

/// Slow child task (for testing cancellation)
#[allow(dead_code)]
#[derive(Default)]
pub struct SlowChildTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowChildParams {
    pub sleep_ms: u64,
}

#[async_trait]
impl Task<()> for SlowChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("slow-child")
    }
    type Params = SlowChildParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        tokio::time::sleep(std::time::Duration::from_millis(params.sleep_ms)).await;
        Ok("done".to_string())
    }
}

/// Parent task that spawns a slow child (for testing cancellation)
#[allow(dead_code)]
#[derive(Default)]
pub struct SpawnSlowChildTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnSlowChildParams {
    pub child_sleep_ms: u64,
}

#[async_trait]
impl Task<()> for SpawnSlowChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("spawn-slow-child")
    }
    type Params = SpawnSlowChildParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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
        let result = ctx.join(handle).await?;
        Ok(result)
    }
}

// ============================================================================
// EventEmitterTask - Task that emits an event then completes
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct EventEmitterTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEmitterParams {
    pub event_name: String,
    pub payload: serde_json::Value,
}

#[async_trait]
impl Task<()> for EventEmitterTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("event-emitter")
    }
    type Params = EventEmitterParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        ctx.emit_event(&params.event_name, &params.payload).await?;
        Ok("emitted".to_string())
    }
}

// ============================================================================
// ManyStepsTask - Task with configurable number of steps
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct ManyStepsTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManyStepsParams {
    pub num_steps: u32,
}

#[async_trait]
impl Task<()> for ManyStepsTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("many-steps")
    }
    type Params = ManyStepsParams;
    type Output = u32;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        for i in 0..params.num_steps {
            let _: u32 = ctx
                .step(&format!("step-{i}"), i, |i, _| async move { Ok(i) })
                .await?;
        }
        Ok(params.num_steps)
    }
}

// ============================================================================
// LargePayloadTask - Task that returns a large payload
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct LargePayloadTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LargePayloadParams {
    /// Size of the payload in bytes (approximately)
    pub size_bytes: usize,
}

#[async_trait]
impl Task<()> for LargePayloadTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("large-payload")
    }
    type Params = LargePayloadParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Create a large string of repeated characters
        let payload: String = ctx
            .step("generate", params, |params, _| async move {
                Ok("x".repeat(params.size_bytes))
            })
            .await?;
        Ok(payload)
    }
}

// ============================================================================
// CpuBoundTask - Task that busy-loops without yielding (can't heartbeat)
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct CpuBoundTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuBoundParams {
    /// Duration to busy-loop in milliseconds
    pub duration_ms: u64,
}

#[async_trait]
impl Task<()> for CpuBoundTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("cpu-bound")
    }
    type Params = CpuBoundParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let start = std::time::Instant::now();
        let duration = std::time::Duration::from_millis(params.duration_ms);

        // Busy loop - no yielding, no heartbeat possible
        while start.elapsed() < duration {
            // Spin - this prevents any async yielding
            std::hint::spin_loop();
        }

        Ok("done".to_string())
    }
}

// ============================================================================
// SlowNoHeartbeatTask - Async task that sleeps longer than lease without heartbeat
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct SlowNoHeartbeatTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowNoHeartbeatParams {
    /// Duration to sleep in milliseconds (should be > claim_timeout)
    pub sleep_ms: u64,
}

#[async_trait]
impl Task<()> for SlowNoHeartbeatTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("slow-no-heartbeat")
    }
    type Params = SlowNoHeartbeatParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        _ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Just sleep - no heartbeat calls
        tokio::time::sleep(std::time::Duration::from_millis(params.sleep_ms)).await;
        Ok("done".to_string())
    }
}

// ============================================================================
// SleepThenCheckpointTask - Sleep past lease then attempt a checkpoint
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct SleepThenCheckpointTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleepThenCheckpointParams {
    /// Duration to sleep in milliseconds (should be > claim_timeout)
    pub sleep_ms: u64,
}

#[async_trait]
impl Task<()> for SleepThenCheckpointTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("sleep-then-checkpoint")
    }
    type Params = SleepThenCheckpointParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        tokio::time::sleep(std::time::Duration::from_millis(params.sleep_ms)).await;
        let _: String = ctx
            .step("after-sleep", (), |_, _| async { Ok("ok".to_string()) })
            .await?;
        Ok("done".to_string())
    }
}

// ============================================================================
// CheckpointReplayTask - Tracks execution count via external counter
// ============================================================================

/// Shared state for tracking task execution across retries.
/// Use Arc<ExecutionTracker> and pass to task via thread-local or similar mechanism.
#[allow(dead_code)]
#[derive(Default)]
pub struct ExecutionTracker {
    pub step1_count: AtomicU32,
    pub step2_count: AtomicU32,
    pub step3_count: AtomicU32,
    pub should_fail_after_step2: AtomicBool,
}

impl ExecutionTracker {
    #[allow(dead_code)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[allow(dead_code)]
    pub fn reset(&self) {
        self.step1_count.store(0, Ordering::SeqCst);
        self.step2_count.store(0, Ordering::SeqCst);
        self.step3_count.store(0, Ordering::SeqCst);
        self.should_fail_after_step2.store(false, Ordering::SeqCst);
    }
}

// Thread-local storage for execution tracker
thread_local! {
    static EXECUTION_TRACKER: std::cell::RefCell<Option<Arc<ExecutionTracker>>> = const { std::cell::RefCell::new(None) };
}

#[allow(dead_code)]
pub fn set_execution_tracker(tracker: Arc<ExecutionTracker>) {
    EXECUTION_TRACKER.with(|t| {
        *t.borrow_mut() = Some(tracker);
    });
}

#[allow(dead_code)]
pub fn get_execution_tracker() -> Option<Arc<ExecutionTracker>> {
    EXECUTION_TRACKER.with(|t| t.borrow().clone())
}

// ============================================================================
// DeterministicReplayTask - Verifies rand/now/uuid7 are deterministic on retry
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct DeterministicReplayTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterministicReplayParams {
    pub fail_on_first_attempt: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterministicReplayOutput {
    pub rand_value: f64,
    pub now_value: String,
    pub uuid_value: uuid::Uuid,
}

// Track whether we've already failed once
thread_local! {
    static DETERMINISTIC_TASK_FAILED: std::cell::RefCell<bool> = const { std::cell::RefCell::new(false) };
}

#[allow(dead_code)]
pub fn reset_deterministic_task_state() {
    DETERMINISTIC_TASK_FAILED.with(|f| *f.borrow_mut() = false);
}

#[async_trait]
impl Task<()> for DeterministicReplayTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("deterministic-replay")
    }
    type Params = DeterministicReplayParams;
    type Output = DeterministicReplayOutput;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let rand_value = ctx.rand().await?;
        let now_value = ctx.now().await?;
        let uuid_value = ctx.uuid7().await?;

        // Fail on first attempt if requested
        if params.fail_on_first_attempt {
            let should_fail = DETERMINISTIC_TASK_FAILED.with(|f| {
                let already_failed = *f.borrow();
                if !already_failed {
                    *f.borrow_mut() = true;
                    true
                } else {
                    false
                }
            });

            if should_fail {
                return Err(TaskError::user_message("First attempt failure".to_string()));
            }
        }

        Ok(DeterministicReplayOutput {
            rand_value,
            now_value: now_value.to_rfc3339(),
            uuid_value,
        })
    }
}

// ============================================================================
// EventThenFailTask - Task that receives event then fails on first attempt
// ============================================================================

thread_local! {
    static EVENT_THEN_FAIL_FAILED: std::cell::RefCell<bool> = const { std::cell::RefCell::new(false) };
}

#[allow(dead_code)]
pub fn reset_event_then_fail_state() {
    EVENT_THEN_FAIL_FAILED.with(|f| *f.borrow_mut() = false);
}

#[allow(dead_code)]
#[derive(Default)]
pub struct EventThenFailTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventThenFailParams {
    pub event_name: String,
}

#[async_trait]
impl Task<()> for EventThenFailTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("event-then-fail")
    }
    type Params = EventThenFailParams;
    type Output = serde_json::Value;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Wait for event (will be checkpointed)
        let payload: serde_json::Value = ctx.await_event(&params.event_name, None).await?;

        // Fail on first attempt to test checkpoint preservation
        let should_fail = EVENT_THEN_FAIL_FAILED.with(|f| {
            let already_failed = *f.borrow();
            if !already_failed {
                *f.borrow_mut() = true;
                true
            } else {
                false
            }
        });

        if should_fail {
            return Err(TaskError::user_message(
                "First attempt failure after event".to_string(),
            ));
        }

        // Second attempt succeeds with the same payload (from checkpoint)
        Ok(payload)
    }
}

// ============================================================================
// EventThenDelayTask - Task that receives event then delays before completing
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct EventThenDelayTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventThenDelayParams {
    pub event_name: String,
    pub delay_ms: u64,
}

#[async_trait]
impl Task<()> for EventThenDelayTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("event-then-delay")
    }
    type Params = EventThenDelayParams;
    type Output = serde_json::Value;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Wait for event (will be checkpointed)
        let payload: serde_json::Value = ctx.await_event(&params.event_name, None).await?;

        // Delay after receiving event - during this time, subsequent writes
        // to the same event should not affect what we received
        tokio::time::sleep(std::time::Duration::from_millis(params.delay_ms)).await;

        // Return the payload we received when first woken
        Ok(payload)
    }
}

// ============================================================================
// MultiEventTask - Task that awaits multiple distinct events
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct MultiEventTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiEventParams {
    pub event1_name: String,
    pub event2_name: String,
}

#[async_trait]
impl Task<()> for MultiEventTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("multi-event")
    }
    type Params = MultiEventParams;
    type Output = serde_json::Value;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        let payload1: serde_json::Value = ctx.await_event(&params.event1_name, None).await?;
        let payload2: serde_json::Value = ctx.await_event(&params.event2_name, None).await?;

        Ok(serde_json::json!({
            "event1": payload1,
            "event2": payload2,
        }))
    }
}

// ============================================================================
// SpawnThenFailTask - Task that spawns a child then fails on first attempt
// ============================================================================

thread_local! {
    static SPAWN_THEN_FAIL_FAILED: std::cell::RefCell<bool> = const { std::cell::RefCell::new(false) };
}

#[allow(dead_code)]
pub fn reset_spawn_then_fail_state() {
    SPAWN_THEN_FAIL_FAILED.with(|f| *f.borrow_mut() = false);
}

#[allow(dead_code)]
#[derive(Default)]
pub struct SpawnThenFailTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnThenFailParams {
    pub child_steps: u32,
}

#[async_trait]
impl Task<()> for SpawnThenFailTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("spawn-then-fail")
    }
    type Params = SpawnThenFailParams;
    type Output = serde_json::Value;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        use durable::SpawnOptions;

        // Spawn child (should be idempotent)
        let child_handle = ctx
            .spawn::<ManyStepsTask>(
                "child",
                ManyStepsParams {
                    num_steps: params.child_steps,
                },
                SpawnOptions::default(),
            )
            .await?;

        // Fail on first attempt
        let should_fail = SPAWN_THEN_FAIL_FAILED.with(|f| {
            let already_failed = *f.borrow();
            if !already_failed {
                *f.borrow_mut() = true;
                true
            } else {
                false
            }
        });

        if should_fail {
            return Err(TaskError::user_message(
                "First attempt failure after spawn".to_string(),
            ));
        }

        // Second attempt - join child
        let child_result: u32 = ctx.join(child_handle).await?;

        Ok(serde_json::json!({
            "child_result": child_result
        }))
    }
}

// ============================================================================
// SpawnByNameTask - Tests spawn_by_name on TaskContext
// ============================================================================

/// Parent task that spawns a child using spawn_by_name (dynamic version)
#[allow(dead_code)]
#[derive(Default)]
pub struct SpawnByNameTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnByNameParams {
    pub child_value: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnByNameOutput {
    pub child_result: i32,
}

#[async_trait]
impl Task<()> for SpawnByNameTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("spawn-by-name")
    }
    type Params = SpawnByNameParams;
    type Output = SpawnByNameOutput;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Spawn child task using spawn_by_name (dynamic version)
        let handle: TaskHandle<i32> = ctx
            .spawn_by_name(
                "child",
                "double", // task name string instead of type
                serde_json::json!({ "value": params.child_value }),
                Default::default(),
            )
            .await?;

        // Join and get result
        let child_result: i32 = ctx.join(handle).await?;

        Ok(SpawnByNameOutput { child_result })
    }
}

// ============================================================================
// JoinCancelledChildTask - Parent that spawns a slow child, then joins after child is cancelled
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct JoinCancelledChildTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCancelledChildParams {
    pub child_sleep_ms: u64,
}

#[async_trait]
impl Task<()> for JoinCancelledChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("join-cancelled-child")
    }
    type Params = JoinCancelledChildParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        mut ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
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

        // Wait a bit for the child to start (it will be cancelled externally)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Try to join - should fail with ChildCancelled if child was cancelled
        let result = ctx.join(handle).await?;
        Ok(result)
    }
}

// ============================================================================
// VerySlowChildTask - Very slow child task (for testing join timeout)
// ============================================================================

#[allow(dead_code)]
#[derive(Default)]
pub struct VerySlowChildTask;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerySlowChildParams {
    pub sleep_ms: u64,
}

#[async_trait]
impl Task<()> for VerySlowChildTask {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("very-slow-child")
    }
    type Params = VerySlowChildParams;
    type Output = String;

    async fn run(
        &self,
        params: Self::Params,
        ctx: TaskContext,
        _state: (),
    ) -> TaskResult<Self::Output> {
        // Heartbeat regularly to keep this task alive
        let interval_ms = 500;
        let total_ms = params.sleep_ms;
        let mut elapsed = 0u64;

        while elapsed < total_ms {
            let sleep_time = std::cmp::min(interval_ms, total_ms - elapsed);
            tokio::time::sleep(std::time::Duration::from_millis(sleep_time)).await;
            ctx.heartbeat(None).await?;
            elapsed += sleep_time;
        }

        Ok("done".to_string())
    }
}
