//! Credential-free timing of the real local driver between consecutive tools.
//! Run alone on bb2: cargo run --locked -p acyclic-harness --all-features
//! --example runtime-turn-cost -- 1000
use acyclic_harness::{
    Error, LocalHarness, Result,
    context::{CompactionPolicy, ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest, ToolCall},
    runtime::ToolContext,
    tool::{JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Timing {
    tool_finished: Option<Instant>,
    intervals: Vec<Duration>,
}
impl Timing {
    fn report_checkpoint(&self) {
        let end = self.intervals.len();
        if [1, 10, 14, 100, 1000].contains(&end) {
            let (total, count) = self
                .intervals
                .iter()
                .rev()
                .take(10)
                .fold((Duration::ZERO, 0_u32), |(total, count), elapsed| {
                    (total + *elapsed, count + 1)
                });
            println!(
                "tool-to-model ending at turn {end}: {:.3} ms/turn",
                (total / count).as_secs_f64() * 1000.0
            );
        }
    }
}
struct Provider {
    turns: u32,
    timing: Arc<Mutex<Timing>>,
}
impl ModelProvider for Provider {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity {
            context_tokens: 1_048_576,
            output_tokens: Some(4096),
        })
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                u32::try_from(request.bytes().len())
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                vec![0; request.request().messages.len()],
            ))
        })
    }
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        // Include destruction of the SDK's owned request in runtime overhead;
        // this fixture retains neither earlier requests nor their full messages.
        drop(request);
        let mut timing = self.timing.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(finished) = timing.tool_finished.take() {
            timing.intervals.push(finished.elapsed());
            timing.report_checkpoint();
        }
        drop(timing);
        let mut events = Vec::new();
        if dispatch.step < self.turns {
            events.push(Ok(ModelEvent::ToolCall(ToolCall::new(
                format!("call-{}", dispatch.step),
                "bench.noop".into(),
                json!({}),
            ))));
        } else {
            events.push(Ok(ModelEvent::Content {
                delta: "done".into(),
            }));
        }
        events.push(Ok(ModelEvent::Completed {
            metadata: Value::Null,
        }));
        Box::pin(futures::stream::iter(events))
    }
}
struct Noop(Arc<Mutex<Timing>>);
impl ToolExecutor for Noop {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .tool_finished = Some(Instant::now());
            Ok(ToolResult { value: json!({}) })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let turns = std::env::args()
        .nth(1)
        .map_or(Ok(1000), |arg| arg.parse::<u32>())?;
    let root = tempfile::tempdir()?;
    let checkout = root.path().join("checkout");
    std::fs::create_dir(&checkout)?;
    let timing = Arc::new(Mutex::new(Timing::default()));
    let provider = Arc::new(Provider {
        turns,
        timing: timing.clone(),
    });
    let tool = Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "bench.noop".into(),
            revision: "1".into(),
            description: "Constant work".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({"type":"object"}),
            projection_schema: acyclic_harness::tool::json_projection_schema(
                json!({"type":"object"}),
            ),
        },
        executor: Arc::new(Noop(timing.clone())),
        projection: Arc::new(JsonToolProjection),
    };
    let config = acyclic_harness::test_support::local_harness_config(
        checkout,
        (
            Model::new("bench".into(), "noop".into(), "1".into(), Value::Null)?,
            provider,
        ),
    )?
    .without_default_tools()
    .tool(tool)
    .compaction(CompactionPolicy::Disabled);
    let harness = LocalHarness::open(config).await?;
    let output = harness.run("Run the deterministic tool sequence").await?;
    assert_eq!(output.text, "done");
    assert_eq!(
        timing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .intervals
            .len(),
        usize::try_from(turns)?
    );
    harness.close().await?;
    Ok(())
}
