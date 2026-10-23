//! Credential-free timing of the real local driver between consecutive tools.
//! Run alone on bb2: cargo run --locked -p acyclic-harness --all-features
//! --example runtime-turn-cost -- 1000
#[path = "runtime_turn_cost/intervals.rs"]
mod intervals;
#[path = "runtime_turn_cost/profile.rs"]
mod profile;
#[path = "runtime_turn_cost/resources.rs"]
mod resources;
#[cfg(test)]
#[path = "runtime_turn_cost/tests.rs"]
mod tests;
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
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Default)]
struct Timing {
    tool_finished: Option<(Instant, u64)>,
    intervals: intervals::Intervals,
    stream_flushes: Arc<AtomicU64>,
    profile: profile::Profile,
    resources: resources::Samples,
    summaries: u32,
    maximum_request_bytes: usize,
}

struct FlushCounter(Arc<AtomicU64>);
impl tracing::field::Visit for FlushCounter {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "fsyncs" {
            self.0.fetch_add(value, Ordering::Relaxed);
        }
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for FlushCounter {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        event.record(&mut Self(self.0.clone()));
    }
}
impl Timing {
    fn report_checkpoint(&mut self) -> Result<()> {
        let end = self.intervals.len();
        if [1, 10, 14, 100, 1000].contains(&end) {
            let (total, flushes, count) = self.intervals.summary();
            println!(
                "tool-to-model ending at turn {end}: {:.3} ms/turn; {flushes} stream flushes/{count} turns",
                (total / count).as_secs_f64() * 1000.0,
            );
            self.profile.report(end);
            self.resources.report(end)?;
            println!(
                "compaction summaries={}; maximum dispatched request={} bytes",
                self.summaries, self.maximum_request_bytes
            );
        }
        Ok(())
    }
}
struct Provider {
    turns: u32,
    compact: bool,
    timing: Arc<Mutex<Timing>>,
}
impl ModelProvider for Provider {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity {
            context_tokens: if self.compact { 65_536 } else { 1_048_576 },
            output_tokens: Some(4096),
        })
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            if self.compact {
                let (count, _) = request.serialized_token_upper_bound(None)?;
                if request.request().messages.last().is_some_and(|message| {
                    matches!(&message.content, acyclic_harness::model::ModelContent::Text(text)
                        if text.starts_with("Summarize the preceding context for continued work."))
                }) {
                    let timing = self.timing.lock().unwrap_or_else(PoisonError::into_inner);
                    eprintln!(
                        "summary candidate after_turn={} summaries={} messages={} request_bytes={} input_tokens={} max_output_tokens={:?}",
                        timing.intervals.len(),
                        timing.summaries,
                        request.request().messages.len(),
                        request.bytes().len(),
                        count.input_tokens,
                        request.request().max_output_tokens,
                    );
                }
                return Ok(count);
            }
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
        let request_bytes = request.bytes().len();
        {
            let mut timing = self.timing.lock().unwrap_or_else(PoisonError::into_inner);
            timing.maximum_request_bytes = timing.maximum_request_bytes.max(request_bytes);
        }
        // The synthetic model answers the SDK's summary instruction with text.
        // It retains the tool catalog when the source includes tool exchanges.
        let summary = request.request().messages.last().is_some_and(|message| {
            matches!(&message.content, acyclic_harness::model::ModelContent::Text(text)
                if text.starts_with("Summarize the preceding context for continued work."))
        });
        if self.compact && summary {
            drop(request);
            self.timing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .summaries += 1;
            return Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "Continue the deterministic bench.noop tool sequence.".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        // Include destruction of the SDK's owned request in runtime overhead;
        // this fixture retains neither earlier requests nor their full messages.
        drop(request);
        let mut timing = self.timing.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((finished, flushes)) = timing.tool_finished.take() {
            let step_flushes = timing.stream_flushes.load(Ordering::Relaxed) - flushes;
            if let Err(error) = timing.intervals.push(finished.elapsed(), step_flushes) {
                return Box::pin(futures::stream::iter([Err(error)]));
            }
            if let Err(error) = timing.report_checkpoint() {
                return Box::pin(futures::stream::iter([Err(error)]));
            }
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
            let mut timing = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let observed = timing.intervals.len();
            if timing.intervals.is_empty() || [90, 990, 9990, 99990, 999990].contains(&observed) {
                timing.resources.start(observed)?;
            }
            timing.tool_finished = Some((
                Instant::now(),
                timing.stream_flushes.load(Ordering::Relaxed),
            ));
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
    let compact = std::env::args().any(|arg| arg == "--compact");
    let root = tempfile::tempdir()?;
    let checkout = root.path().join("checkout");
    std::fs::create_dir(&checkout)?;
    let profile = profile::Profile::from_arguments();
    let timing = Arc::new(Mutex::new(Timing {
        profile: profile.clone(),
        ..Default::default()
    }));
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(profile)
        .with(FlushCounter(
            timing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .stream_flushes
                .clone(),
        ))
        .try_init()?;
    let provider = Arc::new(Provider {
        turns,
        compact,
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
    .state_directory(root.path().join("state"))
    .without_default_tools()
    .tool(tool)
    .compaction(if compact {
        CompactionPolicy::default()
    } else {
        CompactionPolicy::Disabled
    });
    let harness = LocalHarness::open(config).await?;
    timing
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .resources = resources::Samples::from_arguments()?;
    let result = harness.run("Run the deterministic tool sequence").await;
    if let Err(error) = &result {
        let timing = timing.lock().unwrap_or_else(PoisonError::into_inner);
        eprintln!(
            "runtime failed after_turn={} summaries={} maximum_request_bytes={}: {error}",
            timing.intervals.len(),
            timing.summaries,
            timing.maximum_request_bytes
        );
    }
    let output = result?;
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
