//! Interrupted head turns settle or resume deterministically after reopen.
use super::*;
use crate::TaskId;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Blocking {
    executed: Arc<AtomicUsize>,
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ToolExecutor for Blocking {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.executed.fetch_add(1, Ordering::SeqCst);
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        Box::pin(futures::future::pending())
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn blocking(executed: &Arc<AtomicUsize>) -> (Tool, tokio::sync::oneshot::Receiver<()>) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut tool = tool("test.blocking");
    tool.executor = Arc::new(Blocking {
        executed: Arc::clone(executed),
        started: Mutex::new(Some(sender)),
    });
    (tool, receiver)
}

fn call() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
            "blocking".into(),
            "test.blocking".into(),
            json!({}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

fn admitted(
    admission: Admission<crate::runtime::RuntimeTask<crate::executor::TurnOutput>>,
) -> TaskId {
    match admission {
        Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) => task_id,
        Admission::Rejected { reason } => panic!("rejected: {reason}"),
        _ => panic!("not durably admitted"),
    }
}

fn rejected(
    admission: &Admission<crate::runtime::RuntimeTask<crate::executor::TurnOutput>>,
) -> bool {
    matches!(admission, Admission::Rejected { reason } if reason == "local agent already has an admitted turn")
}

async fn outcome(
    harness: &LocalHarness,
    task: TaskId,
) -> Option<crate::Outcome<serde_json::Value>> {
    harness.runtime.task_host().outcome(task).await.unwrap()
}

struct Killed {
    directory: tempfile::TempDir,
    project: VolumeRef,
    executed: Arc<AtomicUsize>,
    provider: Arc<ScriptedModelProvider>,
    task: TaskId,
}

impl Killed {
    async fn reopen(&self) -> Result<LocalHarness> {
        let (tool, _) = blocking(&self.executed);
        LocalHarness::open(
            volume_config(
                self.directory.path(),
                self.project.clone(),
                self.provider.clone(),
            )?
            .tools(vec![tool]),
        )
        .await
    }
}

/// Kills the owning process while the first turn's tool effect is in flight.
async fn killed_mid_tool() -> Result<Killed> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let executed = Arc::new(AtomicUsize::new(0));
    let provider = scripted([call(), ScriptedTurn::Text("after".into())]);
    let (tool, started) = blocking(&executed);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project.clone(), provider.clone())?.tools(vec![tool]),
    )
    .await?;
    let task = admitted(harness.submit(OperationId::new(), "start").await?);
    match futures::future::select(harness.resume(task), started).await {
        futures::future::Either::Right((Ok(()), execution)) => drop(execution),
        _ => panic!("no tool boundary"),
    }
    drop(harness);
    Ok(Killed {
        directory,
        project,
        executed,
        provider,
        task,
    })
}

#[tokio::test]
async fn next_prompt_settles_a_killed_tool_turn_without_running_it_again() -> Result<()> {
    let killed = killed_mid_tool().await?;
    let reopened = killed.reopen().await?;
    // Negative control: the reopened head is unsettled, which bricked admission.
    assert_eq!(outcome(&reopened, killed.task).await, None);
    let next = admitted(reopened.submit(OperationId::new(), "next").await?);
    assert_eq!(
        outcome(&reopened, killed.task).await,
        Some(crate::Outcome::Cancelled)
    );
    assert_eq!(reopened.resume(next).await?.text, "after");
    assert!(matches!(
        reopened.resume(killed.task).await,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled
        ))
    ));
    assert_eq!(killed.executed.load(Ordering::SeqCst), 1, "tool ran again");
    assert_eq!(killed.provider.requests().len(), 2, "model reissued");
    Ok(())
}

#[tokio::test]
async fn a_driven_turn_is_never_superseded_until_its_driver_is_gone() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let executed = Arc::new(AtomicUsize::new(0));
    let provider = scripted([call(), ScriptedTurn::Text("after".into())]);
    let (tool, started) = blocking(&executed);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?.tools(vec![tool]),
    )
    .await?;
    // The submitter's claim, as `execute` takes it, owns the head from
    // before admission until `resume` takes its own.
    let start = OperationId::new();
    let claim = harness.driver_claim(start).await;
    let task = admitted(harness.submit(start, "start").await?);
    assert!(rejected(
        &harness.submit(OperationId::new(), "early").await?
    ));
    drop(claim);
    let mut driving = harness.resume(task);
    match futures::future::select(driving.as_mut(), started).await {
        futures::future::Either::Right((Ok(()), _)) => {}
        _ => panic!("no tool boundary"),
    }
    assert!(rejected(
        &harness.submit(OperationId::new(), "while driven").await?
    ));
    assert_eq!(outcome(&harness, task).await, None);
    // Dropping the driver in-process orphans the turn exactly like a kill.
    drop(driving);
    let next = admitted(harness.submit(OperationId::new(), "next").await?);
    assert_eq!(
        outcome(&harness, task).await,
        Some(crate::Outcome::Cancelled)
    );
    assert_eq!(harness.resume(next).await?.text, "after");
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}

/// Reports its effect as uncertain, as a tool whose transport failed mid-call.
struct Uncertain(Arc<AtomicUsize>);

impl ToolExecutor for Uncertain {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(Error::Indeterminate(OperationId::new())) })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn an_indeterminate_resume_releases_its_reservation_to_the_next_prompt() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let executed = Arc::new(AtomicUsize::new(0));
    let mut uncertain = tool("test.blocking");
    uncertain.executor = Arc::new(Uncertain(Arc::clone(&executed)));
    let provider = scripted([call(), ScriptedTurn::Text("next".into())]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?.tools(vec![uncertain]),
    )
    .await?;
    let task = admitted(harness.submit(OperationId::new(), "start").await?);
    // The driver returns, leaving the turn Indeterminate with its reservation
    // retained for reconciliation.
    assert!(matches!(
        harness.resume(task).await,
        Err(Error::Indeterminate(_))
    ));
    assert!(matches!(
        outcome(&harness, task).await,
        Some(crate::Outcome::Indeterminate { .. })
    ));
    let next = admitted(harness.submit(OperationId::new(), "next").await?);
    assert_eq!(
        outcome(&harness, task).await,
        Some(crate::Outcome::Cancelled)
    );
    // With concurrency 1 this completes only if settlement released the reservation.
    assert_eq!(harness.resume(next).await?.text, "next");
    assert_eq!(
        executed.load(Ordering::SeqCst),
        1,
        "uncertain effect ran again"
    );
    assert_eq!(provider.requests().len(), 2, "model reissued");
    Ok(())
}

/// One entry point into a head's admission or driving, cancelled by a cut.
type Entry = for<'a> fn(
    &'a LocalHarness,
    OperationId,
) -> std::pin::Pin<
    Box<dyn Future<Output = Result<crate::executor::TurnOutput>> + 'a>,
>;

fn run_entry(
    harness: &LocalHarness,
    operation: OperationId,
) -> std::pin::Pin<Box<dyn Future<Output = Result<crate::executor::TurnOutput>> + '_>> {
    Box::pin(harness.run_as(operation, "start"))
}

fn resume_entry(
    harness: &LocalHarness,
    operation: OperationId,
) -> std::pin::Pin<Box<dyn Future<Output = Result<crate::executor::TurnOutput>> + '_>> {
    Box::pin(async move {
        // Admission alone leaves the head undriven; `resume` takes it.
        let task = admitted(harness.submit(operation, "start").await?);
        harness.resume(task).await
    })
}

/// The state an await of `entry` leaves behind if cancelled there: whether
/// the entry holds its claim, and where its head's publication stands.
type Class = (bool, u8);

/// One await, addressed by its class and its ordinal among that class's
/// awaits. Await indices are not stable across runs, since an I/O
/// completion can take one poll or several, but classes and their order are.
type Await = (Class, usize);

fn class(harness: &LocalHarness, operation: OperationId) -> Class {
    (
        !harness.drivers.orphaned(operation),
        super::super::admission::publication_phase::get(),
    )
}

/// Whether `class` ends the swept handoff: the head is admitted and its
/// driver holds the claim. Every later await is inside that one claim
/// binding, whose drop orphans the head
/// (`a_driven_turn_is_never_superseded_until_its_driver_is_gone`, and the
/// `orphans` loom models).
fn driven(class: Class) -> bool {
    class == (true, super::super::admission::publication_phase::ADMITTED)
}

/// Polls `entry` through the handoff, addressing each await. `stop` is
/// asked at each await; when it answers true the future is dropped there.
/// Returns the awaits seen, or `None` if the entry finished first.
async fn step(
    harness: &LocalHarness,
    entry: Entry,
    operation: OperationId,
    mut stop: impl FnMut(Await) -> bool,
) -> Result<Option<Vec<Await>>> {
    use super::super::admission::publication_phase;
    publication_phase::set(publication_phase::BEFORE);
    let mut future = entry(harness, operation);
    let mut seen: Vec<Await> = Vec::new();
    let output = std::future::poll_fn(|context| match future.as_mut().poll(context) {
        std::task::Poll::Ready(output) => std::task::Poll::Ready(Some(output)),
        std::task::Poll::Pending => {
            let class = class(harness, operation);
            let ordinal = seen.iter().filter(|(seen, _)| *seen == class).count();
            seen.push((class, ordinal));
            if stop((class, ordinal)) {
                std::task::Poll::Ready(None)
            } else {
                std::task::Poll::Pending
            }
        }
    })
    .await;
    match output {
        Some(output) => {
            assert_eq!(output?.text, "done");
            Ok(None)
        }
        None => Ok(Some(seen)),
    }
}

/// The awaits that represent every class of the handoff: the first await of
/// each class, and every await inside the head's request write, where the
/// head turns durable at a step the class cannot see. Publication only
/// advances, so each class is one interval.
fn representatives(trace: &[Await]) -> Vec<Await> {
    use super::super::admission::publication_phase::WRITING;
    assert!(
        trace.windows(2).all(|pair| pair[0].0.1 <= pair[1].0.1),
        "publication went backwards: {trace:?}"
    );
    trace
        .iter()
        .copied()
        .filter(|((_, phase), ordinal)| *ordinal == 0 || *phase == WRITING)
        .collect()
}

/// Cancels `entry` at `target` and asks whether a new prompt is then
/// admitted. With `leak`, a claim taken before the entry outlives the cut,
/// as the former submission mark did, until the probe has answered. Returns
/// whether the probe was refused, or `None` where this run's handoff never
/// reached `target`: a write that completed in fewer polls.
async fn handoff_cut(
    harness: &LocalHarness,
    entry: Entry,
    target: Await,
    leak: bool,
) -> Result<Option<bool>> {
    let operation = OperationId::new();
    // Taken without an await, so both sweeps cut the entry identically.
    let leaked = leak.then(|| harness.drivers.drive(operation));
    let seen = step(harness, entry, operation, |current| {
        current == target || driven(current.0)
    })
    .await?
    .expect("the entry finished inside its handoff");
    let reached = seen.last() == Some(&target);
    let refused = rejected(&harness.submit(OperationId::new(), "probe").await?);
    drop(leaked);
    Ok(reached.then_some(refused))
}

/// Cancels `entry` at a representative of every class of await in its
/// handoff, from before admission until its driver holds the admitted head,
/// and after each cut asks whether a new prompt is admitted; the negative
/// control leaks a claim across the same cut. One stepped run classifies the
/// handoff, so the sweep costs that run plus two cuts per representative,
/// each on a fresh host. No cut refuses the probe; the negative control is
/// refused at every cut once the head is admitted (a written but unadmitted
/// head is a settled request), so each class of suspension past admission is
/// shown to need the RAII claim.
async fn every_handoff_cut_leaves_the_head_to_the_next_prompt(entry: Entry) -> Result<()> {
    use super::super::admission::publication_phase::{ADMITTED, BEFORE};
    // Every run starts from the same state: a fresh host whose one previous
    // turn has settled.
    async fn fresh() -> Result<(tempfile::TempDir, LocalHarness)> {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider =
            scripted(std::iter::repeat_with(|| ScriptedTurn::Text("done".into())).take(4));
        let harness =
            LocalHarness::open(volume_config(directory.path(), project, provider)?).await?;
        assert_eq!(harness.run("settle").await?.text, "done");
        Ok((directory, harness))
    }
    let trace = {
        let (_directory, harness) = fresh().await?;
        step(&harness, entry, OperationId::new(), |current| {
            driven(current.0)
        })
        .await?
        .expect("the entry finished inside its handoff")
    };
    assert!(
        trace.iter().any(|((_, phase), _)| *phase == BEFORE),
        "no cut before admission"
    );
    let targets = representatives(&trace);
    let mut covered = std::collections::BTreeSet::new();
    let mut refusals = Vec::new();
    for target in targets {
        let (_directory, harness) = fresh().await?;
        let Some(refused) = handoff_cut(&harness, entry, target, false).await? else {
            continue;
        };
        assert!(!refused, "cut {target:?} bricked the host");
        assert_eq!(harness.run("after").await?.text, "done");
        let (_directory, harness) = fresh().await?;
        let Some(refused) = handoff_cut(&harness, entry, target, true).await? else {
            continue;
        };
        if target.0.1 == ADMITTED {
            assert!(refused, "cut {target:?} escaped the negative control");
        }
        covered.insert(target.0);
        refusals.push(refused);
    }
    assert_eq!(
        covered,
        trace.iter().map(|(class, _)| *class).collect(),
        "a class of the handoff was never cut"
    );
    // The head is admitted at one step of the handoff, and stays so.
    let first = refusals.iter().position(|refused| *refused);
    assert!(
        first.is_some_and(|first| refusals[first..].iter().all(|refused| *refused)),
        "the negative control was not monotone: {refusals:?}"
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_a_run_at_any_handoff_await_leaves_its_head_to_the_next_prompt() -> Result<()> {
    every_handoff_cut_leaves_the_head_to_the_next_prompt(run_entry).await
}

#[tokio::test]
async fn cancelling_a_resume_at_any_await_leaves_its_head_to_the_next_prompt() -> Result<()> {
    every_handoff_cut_leaves_the_head_to_the_next_prompt(resume_entry).await
}

#[tokio::test]
async fn a_retried_admission_of_the_unsettled_head_is_not_superseded() -> Result<()> {
    let killed = killed_mid_tool().await?;
    let reopened = killed.reopen().await?;
    let operation = OperationId::from_bytes(killed.task.into_bytes());
    // The retry reconciles the existing admission, and the claim its driver
    // holds across admission, as `run` and command execution do, owns the
    // head again.
    let _driving = reopened.driver_claim(operation).await;
    assert_eq!(
        admitted(reopened.submit(operation, "start").await?),
        killed.task
    );
    assert!(rejected(
        &reopened.submit(OperationId::new(), "next").await?
    ));
    // Control: next_prompt_settles_a_killed_tool_turn_without_running_it_again
    // supersedes this same head when no retry claims it.
    assert_eq!(outcome(&reopened, killed.task).await, None);
    assert_eq!(killed.executed.load(Ordering::SeqCst), 1);
    Ok(())
}
