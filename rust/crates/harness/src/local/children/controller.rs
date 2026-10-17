use super::*;
use crate::{
    filesystem::TaskCommandHost,
    runtime::TaskStateProvider,
    task_volume::{
        Change, ChildFile, ChildRequest, ChildState, ChildStatus, Component, Control, FileKey,
        FileQuerySource, FileValue, Generation, Query, TaskFiles,
    },
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use std::collections::{BTreeMap, BTreeSet};

type Files = TaskFiles<dyn StreamProvider>;

mod results;
mod stopping;

impl LocalHarness {
    pub(in crate::local) fn advance_with_children<'a>(
        &'a self,
        task: TaskId,
        commands: &'a dyn TaskCommandHost,
        files: Arc<Files>,
        control: Option<(Arc<Files>, Component)>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut observation = files
                .observe_files(
                    Query::Children,
                    None,
                    self.limits.attachments,
                    usize::try_from(self.limits.render_bytes).map_err(storage)?,
                )
                .await
                .map_err(storage)?;
            let snapshot = observation
                .snapshot
                .take()
                .ok_or_else(|| Error::Conflict("child folder snapshot missing".into()))?;
            let mut generation = snapshot.generation;
            let mut values: BTreeMap<_, _> = snapshot
                .files
                .into_iter()
                .map(|file| (file.key.clone(), file))
                .collect();
            let mut active = BTreeSet::new();
            let mut completions = BTreeMap::new();
            let mut jobs = FuturesUnordered::new();
            let inherited = commands.invocation_cancellation().cloned();
            let mut inherited_stopped = false;
            for request in values.values().filter(|file| {
                matches!(
                    file.key,
                    FileKey::Child {
                        file: ChildFile::Request,
                        ..
                    }
                )
            }) {
                let FileKey::Child { reference, .. } = &request.key else {
                    unreachable!()
                };
                if needs_work(reference, &values, false)? {
                    active.insert(reference.clone());
                    jobs.push(self.service_child(
                        files.clone(),
                        reference.clone(),
                        request.clone(),
                        inherited.clone(),
                    ));
                }
            }
            let parent = self.advance_while(task, commands, || async {
                if commands
                    .invocation_cancellation()
                    .is_some_and(|token| token.is_cancelled())
                {
                    return Ok(false);
                }
                match &control {
                    None => Ok(true),
                    Some((files, reference)) => Ok(super::commands::state(
                        files,
                        reference,
                        usize::try_from(self.limits.render_bytes).map_err(storage)?,
                    )
                    .await?
                        == ChildState::Running),
                }
            });
            futures::pin_mut!(parent);
            let mut parent_done = false;
            loop {
                if !inherited_stopped
                    && inherited.as_ref().is_some_and(|token| token.is_cancelled())
                {
                    inherited_stopped = true;
                    for request in values.values().filter(|file| {
                        matches!(
                            file.key,
                            FileKey::Child {
                                file: ChildFile::Request,
                                ..
                            }
                        )
                    }) {
                        let FileKey::Child { reference, .. } = &request.key else {
                            unreachable!()
                        };
                        if !active.contains(reference)
                            && !matches!(
                                desired(reference, &values)?,
                                ChildState::Cancelled | ChildState::Archived
                            )
                        {
                            active.insert(reference.clone());
                            jobs.push(self.service_child(
                                files.clone(),
                                reference.clone(),
                                request.clone(),
                                inherited.clone(),
                            ));
                        }
                    }
                }
                if parent_done && jobs.is_empty() && completions.is_empty() {
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    change = observation.changes.next() => {
                        let change = change.ok_or_else(|| Error::Conflict("child folder follow ended".into()))?.map_err(storage)?;
                        let mut changed = BTreeSet::new();
                        for file in change.file.iter().chain(&change.files) {
                            if let FileKey::Child { reference, file: ChildFile::Request | ChildFile::Inbox(_) | ChildFile::Control } = &file.key {
                                changed.insert(reference.clone());
                            }
                        }
                        advance_fold(&mut values, &mut generation, change);
                        changed.extend(release_completed(generation.position, &mut active, &mut completions));
                        for reference in changed {
                            if active.contains(&reference) || !needs_work(&reference, &values, inherited_stopped)? { continue; }
                            let key = FileKey::Child { reference: reference.clone(), file: ChildFile::Request };
                            let request = values.get(&key).ok_or_else(|| Error::Conflict("child input has no request".into()))?.clone();
                            active.insert(reference.clone());
                            jobs.push(self.service_child(files.clone(), reference, request, inherited.clone()));
                        }
                    }
                    result = jobs.next(), if !jobs.is_empty() => {
                        let (reference, position) = result.ok_or_else(|| Error::Conflict("child worker disappeared".into()))??;
                        completions.insert(reference, position);
                        for reference in release_completed(generation.position, &mut active, &mut completions) {
                        if needs_work(&reference, &values, inherited_stopped)? {
                            let key = FileKey::Child { reference: reference.clone(), file: ChildFile::Request };
                            let request = values.get(&key).ok_or_else(|| Error::Conflict("child has no request".into()))?.clone();
                            active.insert(reference.clone());
                            jobs.push(self.service_child(files.clone(), reference, request, inherited.clone()));
                        }
                        }
                    }
                    _ = async { inherited.as_ref().unwrap().cancelled().await },
                        if inherited.is_some() && !inherited_stopped => {}
                    result = &mut parent, if !parent_done => { result?; parent_done = true; }
                }
            }
        })
    }

    pub(super) fn service_child<'a>(
        &'a self,
        files: Arc<Files>,
        reference: Component,
        request: FileValue,
        inherited: Option<acyclic_fs::CancellationToken>,
    ) -> BoxProviderFuture<'a, Result<(Component, u64)>> {
        Box::pin(async move {
            let mut observation = files
                .observe_files(
                    Query::Child(reference.clone()),
                    None,
                    self.limits.attachments,
                    usize::try_from(self.limits.render_bytes).map_err(storage)?,
                )
                .await
                .map_err(storage)?;
            let snapshot = observation
                .snapshot
                .take()
                .ok_or_else(|| Error::Conflict("child snapshot missing".into()))?;
            let mut generation = snapshot.generation;
            let mut values: BTreeMap<_, _> = snapshot
                .files
                .into_iter()
                .map(|file| (file.key.clone(), file))
                .collect();
            let mut inherited_stopped =
                inherited.as_ref().is_some_and(|token| token.is_cancelled());
            if inherited_stopped {
                stopping::cancel(
                    files.as_ref(),
                    &reference,
                    &mut observation,
                    &mut values,
                    &mut generation,
                )
                .await?;
            }
            if !needs_work(&reference, &values, inherited_stopped)? {
                return Ok((reference, generation.position));
            }
            let (child, _view) = self.admit_child_request(files.as_ref(), &request).await?;
            let parsed = ChildRequest::parse(&request.bytes, self.limits.file_bytes)?;
            let mut model = self.model.clone();
            if let Some(name) = parsed.model {
                model.name = name;
                model.validate()?;
            }
            let commands = self.runtime.commands().with_model(
                &model,
                self.provider.clone(),
                &self.compaction,
                None,
            )?;
            loop {
                while let Some(next) = observation.changes.next().now_or_never() {
                    let change = next
                        .ok_or_else(|| Error::Conflict("child follow ended".into()))?
                        .map_err(storage)?;
                    advance_fold(&mut values, &mut generation, change);
                }
                let state = desired(&reference, &values)?;
                let previous = execution(&reference, &values)?;
                let inputs = input_count(&reference, &values);
                let cancelled = matches!(state, ChildState::Cancelled | ChildState::Archived)
                    && matches!(
                        self.runtime.task_host().outcome(child).await?,
                        Some(crate::Outcome::Cancelled)
                    );
                let suspension = if cancelled
                    || (state == ChildState::Running
                        && previous.is_some_and(|status| {
                            status.settled && status.inputs == inputs && status.task == child
                        })) {
                    let host = self.runtime.task_host();
                    if cancelled {
                        host.cancelled_workflow_suspension(child, &mut None).await?
                    } else {
                        host.workflow_suspension(child, &mut None).await?
                    }
                } else {
                    None
                };
                let idle_revision = suspension
                    .as_ref()
                    .filter(|slot| slot.waiting_command.is_some() && slot.input.is_none())
                    .map(|slot| slot.revision);
                let cancelled_checkpoint = cancelled
                    && stopping::cancelled_turn_is_quiet(
                        previous,
                        child,
                        results::published(&reference, &values),
                        suspension.as_ref(),
                    );
                if cancelled && !cancelled_checkpoint {
                    // The host may be newer than this follow. Consume its accepted
                    // result prefix before deciding whether the receive cut agrees.
                    let head = files.inner.head().await.map_err(storage)?;
                    while generation.position < head.position {
                        let change = observation
                            .changes
                            .next()
                            .await
                            .ok_or_else(|| Error::Conflict("cancelled child follow ended".into()))?
                            .map_err(storage)?;
                        advance_fold(&mut values, &mut generation, change);
                    }
                    if !stopping::cancelled_turn_is_quiet(
                        execution(&reference, &values)?,
                        child,
                        results::published(&reference, &values),
                        suspension.as_ref(),
                    ) {
                        return Err(Error::Indeterminate(OperationId::from_bytes(
                            child.into_bytes(),
                        )));
                    }
                    continue;
                }
                let passive_control = !cancelled
                    && state != ChildState::Running
                    && (previous.is_none() || !stopping::requested(state))
                    && previous.is_none_or(|status| status.settled)
                    && matches!(
                        self.runtime
                            .task_host()
                            .recover_work(child, &mut None)
                            .await?,
                        crate::distributed::WorkPull::Idle
                    );
                if cancelled_checkpoint
                    || passive_control
                    || (settled_turn(previous, child, inputs, idle_revision)
                        && values.contains_key(&FileKey::Child {
                            reference: reference.clone(),
                            file: ChildFile::Result(inputs),
                        }))
                {
                    if state != ChildState::Running {
                        if !cancelled
                            && matches!(state, ChildState::Cancelled | ChildState::Archived)
                        {
                            self.runtime.task_host().cancel(child).await?;
                            if !matches!(
                                self.runtime.task_host().outcome(child).await?,
                                Some(crate::Outcome::Cancelled)
                            ) {
                                return Err(Error::Indeterminate(OperationId::from_bytes(
                                    child.into_bytes(),
                                )));
                            }
                        }
                    }
                    let status = ChildStatus {
                        task: child,
                        state,
                        settled: true,
                        inputs: previous.map_or(1, |status| status.inputs),
                    };
                    if publish(
                        files.as_ref(),
                        &reference,
                        status,
                        &mut values,
                        &mut generation,
                    )
                    .await?
                    {
                        return Ok((reference, generation.position));
                    }
                    let change = observation
                        .changes
                        .next()
                        .await
                        .ok_or_else(|| Error::Conflict("child follow ended".into()))?
                        .map_err(storage)?;
                    advance_fold(&mut values, &mut generation, change);
                    continue;
                }
                if state == ChildState::Running {
                    self.deliver_child_inputs(files.as_ref(), &reference, child, &request, &values)
                        .await?;
                }
                let status = ChildStatus {
                    task: child,
                    state,
                    settled: false,
                    inputs,
                };
                if state == ChildState::Running
                    && !publish(
                        files.as_ref(),
                        &reference,
                        status,
                        &mut values,
                        &mut generation,
                    )
                    .await?
                {
                    let change = observation
                        .changes
                        .next()
                        .await
                        .ok_or_else(|| Error::Conflict("child follow ended".into()))?
                        .map_err(storage)?;
                    advance_fold(&mut values, &mut generation, change);
                    continue;
                }
                #[cfg(all(test, not(acyclic_harness_loom)))]
                super::cuts::stop("status", OperationId::from_bytes(child.into_bytes()))?;
                let (child_files, context) = self.child_files(child, files.as_ref()).await?;
                let journal = self.runtime.workflow_journal_with_context(&context)?;
                drop(context);
                let cancellation = acyclic_fs::CancellationToken::new();
                if stopping::requested(state) {
                    cancellation.cancel();
                }
                let child_commands = super::commands::Commands {
                    harness: self,
                    inner: &commands,
                    files: files.as_ref(),
                    reference: &reference,
                    cancellation: cancellation.clone(),
                };
                {
                    let advance = self.advance_with_children(
                        child,
                        &child_commands,
                        child_files,
                        Some((files.clone(), reference.clone())),
                    );
                    let controls = async {
                        loop {
                            tokio::select! {
                                biased;
                                change = observation.changes.next() => {
                                    let change = change.ok_or_else(|| Error::Conflict("child follow ended".into()))?.map_err(storage)?;
                                    let input = change.file.iter().chain(&change.files).any(|file| matches!(file.key,
                                        FileKey::Child { file: ChildFile::Request | ChildFile::Inbox(_), .. }));
                                    advance_fold(&mut values, &mut generation, change);
                                    let state = desired(&reference, &values)?;
                                    if stopping::requested(state) { cancellation.cancel(); }
                                    if input && state == ChildState::Running { self.deliver_child_inputs(files.as_ref(), &reference, child, &request, &values).await?; }
                                }
                                _ = async { inherited.as_ref().unwrap().cancelled().await },
                                    if inherited.is_some() && !inherited_stopped => {
                                    cancellation.cancel();
                                    stopping::cancel(files.as_ref(), &reference, &mut observation, &mut values, &mut generation).await?;
                                    inherited_stopped = true;
                                }
                            }
                        }
                    };
                    stopping::drain(advance, controls, &cancellation).await?;
                }
                let (processed, results) = self
                    .child_processed(child, journal.as_ref(), &reference, &values)
                    .await?;
                for file in results {
                    loop {
                        if values.get(&file.key) == Some(&file) {
                            break;
                        }
                        match files.inner.append(generation, &file).await {
                            Ok(next) => {
                                generation = next;
                                values.insert(file.key.clone(), file.clone());
                                break;
                            }
                            Err(crate::task_volume::Error::Conflict(_)) => {
                                let change = observation
                                    .changes
                                    .next()
                                    .await
                                    .ok_or_else(|| {
                                        Error::Conflict("child result follow ended".into())
                                    })?
                                    .map_err(storage)?;
                                advance_fold(&mut values, &mut generation, change);
                            }
                            Err(error) => return Err(storage(error)),
                        }
                    }
                }
                let state = desired(&reference, &values)?;
                if state == ChildState::Running && processed < input_count(&reference, &values) {
                    continue;
                }
                if matches!(state, ChildState::Cancelled | ChildState::Archived) {
                    self.runtime.task_host().cancel(child).await?;
                    if !matches!(
                        self.runtime.task_host().outcome(child).await?,
                        Some(crate::Outcome::Cancelled)
                    ) {
                        return Err(Error::Indeterminate(OperationId::from_bytes(
                            child.into_bytes(),
                        )));
                    }
                }
                let status = ChildStatus {
                    task: child,
                    state,
                    settled: true,
                    inputs: if state == ChildState::Running {
                        processed
                    } else {
                        previous.map_or(inputs, |status| status.inputs)
                    },
                };
                if publish(
                    files.as_ref(),
                    &reference,
                    status,
                    &mut values,
                    &mut generation,
                )
                .await?
                {
                    return Ok((reference, generation.position));
                }
                let change = observation
                    .changes
                    .next()
                    .await
                    .ok_or_else(|| Error::Conflict("child follow ended".into()))?
                    .map_err(storage)?;
                advance_fold(&mut values, &mut generation, change);
            }
        })
    }

    async fn deliver_child_inputs(
        &self,
        files: &Files,
        reference: &Component,
        child: TaskId,
        request: &FileValue,
        values: &BTreeMap<FileKey, FileValue>,
    ) -> Result<()> {
        let sender = request
            .requester
            .ok_or_else(|| Error::Unauthorized("child request has no sender".into()))?;
        let publisher = self.input_publisher_with_limit(self.limits.file_bytes)?;
        let prompt = ChildRequest::parse(&request.bytes, self.limits.file_bytes)?.prompt;
        for file in std::iter::once(request).chain(values.values().filter(|file| matches!(&file.key,
            FileKey::Child { reference: candidate, file: ChildFile::Inbox(_) } if candidate == reference))) {
            let operation = admission::child_operation(files.volume(), &file.key.path(), b"mail")?;
            let body = if file.key == request.key { prompt.as_bytes() } else { file.bytes.as_ref() };
            let (payload, _) = publisher.stage_once(operation, &format!("inputs/agents/{child}/{operation}.md"),
                body, "text/plain", "message.md", None)?.await?;
            self.runtime.task_host().send(sender, child, operation, payload).await?;
        }
        Ok(())
    }

    async fn child_files(
        &self,
        child: TaskId,
        parent: &Files,
    ) -> Result<(Arc<Files>, crate::runtime::TaskContext)> {
        use crate::distributed::{WorkPull, Worker};
        let host = self.runtime.task_host();
        let lease = match host.recover_work(child, &mut None).await? {
            WorkPull::Claimed(lease) => lease,
            WorkPull::Idle => {
                if !self.runtime.poll_task_wake(child, None).await? {
                    if let Some(slot) = host.workflow_suspension(child, &mut None).await?
                        && let Some(waiting) = slot.waiting_command
                    {
                        host.wake_command(child, slot.revision, waiting, &mut None)
                            .await?;
                    }
                }
                let worker = Worker {
                    id: "child-file-controller".into(),
                    available: Default::default(),
                    labels: Default::default(),
                };
                let WorkPull::Claimed(lease) = host
                    .pull_work(
                        &worker,
                        Some(OperationId::from_bytes(child.into_bytes())),
                        &mut None,
                    )
                    .await?
                else {
                    return Err(Error::Indeterminate(OperationId::from_bytes(
                        child.into_bytes(),
                    )));
                };
                lease
            }
            WorkPull::Unresolved { error, .. } => return Err(error),
        };
        host.start_task(&lease, &mut None).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let context = self.runtime.task_context(child, fence.clone()).await?;
        let files = parent
            .for_created_task(&context, context.scope())
            .await?
            .ok_or_else(|| Error::Unauthorized("child has no created private files".into()))?;
        Ok((Arc::new(files), context))
    }
}

fn fold(values: &mut BTreeMap<FileKey, FileValue>, change: Change) {
    for key in change.removed {
        values.remove(&key);
    }
    for file in change.file.into_iter().chain(change.files) {
        values.insert(file.key.clone(), file);
    }
}

pub(super) fn advance_fold(
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
    change: Change,
) {
    if change.cursor.position <= generation.position {
        return;
    }
    *generation = Generation {
        volume: change.cursor.volume,
        epoch: change.cursor.epoch,
        position: change.cursor.position,
        commit: change.cursor.commit,
    };
    fold(values, change);
}

fn desired(reference: &Component, values: &BTreeMap<FileKey, FileValue>) -> Result<ChildState> {
    values
        .get(&FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Control,
        })
        .map(|file| {
            Control::parse(&file.bytes)
                .map(|control| ChildState::Running.apply(control))
                .map_err(storage)
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

fn execution(
    reference: &Component,
    values: &BTreeMap<FileKey, FileValue>,
) -> Result<Option<ChildStatus>> {
    values
        .get(&FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Status,
        })
        .map(|file| ChildStatus::parse(&file.bytes).map_err(storage))
        .transpose()
}

fn input_count(reference: &Component, values: &BTreeMap<FileKey, FileValue>) -> u64 {
    values
        .keys()
        .filter(|key| {
            matches!(key, FileKey::Child { reference: candidate,
        file: ChildFile::Request | ChildFile::Inbox(_) } if candidate == reference)
        })
        .count() as u64
}

fn settled_turn(
    previous: Option<ChildStatus>,
    child: TaskId,
    inputs: u64,
    idle_revision: Option<u64>,
) -> bool {
    // Initial receive is revision one. Each completed receive/model pair adds
    // two revisions; a pause before the first message has no completed pair.
    let expected = inputs.checked_mul(2).and_then(|value| value.checked_add(1));
    if expected.is_none() || idle_revision != expected {
        return false;
    }
    previous.is_some_and(|status| {
        status.task == child
            && status.settled
            && status.inputs == inputs
            && matches!(
                status.state,
                ChildState::Running | ChildState::Paused | ChildState::Interrupted
            )
    })
}

fn needs_work(
    reference: &Component,
    values: &BTreeMap<FileKey, FileValue>,
    stopping: bool,
) -> Result<bool> {
    let state = desired(reference, values)?;
    if stopping && !matches!(state, ChildState::Cancelled | ChildState::Archived) {
        return Ok(true);
    }
    Ok(execution(reference, values)?.is_none_or(|status| {
        !status.settled
            || status.state != state
            || state == ChildState::Running
                && (status.inputs != input_count(reference, values)
                    || !values.contains_key(&FileKey::Child {
                        reference: reference.clone(),
                        file: ChildFile::Result(status.inputs),
                    }))
    }))
}

// Keep a worker active until the same file follow has observed its accepted
// completion. Mixing a callback's newer status with an older control snapshot
// can reschedule stale work or consume an inbox wakeup.
fn release_completed(
    position: u64,
    active: &mut BTreeSet<Component>,
    completions: &mut BTreeMap<Component, u64>,
) -> BTreeSet<Component> {
    let ready: BTreeSet<_> = completions
        .iter()
        .filter(|(_, accepted)| **accepted <= position)
        .map(|(reference, _)| reference.clone())
        .collect();
    for reference in &ready {
        completions.remove(reference);
        active.remove(reference);
    }
    ready
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

async fn publish(
    files: &Files,
    reference: &Component,
    status: ChildStatus,
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
) -> Result<bool> {
    let file = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Status,
        },
        serde_json::to_vec(&status).map_err(storage)?,
    );
    match files.inner.append(*generation, &file).await {
        Ok(next) => {
            *generation = next;
            values.insert(file.key.clone(), file);
            Ok(true)
        }
        Err(crate::task_volume::Error::Conflict(_)) => Ok(false),
        Err(error) => Err(storage(error)),
    }
}
