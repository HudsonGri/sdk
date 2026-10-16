use super::*;
use crate::{
    filesystem::{FilesystemWorkflowJournal, TaskCommandHost},
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

impl LocalHarness {
    pub(in crate::local) fn advance_with_children<'a>(
        &'a self,
        task: TaskId,
        commands: &'a dyn TaskCommandHost,
        files: Arc<Files>,
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
                if needs_work(reference, &values)? {
                    active.insert(reference.clone());
                    jobs.push(self.service_child(
                        files.clone(),
                        reference.clone(),
                        request.clone(),
                    ));
                }
            }
            let parent = self.advance(task, commands);
            futures::pin_mut!(parent);
            let mut parent_done = false;
            loop {
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
                            if active.contains(&reference) || !needs_work(&reference, &values)? { continue; }
                            let key = FileKey::Child { reference: reference.clone(), file: ChildFile::Request };
                            let request = values.get(&key).ok_or_else(|| Error::Conflict("child input has no request".into()))?.clone();
                            active.insert(reference.clone());
                            jobs.push(self.service_child(files.clone(), reference, request));
                        }
                    }
                    result = jobs.next(), if !jobs.is_empty() => {
                        let (reference, position) = result.ok_or_else(|| Error::Conflict("child worker disappeared".into()))??;
                        completions.insert(reference, position);
                        for reference in release_completed(generation.position, &mut active, &mut completions) {
                        if needs_work(&reference, &values)? {
                            let key = FileKey::Child { reference: reference.clone(), file: ChildFile::Request };
                            let request = values.get(&key).ok_or_else(|| Error::Conflict("child has no request".into()))?.clone();
                            active.insert(reference.clone());
                            jobs.push(self.service_child(files.clone(), reference, request));
                        }
                        }
                    }
                    result = &mut parent, if !parent_done => { result?; parent_done = true; }
                }
            }
        })
    }

    fn service_child<'a>(
        &'a self,
        files: Arc<Files>,
        reference: Component,
        request: FileValue,
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
            if !needs_work(&reference, &values)? {
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
                if state != ChildState::Running {
                    if !matches!(
                        self.runtime
                            .task_host()
                            .recover_work(child, &mut None)
                            .await?,
                        crate::distributed::WorkPull::Idle
                    ) {
                        return Err(Error::Unsupported(
                            "child control requires active-effect reconciliation".into(),
                        ));
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
                self.deliver_child_inputs(files.as_ref(), &reference, child, &request, &values)
                    .await?;
                let status = ChildStatus {
                    task: child,
                    state,
                    settled: false,
                    inputs,
                };
                if !publish(
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
                let (child_files, context) = self.child_files(child, files.as_ref()).await?;
                let journal = self.runtime.workflow_journal_with_context(&context)?;
                drop(context);
                let advance = self.advance_with_children(child, &commands, child_files);
                futures::pin_mut!(advance);
                loop {
                    tokio::select! {
                        biased;
                        change = observation.changes.next() => {
                            let change = change.ok_or_else(|| Error::Conflict("child follow ended".into()))?.map_err(storage)?;
                            let input = change.file.iter().chain(&change.files).any(|file| matches!(file.key,
                                FileKey::Child { file: ChildFile::Request | ChildFile::Inbox(_), .. }));
                            advance_fold(&mut values, &mut generation, change);
                            let state = desired(&reference, &values)?;
                            if state != ChildState::Running {
                                return Err(Error::Unsupported("control of an active child turn requires effect reconciliation".into()));
                            }
                            if input { self.deliver_child_inputs(files.as_ref(), &reference, child, &request, &values).await?; }
                        }
                        result = &mut advance => { result?; break; }
                    }
                }
                let processed = self.child_processed(child, journal.as_ref()).await?;
                if processed < input_count(&reference, &values) {
                    continue;
                }
                let status = ChildStatus {
                    task: child,
                    state: ChildState::Running,
                    settled: true,
                    inputs: processed,
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
                self.runtime.poll_task_wake(child, None).await?;
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

    async fn child_processed(
        &self,
        child: TaskId,
        journal: &FilesystemWorkflowJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<u64> {
        if self.runtime.task_host().outcome(child).await?.is_some() {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                child.into_bytes(),
            )));
        }
        // The worker has released its live lease at the receive checkpoint.
        // Bounded observation verifies the retained original reservation proof
        // rather than trying to publish under that released execution fence.
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(65_536, self.limits.file_bytes),
            &cancellation,
        )?;
        let record = journal
            .observe_latest_record(Some(&io))
            .await?
            .ok_or_else(|| Error::Conflict("child checkpoint missing".into()))?;
        if record.transition.state["stage"].as_str() != Some("receive") {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                child.into_bytes(),
            )));
        }
        record.transition.state["mail"]["sequence"]
            .as_u64()
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| Error::Conflict("child has no settled receive cursor".into()))
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

fn advance_fold(
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

fn needs_work(reference: &Component, values: &BTreeMap<FileKey, FileValue>) -> Result<bool> {
    let state = desired(reference, values)?;
    Ok(execution(reference, values)?.is_none_or(|status| {
        !status.settled
            || status.state != state
            || state == ChildState::Running && status.inputs != input_count(reference, values)
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
