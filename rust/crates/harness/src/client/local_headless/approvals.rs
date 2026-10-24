//! Descendant discovery follows owner statuses; every edge is checked against
//! immutable admission before opening that child's private approval volume.
use super::*;
use crate::{
    TaskId,
    task_volume::{ChildFile, ChildStatus, Snapshot, TaskFiles},
};
use acyclic_stream::{BoxProviderStream, LocalStream};
use futures::stream::SelectAll;
use std::collections::{BTreeSet, VecDeque};

pub(super) enum Event {
    Snapshot(Query, Snapshot),
    Change(Change),
}
impl Event {
    pub(super) fn publish(self, output: &mut impl Write) -> Result<()> {
        match self {
            Self::Snapshot(query, snapshot) => emit(
                output,
                super::super::jsonl::snapshot(&query, &snapshot.into_heads()?)?,
            ),
            Self::Change(change) => publish(output, &change),
        }
    }
}
pub(super) struct Events {
    events: BoxProviderStream<'static, Result<Box<Event>>>,
    tasks: Arc<std::sync::Mutex<Vec<TaskId>>>,
}
impl Events {
    // Discovery authenticates descendants through the coordinator. Its stream
    // owns any in-flight metadata read, so every other coordinator wait must
    // keep polling it until that read releases its guard.
    pub(super) fn during<'a, T: 'a>(
        &'a mut self,
        work: impl Future<Output = T> + 'a,
        output: &'a mut impl Write,
    ) -> Pin<Box<dyn Future<Output = Result<T>> + 'a>> {
        // Erase the composed work before constructing the polling future. The
        // same boundary serves prompt, metadata and cancellation futures without
        // nesting their entire state machines in the CLI's native stack frame.
        let mut work: Pin<Box<dyn Future<Output = T> + 'a>> = Box::pin(work);
        Box::pin(async move {
            loop {
                tokio::select! {
                    result = work.as_mut() => return Ok(result),
                    event = self.next() => match event {
                        Some(event) => (*event?).publish(output)?,
                        None => return Err(Error::Storage("approval tree follow ended".into())),
                    },
                }
            }
        })
    }
    pub(super) async fn next(&mut self) -> Option<Result<Box<Event>>> {
        self.events.next().await
    }
    pub(super) fn tasks(&self) -> Result<Vec<TaskId>> {
        self.tasks
            .lock()
            .map(|tasks| tasks.clone())
            .map_err(|_| Error::Storage("approval discovery state poisoned".into()))
    }
}
type Feed = BoxProviderStream<'static, Result<(TaskId, bool, Change)>>;

struct Tree {
    local: Arc<LocalHarness>,
    root: TaskId,
    initialized: bool,
    seen: BTreeSet<TaskId>,
    feeds: SelectAll<Feed>,
    statuses: VecDeque<(TaskId, FileValue)>,
    ready: VecDeque<Event>,
    tasks: Arc<std::sync::Mutex<Vec<TaskId>>>,
}
impl Tree {
    async fn watch(
        &mut self,
        task: TaskId,
        files: Arc<TaskFiles<LocalStream>>,
        root: bool,
    ) -> Result<()> {
        let snapshot = files.inner.child_lifecycle(None).await?;
        let changes = files.inner.follow(Query::Children, snapshot.cursor).await?;
        self.statuses
            .extend(snapshot.files.into_iter().map(|file| (task, file)));
        self.feeds.push(Box::pin(
            changes.map(move |change| Ok((task, false, change?))),
        ));
        if !root {
            let component = Component::new(task.to_string())?;
            let query = Query::Approvals(component.clone());
            let snapshot = files.inner.pending_approvals(&component).await?;
            let changes = files
                .inner
                .follow(query.clone(), snapshot.cursor.clone())
                .await?;
            self.feeds.push(Box::pin(
                changes.map(move |change| Ok((task, true, change?))),
            ));
            self.ready.push_back(Event::Snapshot(query, snapshot));
        }
        self.seen.insert(task);
        if !root {
            // Publish discovery before its snapshot can be delivered. The same
            // authenticated set owns both following and disconnect detection.
            self.tasks
                .lock()
                .map_err(|_| Error::Storage("approval discovery state poisoned".into()))?
                .push(task);
        }
        Ok(())
    }
    async fn next(&mut self) -> Result<Event> {
        if !self.initialized {
            self.watch(self.root, self.local.task_files().clone(), true)
                .await?;
            self.initialized = true;
        }
        loop {
            if let Some(event) = self.ready.pop_front() {
                return Ok(event);
            }
            if let Some((parent, file)) = self.statuses.pop_front() {
                if matches!(
                    file.key,
                    FileKey::Child {
                        file: ChildFile::Status,
                        ..
                    }
                ) {
                    let child = ChildStatus::parse(&file.bytes)?.task;
                    if !self.seen.contains(&child)
                        && let Some(files) = self.local.approval_child_files(parent, child).await?
                    {
                        self.watch(child, files, false).await?;
                    }
                }
                continue;
            }
            let (parent, approval, change) =
                self.feeds.next().await.ok_or_else(|| {
                    Error::Storage("approval tree lost its owner follow".into())
                })??;
            if approval {
                if !change.is_checkpoint() {
                    return Ok(Event::Change(change));
                }
            } else {
                self.statuses.extend(
                    change
                        .file
                        .into_iter()
                        .chain(change.files)
                        .map(|file| (parent, file)),
                );
            }
        }
    }
}

pub(super) fn follow(local: Arc<LocalHarness>, root: TaskId) -> Events {
    let tasks = Arc::new(std::sync::Mutex::new(vec![root]));
    let tree = Tree {
        local,
        root,
        initialized: false,
        seen: BTreeSet::new(),
        feeds: SelectAll::new(),
        statuses: VecDeque::new(),
        ready: VecDeque::new(),
        tasks: tasks.clone(),
    };
    let events = Box::pin(futures::stream::try_unfold(tree, |mut tree| async move {
        let event = tree.next().await?;
        Ok(Some((Box::new(event), tree)))
    }));
    Events { events, tasks }
}

#[cfg(all(test, not(acyclic_harness_loom), not(acyclic_loom)))]
mod tests {
    use super::*;
    use crate::{Admission, runtime::RuntimeTask};

    async fn discovery_releases_gate(host_done: bool, broken: bool) -> Result<bool> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(
            crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        let model = crate::model::Model::new(
            "test".into(),
            "approval-discovery".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let local = Arc::new(
            LocalHarness::open(crate::test_support::local_harness_config(
                directory.path(),
                (model, provider),
            )?)
            .await?,
        );
        let Admission::Accepted(RuntimeTask::Durable { task_id: task, .. }) =
            local.submit(OperationId::new(), "parent").await?
        else {
            panic!("admission");
        };
        let mut client = LocalFileClient::open(local.clone(), OperationId::new()).await?;
        drop(client.attachment.take());
        client.host.as_mut().await?;
        client.host = Box::pin(std::future::pending());
        client.host_done = host_done;
        let snapshot = local
            .task_files()
            .inner
            .snapshot(&Query::Root, MAXIMUM_FILES)
            .await?;
        let host = Arc::clone(local.test_coordinator());
        let discovery_host = host.clone();
        let (entered, locked) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        client.approvals = Events {
            events: Box::pin(
                futures::stream::once(async move {
                    discovery_host
                        .test_coordinator_barrier(entered, released)
                        .await;
                    Ok(Box::new(Event::Snapshot(Query::Root, snapshot)))
                })
                .chain(futures::stream::pending()),
            ),
            tasks: Arc::new(std::sync::Mutex::new(vec![task])),
        };
        assert!(futures::poll!(Box::pin(client.approvals.next()).as_mut()).is_pending());
        locked.await.unwrap();
        release.send(()).unwrap();
        // IO is ready, but only polling discovery releases the actual gate.
        // Probe once without a deadline; the parked-owner control is Pending.
        let mut output = Vec::new();
        let ready = if broken {
            tokio::task::unconstrained(host.terminal_status(task))
                .now_or_never()
                .map(|result| result.map(|status| status.is_none()))
        } else {
            tokio::task::unconstrained(
                client.while_driving(host.terminal_status(task), &mut output),
            )
            .now_or_never()
            .map(|result| result.and_then(|status| status.map(|status| status.is_none())))
        };
        let ready = ready.transpose()?.unwrap_or(false);
        if ready {
            assert!(!output.is_empty(), "discovery event was not published");
        }
        drop(client);
        drop(host);
        super::super::super::disconnect_local(local).await?;
        Ok(ready)
    }

    #[tokio::test]
    async fn approval_metadata_drives_discovery_before_and_after_host_settlement() -> Result<()> {
        for host_done in [false, true] {
            assert!(
                discovery_releases_gate(host_done, false).await?,
                "metadata parked approval discovery"
            );
            assert!(
                !discovery_releases_gate(host_done, true).await?,
                "old parked-owner control progressed"
            );
        }
        Ok(())
    }
}
