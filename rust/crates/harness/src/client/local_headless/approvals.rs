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
