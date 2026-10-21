//! Search requests and bounded result publications are ordinary followed files.
use super::*;
use crate::runtime::TaskStateProvider as _;
use crate::task_volume::{ChangeStream, FileKey, FileValue, Query, TaskFiles};
use crate::tools::git::View;
use crate::tui_files::NameSearchRequest;
use acyclic_fs::access::Action;
use futures::StreamExt as _;

impl LocalHarness {
    pub(crate) async fn bind_name_search(
        &self,
        completion: &mut crate::tui_files::CompletionFiles,
        input: &mut crate::tui::PromptInput,
    ) -> Result<()> {
        let Some(workspace) = self.workspace().cloned() else {
            return Ok(());
        };
        let files = Arc::clone(self.task_files());
        // Mount policies describe the host's view. The client must also hold
        // the signed content grant; a host-owned auxiliary mount alone must
        // never make another agent's filenames visible in this task.
        self.verifier.verify(&self.signed, &mut None)?;
        let verifier = self.verifier.clone();
        let signed = self.signed.clone();
        let runtime = Arc::clone(&self.runtime);
        let key = FileKey::parse("search/query.json").map_err(storage)?;
        let observation = files
            .inner
            .observe(Query::File(key), None, 1)
            .await
            .map_err(storage)?;
        let initial = observation
            .snapshot
            .and_then(|snapshot| snapshot.files.into_iter().next());
        let writer = Arc::clone(&files);
        let worker = tokio::spawn(async move {
            let mut changes = observation.changes;
            let mut query = initial
                .map(|file| serde_json::from_slice::<Option<NameSearchRequest>>(&file.bytes))
                .transpose()
                .map_err(storage)?
                .flatten();
            loop {
                let Some(active) = query.take() else {
                    query = request(&mut changes).await?;
                    continue;
                };
                let search = search(&workspace, &runtime, &writer, &active, &verifier, &signed);
                tokio::pin!(search);
                tokio::select! {
                    biased;
                    newer = request(&mut changes) => { query = newer?; }
                    done = &mut search => { done?; query = request(&mut changes).await?; }
                }
            }
        });
        completion.bind_search(input, files, worker)?;
        Ok(())
    }
}

async fn request(changes: &mut ChangeStream) -> Result<Option<NameSearchRequest>> {
    loop {
        let change = changes
            .next()
            .await
            .ok_or_else(|| Error::Storage("name query follow ended".into()))?
            .map_err(storage)?;
        if let Some(file) = change.file {
            return serde_json::from_slice(&file.bytes).map_err(storage);
        }
        if !change.removed.is_empty() {
            return Ok(None);
        }
    }
}

async fn publish(
    files: &TaskFiles<LocalStream>,
    path: &str,
    value: &impl serde::Serialize,
) -> Result<()> {
    let key = FileKey::parse(path).map_err(storage)?;
    let bytes = serde_json::to_vec(value).map_err(storage)?;
    loop {
        let snapshot = files
            .inner
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        match files
            .inner
            .append(
                snapshot.generation,
                &FileValue::new(key.clone(), bytes.clone()),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
        }
    }
}

async fn results(
    files: &TaskFiles<LocalStream>,
    request: &NameSearchRequest,
    best: &crate::tui::PathMatches,
    pending: usize,
    complete: bool,
    unavailable: usize,
) -> Result<()> {
    publish(
        files,
        "search/results.json",
        &serde_json::json!({"request":request.request,"query":request.query,"paths":best.paths().collect::<Vec<_>>()}),
    )
    .await?;
    publish(
        files,
        "search/progress.json",
        &serde_json::json!({"request":request.request,"query":request.query,"pending_dirs":pending,"complete":complete,"unavailable_entries":unavailable}),
    )
    .await
}

async fn search(
    workspace: &LocalWorkspace,
    runtime: &LocalTaskRuntime,
    files: &TaskFiles<LocalStream>,
    request: &NameSearchRequest,
    verifier: &AuthorityVerifier,
    signed: &Scope,
) -> Result<()> {
    let query = request.query.as_str();
    let admitted = match request.task {
        Some(task) => Some(runtime.task_host().observe_admission(task).await?),
        None => None,
    };
    let task_grants = admitted.as_ref().map(|record| &record.grants);
    let best = crate::tui::PathMatches::new(query);
    results(files, request, &best, 1, false, 0).await?;
    // Keep the selected branch alive for the complete query. Each mounted
    // volume is searched through its logical view and its own access policy.
    let selected = selected_view(workspace, runtime, request.task).await?;
    let mut volumes = Vec::new();
    for (prefix, mount) in workspace.mounts.mounts() {
        let view = if prefix.is_empty() {
            View::Checkout(selected.as_ref().unwrap_or(&workspace.view))
        } else if let Some(source) = &mount.view.source {
            source.workspace.view()
        } else {
            continue;
        };
        let names = crate::tools::git::IndexedNames::open(view).await?;
        volumes.push(VolumeSearch {
            names,
            prefix,
            mount,
            best: crate::tui::PathMatches::new(query),
            complete: false,
        });
    }
    if volumes.is_empty() {
        return results(files, request, &best, 0, true, 0).await;
    }
    let mut last = None;
    loop {
        // Poll all mounted indexes, including completed ones. An incomplete
        // large primary volume cannot starve an already indexed auxiliary
        // volume; a completed popup remains subscribed to later file changes.
        let (index, page) = {
            let mut next = futures::stream::FuturesUnordered::new();
            for (index, volume) in volumes.iter_mut().enumerate() {
                next.push(async move {
                    if volume.complete {
                        volume.names.changed().await?;
                    }
                    Ok::<_, Error>((index, volume.names.next_page().await?))
                });
            }
            next.next()
                .await
                .ok_or_else(|| Error::Storage("name searches ended".into()))??
        };
        verifier.verify(signed, &mut None)?;
        let volume = &mut volumes[index];
        volume.complete = page.is_none();
        if let Some(page) = page {
            if page.reset {
                volume.best = crate::tui::PathMatches::new(query);
            }
            for name in page.names.into_iter().filter(|name| name.regular) {
                let now = acyclic_stream::UnixMillisClock::now_unix_millis(
                    &acyclic_stream::SystemUnixMillisClock,
                ) / 1000;
                let parent = name.path.rsplit_once('/').map_or("", |(parent, _)| parent);
                if volume.mount.check(Action::List, parent, now).is_err()
                    || volume.mount.check(Action::Read, &name.path, now).is_err()
                    || !authorized_name(signed, task_grants, &volume.mount.view.volume, &name.path)?
                {
                    continue;
                }
                let path = if volume.prefix.is_empty() {
                    name.path
                } else {
                    format!("{}/{}", volume.prefix, name.path)
                };
                volume.best.push(path);
            }
        }
        let mut combined = crate::tui::PathMatches::new(query);
        let mut pending = 0;
        let mut complete = true;
        let mut unavailable = 0;
        for volume in &volumes {
            for path in volume.best.paths() {
                combined.push(path.to_owned());
            }
            complete &= volume.complete;
            pending += volume.names.pending_directories().await;
            unavailable += volume.names.unavailable_entries().await;
        }
        let publication = (
            combined.paths().map(str::to_owned).collect::<Vec<_>>(),
            pending,
            complete,
            unavailable,
        );
        if last.as_ref() != Some(&publication) {
            results(files, request, &combined, pending, complete, unavailable).await?;
            last = Some(publication);
        }
        tokio::task::yield_now().await;
    }
}

struct VolumeSearch<'a> {
    names: crate::tools::git::IndexedNames,
    prefix: &'a str,
    mount: &'a crate::workspace::Mount<Option<Arc<workspace::MountedVolume>>>,
    best: crate::tui::PathMatches,
    complete: bool,
}

// Read durable fork/selection metadata without constructing a task executor or
// mounting its checkout. Search must not trigger recovery or content capture.
async fn selected_view(
    workspace: &LocalWorkspace,
    runtime: &LocalTaskRuntime,
    task: Option<TaskId>,
) -> Result<Option<workspace::View>> {
    let mut task = task;
    let mut visited = std::collections::BTreeSet::new();
    while let Some(candidate) = task {
        if !visited.insert(candidate) {
            return Err(Error::Conflict("task ancestry contains a cycle".into()));
        }
        let admission = runtime.task_host().observe_admission(candidate).await?;
        if let Some(view) = workspace.existing_task_view(candidate).await? {
            return Ok(Some(view));
        }
        task = admission.parent;
    }
    workspace
        .selected_view()
        .await
        .map(|(selected, _)| selected)
}

fn authorized_name(
    scope: &Scope,
    task_grants: Option<&Capabilities>,
    volume: &VolumeRef,
    path: &str,
) -> Result<bool> {
    if !name_allowed(scope.capabilities(), scope.agent().is_some(), volume, path)? {
        return Ok(false);
    }
    match task_grants {
        Some(grants) => name_allowed(grants, scope.agent().is_some(), volume, path),
        None => Ok(true),
    }
}

fn name_allowed(
    grants: &Capabilities,
    has_agent: bool,
    volume: &VolumeRef,
    path: &str,
) -> Result<bool> {
    if crate::conversation::is_internal_path(path) {
        return Ok(false);
    }
    if grants.contains(&volume.capability(VolumeOperation::Read, &mut None)?) {
        return Ok(true);
    }
    // Directory grants are owner-delegated private content authority. Match
    // component boundaries, so a grant for `src` cannot expose `src-private`.
    if volume.class() != VolumeClass::AgentPrivate || !has_agent {
        return Ok(false);
    }
    for prefix in std::iter::once("").chain(path.match_indices('/').map(|(end, _)| &path[..end])) {
        if grants.contains(&volume.directory_read_capability(prefix, &mut None)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn name_search_selects_the_tasks_durable_fork_without_mounting_it() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let project = super::super::tests::volume(directory.path()).await?;
        let harness = LocalHarness::open(
            super::super::tests::volume_config(
                directory.path(),
                project,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            )?
            .without_default_tools(),
        )
        .await?;
        let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
            harness.submit(OperationId::new(), "search fork").await?
        else {
            panic!("search admission")
        };
        let workspace = harness.workspace.as_ref().unwrap();
        assert!(
            selected_view(workspace, &harness.runtime, Some(task_id))
                .await?
                .is_none()
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (fork, _) = workspace
            .view
            .fork(
                &format!("task-{task_id}"),
                acyclic_fs::IdempotencyKey::from_bytes(task_id.into_bytes()),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                },
            )
            .await?;
        let selected = selected_view(workspace, &harness.runtime, Some(task_id))
            .await?
            .expect("saved task fork");
        assert_eq!(selected.workspace().id(), fork.workspace().id());
        // Negative control: the global selector still names the original view.
        assert!(workspace.selected_view().await?.0.is_none());
        assert_ne!(selected.workspace().id(), workspace.view.workspace().id());
        assert!(workspace.mounted.is_none());
        assert!(workspace.state.lock().await.children.is_empty());
        Ok(())
    }

    #[test]
    fn name_search_requires_the_clients_signed_content_grant() -> Result<()> {
        let agent = AgentId::from_bytes([1; 16]);
        let provider = crate::resources::ProviderRef::new(
            "search-test".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let private = VolumeRef::new(
            provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "search-test",
            [7; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: agent.to_string(),
            },
        );
        let issue = |grants| {
            issuer.issue(
                "search",
                &Capabilities::new(grants),
                Some(agent),
                None,
                &mut None,
            )
        };
        let directory = issue(vec![private.directory_read_capability("src", &mut None)?])?;
        assert!(authorized_name(&directory, None, &private, "src/file.rs")?);
        assert!(!authorized_name(
            &directory,
            None,
            &private,
            "src-private/file.rs"
        )?);
        assert!(!authorized_name(&directory, None, &private, "other.rs")?);
        let absent = issue(Vec::<String>::new())?;
        assert!(!authorized_name(&absent, None, &private, "src/file.rs")?);
        // Negative control: the host mount's policy alone permits this name.
        let root = std::env::current_dir().map_err(storage)?;
        let mounts = workspace::mounts::owner(&root, &private, (), [])?;
        assert!(
            mounts
                .named(None)?
                .1
                .check(Action::Read, "src/file.rs", 0)
                .is_ok()
        );
        let whole = issue(vec![private.capability(VolumeOperation::Read, &mut None)?])?;
        assert!(authorized_name(&whole, None, &private, "other.rs")?);
        assert!(!authorized_name(&whole, None, &private, ".system/secret")?);
        let task_grants =
            Capabilities::new(vec![private.directory_read_capability("src", &mut None)?]);
        assert!(authorized_name(
            &whole,
            Some(&task_grants),
            &private,
            "src/file.rs"
        )?);
        assert!(!authorized_name(
            &whole,
            Some(&task_grants),
            &private,
            "other.rs"
        )?);
        // Negative control: the client's broader grant alone exposes this name.
        assert!(authorized_name(&whole, None, &private, "other.rs")?);
        let broad_task =
            Capabilities::new(vec![private.capability(VolumeOperation::Read, &mut None)?]);
        assert!(!authorized_name(
            &absent,
            Some(&broad_task),
            &private,
            "src/file.rs"
        )?);
        Ok(())
    }
}
