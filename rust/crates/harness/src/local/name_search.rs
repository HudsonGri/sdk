//! Search requests and bounded result publications are ordinary followed files.
use super::*;
use crate::task_volume::{ChangeStream, FileKey, FileValue, Query, TaskFiles};
use crate::tools::git::View;
use acyclic_fs::access::Action;
use futures::StreamExt as _;

impl LocalHarness {
    pub(crate) async fn bind_name_search(
        &self,
        completion: &mut crate::tui_files::CompletionFiles,
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
                .map(|file| serde_json::from_slice::<Option<String>>(&file.bytes))
                .transpose()
                .map_err(storage)?
                .flatten();
            loop {
                let Some(active) = query.take() else {
                    query = request(&mut changes).await?;
                    continue;
                };
                let search = search(&workspace, &writer, &active, &verifier, &signed);
                tokio::pin!(search);
                tokio::select! {
                    biased;
                    newer = request(&mut changes) => { query = newer?; }
                    done = &mut search => { done?; query = request(&mut changes).await?; }
                }
            }
        });
        completion.bind_search(files, worker);
        Ok(())
    }
}

async fn request(changes: &mut ChangeStream) -> Result<Option<String>> {
    loop {
        let change = changes
            .next()
            .await
            .ok_or_else(|| Error::Storage("name query follow ended".into()))?
            .map_err(storage)?;
        if let Some(file) = change.file {
            return serde_json::from_slice(&file.bytes).map_err(storage);
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
    query: &str,
    best: &crate::tui::PathMatches,
    pending: usize,
    complete: bool,
) -> Result<()> {
    publish(
        files,
        "search/results.json",
        &serde_json::json!({"query":query,"paths":best.paths().collect::<Vec<_>>()}),
    )
    .await?;
    publish(
        files,
        "search/progress.json",
        &serde_json::json!({"query":query,"pending_dirs":pending,"complete":complete}),
    )
    .await
}

async fn search(
    workspace: &LocalWorkspace,
    files: &TaskFiles<LocalStream>,
    query: &str,
    verifier: &AuthorityVerifier,
    signed: &Scope,
) -> Result<()> {
    let best = crate::tui::PathMatches::new(query);
    results(files, query, &best, 1, false).await?;
    // Keep the selected branch alive for the complete query. Each mounted
    // volume is searched through its logical view and its own access policy.
    let (selected, _) = workspace.selected_view().await?;
    let mut volumes = Vec::new();
    for (prefix, mount) in workspace.mounts.mounts() {
        let view = if prefix.is_empty() {
            View::Checkout(selected.as_ref().unwrap_or(&workspace.view))
        } else if let Some(source) = &mount.view.source {
            source.workspace.view()
        } else {
            continue;
        };
        let ignore = crate::tools::git::search_ignore(view).await?;
        let names = crate::tools::git::IndexedNames::open(view, ignore).await?;
        volumes.push(VolumeSearch {
            names,
            prefix,
            mount,
            best: crate::tui::PathMatches::new(query),
            complete: false,
        });
    }
    if volumes.is_empty() {
        return results(files, query, &best, 0, true).await;
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
                    || !signed_name_allowed(signed, &volume.mount.view.volume, &name.path)?
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
        for volume in &volumes {
            for path in volume.best.paths() {
                combined.push(path.to_owned());
            }
            complete &= volume.complete;
            pending += volume.names.pending_directories().await;
        }
        let publication = (
            combined.paths().map(str::to_owned).collect::<Vec<_>>(),
            pending,
            complete,
        );
        if last.as_ref() != Some(&publication) {
            results(files, query, &combined, pending, complete).await?;
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

fn signed_name_allowed(scope: &Scope, volume: &VolumeRef, path: &str) -> Result<bool> {
    if crate::conversation::is_internal_path(path) {
        return Ok(false);
    }
    if scope
        .capabilities()
        .contains(&volume.capability(VolumeOperation::Read, &mut None)?)
    {
        return Ok(true);
    }
    // Directory grants are owner-delegated private content authority. Match
    // component boundaries, so a grant for `src` cannot expose `src-private`.
    if volume.class() != VolumeClass::AgentPrivate || scope.agent().is_none() {
        return Ok(false);
    }
    for prefix in std::iter::once("").chain(path.match_indices('/').map(|(end, _)| &path[..end])) {
        if scope
            .capabilities()
            .contains(&volume.directory_read_capability(prefix, &mut None)?)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(signed_name_allowed(&directory, &private, "src/file.rs")?);
        assert!(!signed_name_allowed(
            &directory,
            &private,
            "src-private/file.rs"
        )?);
        assert!(!signed_name_allowed(&directory, &private, "other.rs")?);
        let absent = issue(Vec::<String>::new())?;
        assert!(!signed_name_allowed(&absent, &private, "src/file.rs")?);
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
        assert!(signed_name_allowed(&whole, &private, "other.rs")?);
        assert!(!signed_name_allowed(&whole, &private, ".system/secret")?);
        Ok(())
    }
}
