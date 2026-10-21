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
                let search = search(&workspace, &writer, &active);
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
) -> Result<()> {
    let mut best = crate::tui::PathMatches::new(query);
    results(files, query, &best, 1, false).await?;
    // Keep the selected branch alive for the complete query. Each mounted
    // volume is searched through its logical view and its own access policy.
    let (selected, _) = workspace.selected_view().await?;
    for (prefix, mount) in workspace.mounts.mounts() {
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        ) / 1000;
        if mount.check(Action::List, "", now).is_err() {
            continue;
        }
        let view = if prefix.is_empty() {
            View::Checkout(selected.as_ref().unwrap_or(&workspace.view))
        } else if let Some(source) = &mount.view.source {
            source.workspace.view()
        } else {
            continue;
        };
        let ignore = crate::tools::git::search_ignore(view).await?;
        let mut names = crate::tools::git::IndexedNames::open(view, ignore).await?;
        let mut volume_best = crate::tui::PathMatches::new(query);
        while let Some(page) = names.next_page().await.map_err(storage)? {
            if page.reset {
                volume_best = crate::tui::PathMatches::new(query);
            }
            for name in page.names.into_iter().filter(|name| name.regular) {
                if mount.check(Action::Read, &name.path, now).is_err() {
                    continue;
                }
                let path = if prefix.is_empty() {
                    name.path
                } else {
                    format!("{prefix}/{}", name.path)
                };
                volume_best.push(path);
            }
            let mut combined = best.clone();
            for path in volume_best.paths() {
                combined.push(path.to_owned());
            }
            results(
                files,
                query,
                &combined,
                names.pending_directories().await,
                false,
            )
            .await?;
            tokio::task::yield_now().await;
        }
        for path in volume_best.paths() {
            best.push(path.to_owned());
        }
    }
    results(files, query, &best, 0, true).await
}
