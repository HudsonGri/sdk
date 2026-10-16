//! Lock order: registry -> host setup/provider; providers never enter the registry.
use super::*;
use crate::sync::owned::{Arc as StateArc, Mutex, lock_owned};
use std::collections::BTreeMap;

pub(super) struct Opened<T, R> {
    entries: BTreeMap<String, Arc<T>>,
    resources: Vec<R>,
}

impl<T, R> Default for Opened<T, R> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            resources: Vec::new(),
        }
    }
}

async fn open_once<T, R>(
    gate: &StateArc<Mutex<Opened<T, R>>>,
    key: String,
    open: impl AsyncFnOnce() -> Result<(Arc<T>, R)>,
) -> Result<Arc<T>> {
    let mut state = lock_owned(StateArc::clone(gate)).await;
    if let Some(value) = state.entries.get(&key) {
        return Ok(Arc::clone(value));
    }
    #[cfg(acyclic_harness_loom)]
    if model::unlocked() {
        drop(state);
        let (value, resource) = open().await?;
        let mut state = lock_owned(StateArc::clone(gate)).await;
        state.entries.insert(key, Arc::clone(&value));
        state.resources.push(resource);
        return Ok(value);
    }
    let (value, resource) = open().await?;
    state.entries.insert(key, Arc::clone(&value));
    state.resources.push(resource);
    Ok(value)
}

async fn close_after_workers<T, R>(
    opened: Opened<T, R>,
    join: impl AsyncFn(R) -> Result<()>,
    close: impl AsyncFn(Arc<T>) -> Result<()>,
) -> Result<()> {
    #[cfg(acyclic_harness_loom)]
    if model::early_close() {
        for value in opened.entries.into_values() {
            close(value).await?;
        }
        return Ok(());
    }
    let mut failed = None;
    for worker in opened.resources {
        if let Err(error) = join(worker).await {
            failed.get_or_insert(error);
        }
    }
    for value in opened.entries.into_values() {
        if let Err(error) = close(value).await {
            failed.get_or_insert(error);
        }
    }
    failed.map_or(Ok(()), Err)
}

pub(super) struct Registry {
    root: PathBuf,
    stop: Arc<crate::task_volume::TaskVolume<LocalStream>>,
    instance: uuid::Uuid,
    state: StateArc<Mutex<Opened<LocalHarness, tokio::task::JoinHandle<Result<()>>>>>,
}

impl Registry {
    pub(super) fn new(
        root: PathBuf,
        stop: Arc<crate::task_volume::TaskVolume<LocalStream>>,
        instance: uuid::Uuid,
    ) -> Self {
        Self {
            root,
            stop,
            instance,
            state: StateArc::new(Mutex::new(Opened::default())),
        }
    }

    pub(super) async fn get(&self, workspace: &str) -> Result<Arc<LocalHarness>> {
        open_once(&self.state, workspace.into(), async || {
            let path: PathBuf = super::super::volume::decode(&read(
                &self
                    .root
                    .join("workspaces")
                    .join(format!("{workspace}.json")),
            )?)
            .map_err(storage)?;
            let (key, path) = credential::workspace(&path)?;
            if key != workspace {
                return Err(Error::Unauthorized("workspace binding was replaced".into()));
            }
            let harness = Arc::new(LocalHarness::open(LocalHarnessConfig::external(path)).await?);
            let driver = Arc::clone(&harness);
            let stop = Arc::clone(&self.stop);
            let instance = self.instance;
            let worker = tokio::spawn(async move { server::drive(driver, stop, instance).await });
            Ok((harness, worker))
        })
        .await
    }

    pub(super) async fn drain(&self) -> Result<()> {
        // The server has stopped admitting calls. Move ownership out of the
        // registry lock before joining effects or entering provider shutdown.
        let opened = {
            let mut state = lock_owned(StateArc::clone(&self.state)).await;
            std::mem::take(&mut *state)
        };
        close_after_workers(
            opened,
            async |worker| worker.await.map_err(storage)?,
            async |harness| {
                Arc::try_unwrap(harness)
                    .map_err(|_| Error::Conflict("workspace still has an execution owner".into()))?
                    .close()
                    .await
            },
        )
        .await
    }
}

#[cfg(acyclic_harness_loom)]
mod model;
