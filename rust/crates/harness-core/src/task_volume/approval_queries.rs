//! Active approval metadata is disposable; settled files remain canonically addressable.
use super::*;
use std::collections::BTreeSet;

#[derive(Default)]
#[cfg_attr(acyclic_harness_loom, derive(Clone))]
pub(super) struct Pending {
    requests: BTreeMap<(Component, Component), BTreeSet<FileKey>>,
}
impl Pending {
    pub(super) fn record(&mut self, files: &FileIndex, key: &FileKey) {
        let FileKey::Task { task, file } = key else {
            return;
        };
        match file {
            TaskFile::ApprovalRequest(request) => {
                let decision = |principal| FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::ApprovalDecision {
                        request: request.clone(),
                        principal,
                    },
                };
                if files
                    .range(
                        decision(Component(Arc::from("")))
                            ..=decision(Component(Arc::from("\u{10ffff}"))),
                    )
                    .next()
                    .is_none()
                {
                    self.requests
                        .entry((task.clone(), request.clone()))
                        .or_default()
                        .insert(key.clone());
                }
            }
            TaskFile::ApprovalReview { request, .. } => {
                if let Some(keys) = self.requests.get_mut(&(task.clone(), request.clone())) {
                    keys.insert(key.clone());
                }
            }
            TaskFile::ApprovalDecision { request, .. } => {
                self.requests.remove(&(task.clone(), request.clone()));
            }
            _ => {}
        }
    }
    pub(super) fn files(&self, index: &Index, task: &Component) -> Vec<FileValue> {
        self.requests
            .range(
                (task.clone(), Component(Arc::from("")))
                    ..=(task.clone(), Component(Arc::from("\u{10ffff}"))),
            )
            .flat_map(|(_, keys)| keys)
            .filter_map(|key| index.files.get(key))
            .map(|stored| stored.file.clone())
            .collect()
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn native_index_bytes(&self) -> usize {
        self.requests.len() * std::mem::size_of::<((Component, Component), BTreeSet<FileKey>)>()
            + self
                .requests
                .values()
                .map(|keys| keys.len() * std::mem::size_of::<FileKey>())
                .sum::<usize>()
    }
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Active requests and reviews at one cut, without copying settled history.
    /// The cursor follows all approval events, including decisions that retire them.
    #[doc(hidden)]
    pub async fn pending_approvals(&self, task: &Component) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let files = index.approvals.files(&index, task);
        let generation = self.generation(&index);
        drop(index);
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(&Query::Approvals(task.clone()), generation, watermarks)?,
            generation,
        })
    }
}

#[cfg(test)]
mod tests;
