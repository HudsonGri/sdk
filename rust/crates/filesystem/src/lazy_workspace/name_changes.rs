use super::*;
use crate::demand::{SourceChange, SourceChangeSink, SourceWatch, native::NativeDemandSource};
use tokio::sync::watch;

/// Bounded native namespace delta. Overflow or uncertain delivery requires
/// discovery; it is never represented as an empty successful change set.
#[derive(Default)]
pub struct NativeNameDelta {
    /// Names whose bindings changed. Content-only notifications need no names.
    pub paths: std::collections::BTreeSet<String>,
    /// Some affected names are unknown.
    pub all: bool,
}

struct NameSignal {
    revision: watch::Sender<u64>,
    pending: Arc<Mutex<NativeNameDelta>>,
}

impl SourceChangeSink for NameSignal {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    fn source_changed(
        &self,
        changes: &[SourceChange],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        accounting.check().map_err(DemandError::from)?;
        if !changes.is_empty() {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| DemandError::SourceUnavailable)?;
            for change in changes {
                match change {
                    SourceChange::Everything | SourceChange::Unconfirmed => pending.all = true,
                    SourceChange::Name(path) => {
                        if pending.all {
                            continue;
                        }
                        if path.is_root() {
                            pending.all = true;
                            pending.paths.clear();
                            continue;
                        }
                        if path
                            .components()
                            .iter()
                            .any(|name| name.unicode_utf8_bytes().is_none())
                        {
                            // The string-based mention surface cannot name this
                            // entry. Keep observation live and let discovery
                            // publish an explicit unavailable-entry count.
                            pending.all = true;
                            pending.paths.clear();
                            continue;
                        }
                        let mut text = String::new();
                        for component in path.components() {
                            if !text.is_empty() {
                                text.push('/');
                            }
                            text.push_str(
                                &component
                                    .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                                    .map_err(|_| DemandError::SourceUnavailable)?
                                    .value,
                            );
                        }
                        if pending.paths.len() == 512 && !pending.paths.contains(&text) {
                            pending.all = true;
                            pending.paths.clear();
                        } else {
                            pending.paths.insert(text);
                        }
                    }
                    SourceChange::Entry(path)
                        if path.components().last().is_some_and(|name| {
                            name.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                                .is_ok_and(|name| {
                                    name.value == ".gitignore" || name.value == ".ignore"
                                })
                        }) =>
                    {
                        pending.all = true
                    }
                    SourceChange::Entry(_) | SourceChange::Node(_) => {}
                }
            }
            drop(pending);
            self.revision
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facade::FsWork as _;

    fn path(text: &str, windows: bool) -> NamespacePath {
        let (encoding, bytes) = if windows {
            (
                crate::kernel::NameEncoding::WindowsUtf16Le,
                text.encode_utf16().flat_map(u16::to_le_bytes).collect(),
            )
        } else {
            (crate::kernel::NameEncoding::Utf8, text.as_bytes().to_vec())
        };
        NamespacePath::new(
            vec![LogicalName::new(encoding, bytes, 255).unwrap()],
            crate::model::VolumeLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn name_search_native_signal_bounds_pending_names_and_marks_overflow() {
        let (revision, receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(NativeNameDelta::default())),
        };
        let stop = CancellationToken::new();
        for i in 0..513 {
            signal
                .source_changed(
                    &[SourceChange::Name(path(&format!("file-{i}"), false))],
                    &mut WorkBudget::UNBOUNDED.accounting(&stop),
                )
                .unwrap();
            assert!(signal.pending.lock().unwrap().paths.len() <= 512);
        }
        let pending = signal.pending.lock().unwrap();
        assert!(pending.all);
        assert!(pending.paths.is_empty());
        assert_eq!(*receiver.borrow(), 513);
        // Negative control: an unbounded fold retains every distinct event.
        assert!((0..513).collect::<std::collections::BTreeSet<_>>().len() > 512);
    }

    #[test]
    fn name_search_native_policy_notifications_decode_windows_names() {
        let (revision, _receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(NativeNameDelta::default())),
        };
        let policy = path(".gitignore", true);
        // Negative control: comparing raw bytes with UTF-8 misses this policy.
        assert_ne!(
            policy.components().first().unwrap().as_bytes(),
            b".gitignore"
        );
        signal
            .source_changed(
                &[SourceChange::Entry(policy)],
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .unwrap();
        assert!(signal.pending.lock().unwrap().all);
    }

    #[test]
    fn name_search_native_signal_keeps_following_after_a_non_unicode_name() {
        let (revision, receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(NativeNameDelta::default())),
        };
        let name =
            LogicalName::new(crate::kernel::NameEncoding::PosixBytes, vec![0xff], 255).unwrap();
        // Negative control: projecting the original event into a string fails.
        assert!(
            name.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .is_err()
        );
        let native = NamespacePath::new(vec![name], crate::model::VolumeLimits::default()).unwrap();
        signal
            .source_changed(
                &[SourceChange::Name(native)],
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .unwrap();
        signal
            .source_changed(
                &[SourceChange::Name(path("healthy.txt", false))],
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .unwrap();
        assert_eq!(*receiver.borrow(), 2);
        assert!(signal.pending.lock().unwrap().all);
    }
}

/// One shared producer's native source subscription. Notifications only wake
/// index maintenance; clients continue to follow the committed index files.
pub struct NativeNameChanges {
    source: Arc<NativeDemandSource>,
    revision: watch::Receiver<u64>,
    watch: Box<dyn SourceWatch>,
    pending: Arc<Mutex<NativeNameDelta>>,
}

impl NativeNameChanges {
    /// Waits for a host change, then advances the source epoch. A consumer
    /// cancels its old walk before starting work at the resulting fresh cut.
    pub async fn changed(&mut self) -> Result<NativeNameDelta, LazyWorkspaceError> {
        self.revision
            .changed()
            .await
            .map_err(|_| DemandError::SourceUnavailable)?;
        self.revision.borrow_and_update();
        if !self.watch.is_exact() {
            return Err(DemandError::SourceUnavailable.into());
        }
        self.source.invalidate();
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| DemandError::SourceUnavailable)?;
        Ok(std::mem::take(&mut *pending))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, NativeDemandSource, S>
{
    /// Reinstalls native observation from an indexed directory name. This
    /// deliberately bypasses authored directory bindings, which can coexist
    /// with unindexed native children. It performs no directory enumeration.
    pub async fn observe_indexed_native_directory(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let namespace = self.namespace_path(path, accounting)?;
        let observed = accounted!(accounting, |work, stop| self.source.lookup(
            snapshot.source,
            &namespace.value,
            work,
            stop
        ));
        namespace.retire(accounting)?;
        Ok(observed?.is_some_and(|node| node.kind == SourceNodeKind::Directory))
    }

    /// Subscribes before pathname discovery, so a completed index can remain
    /// live without a timer or a periodic directory scan.
    pub fn follow_native_names(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<NativeNameChanges>, LazyWorkspaceError> {
        let (sender, revision) = watch::channel(0);
        let pending = Arc::new(Mutex::new(NativeNameDelta::default()));
        let watch = self.watch_source(
            Arc::new(NameSignal {
                revision: sender,
                pending: Arc::clone(&pending),
            }),
            accounting,
        )?;
        Ok(watch.map(|watch| NativeNameChanges {
            source: Arc::clone(&self.source),
            revision,
            watch,
            pending,
        }))
    }
}
