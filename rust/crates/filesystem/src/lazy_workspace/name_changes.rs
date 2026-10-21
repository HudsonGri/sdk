use super::*;
use crate::demand::{SourceChange, SourceChangeSink, SourceWatch, native::NativeDemandSource};
use tokio::sync::watch;

struct NameSignal(watch::Sender<u64>);

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
            self.0
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }
}

/// One shared producer's native source subscription. Notifications only wake
/// index maintenance; clients continue to follow the committed index files.
pub struct NativeNameChanges {
    source: Arc<NativeDemandSource>,
    revision: watch::Receiver<u64>,
    watch: Box<dyn SourceWatch>,
}

impl NativeNameChanges {
    /// Waits for a host change, then advances the source epoch. A consumer
    /// cancels its old walk before starting work at the resulting fresh cut.
    pub async fn changed(&mut self) -> Result<(), LazyWorkspaceError> {
        self.revision
            .changed()
            .await
            .map_err(|_| DemandError::SourceUnavailable)?;
        self.revision.borrow_and_update();
        if !self.watch.is_exact() {
            return Err(DemandError::SourceUnavailable.into());
        }
        self.source.invalidate();
        Ok(())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, NativeDemandSource, S>
{
    /// Subscribes before pathname discovery, so a completed index can remain
    /// live without a timer or a periodic directory scan.
    pub fn follow_native_names(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<NativeNameChanges>, LazyWorkspaceError> {
        let (sender, revision) = watch::channel(0);
        let watch = self.watch_source(Arc::new(NameSignal(sender)), accounting)?;
        Ok(watch.map(|watch| NativeNameChanges {
            source: Arc::clone(&self.source),
            revision,
            watch,
        }))
    }
}
