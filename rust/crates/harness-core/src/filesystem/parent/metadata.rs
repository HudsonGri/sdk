//! One construction owner for a merge plan's parent and source bindings.
use super::*;

pub(in crate::filesystem) struct MergeMetadata {
    pub(super) child_project: VolumeRef,
    pub(super) parent_project: VolumeRef,
    pub(super) parent_scope: Scope,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> ParentProjectController<'_, A, O> {
    pub(in crate::filesystem) fn merge_metadata(
        &self,
        source: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<MergeMetadata> {
        crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let child_project = source.copy_in_quota(&mut quota)?;
            let parent_project = self.project.copy_in_quota(&mut quota)?;
            let parent_scope = self.authority.scope().copy_in_quota(&mut quota)?;
            let _native_custody = quota.into_custody()?;
            Ok(MergeMetadata {
                child_project,
                parent_project,
                parent_scope,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: _native_custody,
            })
        })
    }
}
