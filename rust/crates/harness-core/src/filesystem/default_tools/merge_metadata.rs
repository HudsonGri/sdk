use super::*;
use crate::{core::AuthorityIssuer, executor::with_history_io, filesystem::PreparationIo};

impl MergeBindingMetadata {
    pub(in crate::filesystem) fn new(
        issuer: &AuthorityIssuer,
        scope: &Scope,
        project: &VolumeRef,
        source: &Authority,
        source_project: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let verifier = issuer.verifier_in_quota(&mut quota)?;
            let scope = scope.copy_in_quota(&mut quota)?;
            let project = project.copy_in_quota(&mut quota)?;
            let source = crate::core::copy_authority(source, &mut quota)?;
            let source_project = source_project.copy_in_quota(&mut quota)?;
            let _native_metadata = quota.into_custody()?;
            Ok(Self {
                verifier,
                scope,
                project,
                source,
                source_project,
                #[cfg(not(target_arch = "wasm32"))]
                _native_metadata,
            })
        })
    }
}
