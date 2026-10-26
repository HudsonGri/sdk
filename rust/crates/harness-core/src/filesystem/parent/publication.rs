//! Owned authority at the native project publication boundary.
use super::*;

pub(in crate::filesystem) struct PublicationAuthority {
    authority: crate::core::VerifiedScope,
    project: VolumeRef,
    custody: acyclic_fs::NativeAllocation,
}

impl<A, O> ParentProjectController<'_, A, O> {
    pub(in crate::filesystem) fn publication_authority(
        &self,
        io: &PreparationIo,
    ) -> Result<Arc<PublicationAuthority>> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write, Some(io))?;
        let mut history = io.history_io();
        let mut observer = Some(&mut history as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
        let authority = self
            .authority
            .verifier()
            .authenticate_scope(self.authority.scope(), quota.work())?;
        quota.adopt(authority.logical_owned_bytes()?)?;
        let project = self.project.copy_in_quota(&mut quota)?;
        let bytes =
            (std::mem::size_of::<PublicationAuthority>() + 2 * std::mem::size_of::<usize>()) as u64;
        quota.retain(bytes)?;
        quota.observer()?.allocation_attempt(bytes)?;
        let custody = quota
            .into_custody()?
            .ok_or_else(|| io.invalid("native merge owner has no custody"))?;
        Ok(Arc::new(PublicationAuthority {
            authority,
            project,
            custody,
        }))
    }
}

impl PublicationAuthority {
    pub(in crate::filesystem) fn require(
        &self,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self.custody.work().shares_allocation_context(original) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "merge publication owner belongs to another original resource",
            });
        }
        let io = PreparationIo::from_native_work(original)?;
        ContentGrant::authorize_authenticated_volume(
            &self.authority,
            &self.project,
            VolumeOperation::Write,
            u64::MAX,
            &mut Some(&mut io.history_io()),
        )?;
        if !self
            .authority
            .scope()
            .capabilities()
            .contains(capability::PROJECT_MERGE)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent project publication is not currently granted",
            });
        }
        Ok(())
    }
}
