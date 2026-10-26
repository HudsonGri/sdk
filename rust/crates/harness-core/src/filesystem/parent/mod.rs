use super::*;
use crate::executor::stock_boundary;

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) type MergeAllocation = acyclic_fs::NativeAllocation;
#[cfg(target_arch = "wasm32")]
pub(in crate::filesystem) type MergeAllocation = ();

/// Parent-owned project operations.
pub struct ParentProjectController<'a, A, O> {
    pub(in crate::filesystem) host: &'a FilesystemHost<A, O>,
    pub(in crate::filesystem) project: VolumeRef,
    pub(in crate::filesystem) authority: crate::core::VerifiedScope,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Inspected project changes whose publication remains bound to the parent controller.
pub struct ParentMergePlan<A, O> {
    pub(in crate::filesystem) plan: JoinPlan<A, O>,
    pub(in crate::filesystem) metadata: metadata::MergeMetadata,
}

/// Prepared child-owned prefix and its exact reader grants.
pub struct InheritedContextCapture {
    /// Child-owned inherited context file.
    pub file: FileRef,
    /// Pinned child-private generation containing the file.
    pub generation: GenerationRef,
    /// Exact attached-reader grants for referenced files.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Pinned attachment-list manifests admitted with the context.
    pub attachment_manifests: Vec<FileRef>,
}

impl<A, O> ParentMergePlan<A, O> {
    /// Exact parent generation observed while planning; publication uses this CAS target.
    #[must_use]
    pub const fn target_head(&self) -> GenerationId {
        self.plan.target_head()
    }

    /// Exact child generation whose changes were inspected.
    #[must_use]
    pub const fn source_head(&self) -> GenerationId {
        self.plan.source_head()
    }
}

impl<'a, A, O> ParentProjectController<'a, A, O> {
    /// Binds the authenticated parent and project on the optional original IO.
    pub fn new(
        host: &'a FilesystemHost<A, O>,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        project: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let reject = |code, reason| stock_boundary(io, code, reason);
        if parent.authority().kind != crate::core::AggregateKind::Conversation {
            return Err(reject(
                crate::wire::ErrorCode::Invalid,
                "project controller requires a parent conversation",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            verifier.verify_audience(parent.authority(), work)?;
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let authority = verifier.authenticate_scope(scope, quota.work())?;
            quota.adopt(authority.logical_owned_bytes()?)?;
            if scope.agent().is_none()
                || parent.conversation().and_then(|state| state.agent) != scope.agent()
            {
                return Err(reject(
                    crate::wire::ErrorCode::Unauthorized,
                    "project controller requires the bound parent agent",
                ));
            }
            project.validate(quota.work())?;
            if project.provider() != &host.provider {
                return Err(reject(
                    crate::wire::ErrorCode::Invalid,
                    "parent project belongs to another provider",
                ));
            }
            let project = project.copy_in_quota(&mut quota)?;
            let _native_custody = quota.into_custody()?;
            Ok(Self {
                host,
                project,
                authority,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: _native_custody,
            })
        })
    }

    pub(in crate::filesystem) fn require(
        &self,
        capability: &str,
        operation: VolumeOperation,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work)
            && !self
                .native_custody
                .as_ref()
                .is_some_and(|allocation| allocation.work().shares_allocation_context(work))
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "parent project controller belongs to another original resource",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_authenticated_volume(
                &self.authority,
                &self.project,
                operation,
                u64::MAX,
                work,
            )
        })?;
        if !self.authority.scope().capabilities().contains(capability) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "parent project operation is not granted",
            ));
        }
        Ok(())
    }
}
mod inherited;
mod metadata;
mod project;
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) mod publication;
