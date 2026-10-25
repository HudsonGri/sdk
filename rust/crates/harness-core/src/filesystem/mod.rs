#![cfg_attr(
    test,
    allow(
        clippy::too_many_lines,
        clippy::indexing_slicing,
        reason = "adapter tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../../docs/filesystem.md")]
use crate::contract::capability;
use crate::{
    AgentId, Error, IdempotencyKey, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef,
        MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryEntry, PrivateDirectoryEntryKind,
        PrivateDirectoryPage, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        ForkPublicationGuard, ForkSeed, ForkSeedVerifier, InheritedConversationPrefix,
        ReferenceGrant, ResourceRevision,
    },
    merge::{ProjectMergeReceipt, ProjectMergeVerifier, ProviderJoinProof},
    resources::{GenerationRef, ProviderRef, WorkspaceRef},
};
use acyclic_fs::WorkCounters;
use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding};
use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ConflictSide, Digest, DirectoryPage, Fs,
    Generation, GenerationId, IdempotencyKey as FilesystemKey, JoinCommitWitness, JoinHistory,
    JoinOutcome, JoinPlan, MergeConflict, MergePlan, PublicationReservation, TransactionCommit,
    Workspace, WorkspaceError, WorkspaceStat,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};
const FILESYSTEM_JOIN_PROOF_FORMAT: &str = "acyclic.filesystem.join-commit.v2";
mod content_input;
use content_input::ContentInput;
mod content_publisher;
pub use content_publisher::FilesystemContentPublisher;
mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod fork_preparer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::AllocationClaim;
use fork_preparer::bounded_io as fork_preparer_io;
pub use fork_preparer::bounded_io::PreparationIo;
#[doc(hidden)]
pub use fork_preparer::bounded_io::PreparationRecord;
#[doc(hidden)]
pub use fork_preparer::bounded_io::UploadRecord;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::bounded_io::native_decoded;
pub(crate) use fork_preparer::bounded_io::{
    DecodeReservation, preparation_file_record_slot_bytes, preparation_slot_bytes, retain_decoded,
};
pub(crate) use fork_preparer_io::PreparationScope;
use fork_preparer_io::decode_record;
mod summary_fork;
pub use fork_preparer::{FilesystemForkPreparer, FilesystemResourceCreationProvider};
mod interaction_host;
pub use interaction_host::FilesystemInteractionHost;
mod project_workspaces;
pub use project_workspaces::FilesystemProjectWorkspaces;
mod workflow_journal;
pub use workflow_journal::FilesystemWorkflowJournal;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use workflow_journal::WorkflowReleaseWitness;
mod memory;
pub use memory::MemoryHarnessStorage;
mod text_tools;
#[cfg(not(target_arch = "wasm32"))]
pub use host::{
    bind_original_root_conversation_with_native_work, native_root_output_binding_operation,
};
pub use text_tools::ExactTextReplacement;
mod root_source;
pub use root_source::FsRootProjectSourceProvider;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_execution;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_execution::NativePreparationCompletionTemplate;
#[cfg(all(
    any(fuzzing, feature = "test-support"),
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use native_execution::{fuzz_native_preparation_intent, fuzz_native_preparation_journal};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod native_process;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_process::{NativeProcessAuthority, NativeProcessObserver, NativeProcessProvider};
mod native_capture_bounds;
mod native_contract;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod native_directory;
pub use native_contract::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeViewManifest, NativeViewOptions, NativeVolumeMapping,
    NativeVolumeNamespace,
};
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use native_directory::NativeDirectory;
mod configured_recursive_tools;
mod default_tools;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub use default_tools::InvocationCancellation;
mod mailbox_machine;
mod recursive;
mod task_commands;
mod task_runtime;
pub use configured_recursive_tools::{
    ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller, ConfiguredRecursiveFork,
    ConfiguredRecursiveMerge, ConfiguredRecursiveToolSource, ConfiguredRecursiveToolsConfig,
};
pub use default_tools::{MergeToolArguments, MergeToolSelection, MergeToolSide, recursive_tools};
pub(in crate::filesystem) use default_tools::{
    RecursiveForkSourceIntent, RecursiveForkToolBinding, RecursiveForkToolPlan,
    RecursiveMergeToolBinding, RecursiveMergeToolPlan,
};
pub use mailbox_machine::MailboxModelMachine;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_execution::journal::preparation_owner;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_execution::{
    BorrowedNativeVolumeBinding, NativeAccessAuthority, NativeMountAuthority, NativeMountedVolume,
    NativePreparationRoot, NativeViewPublicationError, NativeVolumeView,
    authenticated_mount_options, owner_mount_options, require_unrestricted_volume_access,
    require_unrestricted_workspace_read,
};
#[cfg(all(
    feature = "native-execution",
    feature = "filesystem-local",
    not(target_arch = "wasm32")
))]
pub use native_execution::{local_owner_mount_options, local_owner_workspace};
pub use recursive::{
    ForkPreparationLimits, ForkPreparationMachine, ForkToolArguments, RecursiveForkConfig,
    RecursiveSourceSnapshot, RetainedSourceBinding, fork_preparation_request,
    recursive_fork_source, restore_recursive_fork_source,
};
pub use task_commands::{
    EFFECT_TASK_COMMAND_KIND, FORK_PREPARE_TASK_COMMAND_KIND, FilesystemTaskCommands,
    MAIL_RECEIVE_TASK_COMMAND_KIND, MAIL_SEND_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND,
    MailReceiveTaskCommand, MailSendTaskCommand, ModelTaskCommand, PreparedTaskAdmitCommand,
    TASK_ADMIT_COMMAND_KIND, TASK_ADMIT_PREPARED_COMMAND_KIND, TASK_CANCEL_COMMAND_KIND,
    TASK_OBSERVE_COMMAND_KIND, TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskAdmitCommand,
    TaskCancelCommand, TaskObserveCommand, TimerTaskCommand, ToolTaskCommand,
};
pub use task_runtime::{
    FilesystemTaskExecution, FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress,
    TaskSourceInspection, TaskWakeCursor, TaskWakePage, TaskWorkerAttempt, TaskWorkerOutcome,
    TaskWorkerTick,
};
mod config;
use config::*;
pub use config::{FilesystemJournalBinding, WorkspaceMutation, WorkspaceObservation};
mod host;
pub(crate) use host::file_publication;
pub(crate) use host::map_error;
use host::*;
pub use host::{FilesystemHost, VolumeImportPlan, filesystem_key, workspace_ref};
pub(crate) mod access;
pub use access::INITIAL_POLICY as INITIAL_ACCESS_POLICY;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use access::check_access_record;
mod content_verifier;
pub use content_verifier::FilesystemContentVerifier;
mod file_query;
#[cfg(not(target_arch = "wasm32"))]
use content_verifier::require_file_read;
pub use file_query::FilesystemFileQuery;
mod fork_verifier;
pub use fork_verifier::FilesystemForkVerifier;
mod project_verifier;
pub use project_verifier::FilesystemProjectMergeVerifier;
mod parent;
pub use parent::{InheritedContextCapture, ParentMergePlan, ParentProjectController};
/// Pins a generation that a published file version names, under the one
/// label every unmeasured publication uses, so the version stays readable.
#[doc(hidden)]
pub async fn retain_published_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
) -> Result<()> {
    generation
        .retain_if_unpublished(
            format!(
                "harness-generation-{}",
                hex::encode(generation.id().digest().into_bytes())
            ),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|_| ())
        .map_err(|failure| map_error(failure.error))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    #[allow(
        clippy::too_many_arguments,
        reason = "one implementation pins the complete staged file contract"
    )]
    async fn put_content_impl<B: ContentInput + ?Sized>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        input: &B,
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        internal_class: Option<InternalContentClass>,
        expected_generation: Option<&GenerationRef>,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        let item = ContentItem {
            path,
            input,
            media_type,
            display_name,
        };
        let mut files = self
            .put_contents_impl(
                volume,
                grant,
                std::slice::from_ref(&item),
                maximum_bytes,
                idempotency_key,
                internal_class,
                expected_generation,
                ceiling,
                io,
            )
            .await?;
        files.pop().ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "content publication returned no file",
            )
        })
    }

    /// Publishes every item in one generation under one idempotency key: a
    /// group commit. Each item keeps its own path, metadata index and upload
    /// receipt, and every returned reference names the shared generation.
    /// Several items need the execution class, whose paths derive from each
    /// member's own retry key and whose receipt identity is that path.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one implementation pins the complete staged file contract"
    )]
    async fn put_contents_impl<B: ContentInput + ?Sized>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        items: &[ContentItem<'_, B>],
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        internal_class: Option<InternalContentClass>,
        expected_generation: Option<&GenerationRef>,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<Vec<FileRef>>
    where
        A: 'static,
        O: 'static,
    {
        grant.require(volume, VolumeOperation::Write, io)?;
        if let Some(grants) = ceiling
            && !crate::executor::with_history_io(io, |work| {
                crate::core::volume_write_granted_with_work(grants, volume, work)
            })?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a filesystem write grant",
            ));
        }
        // Execution payload paths derive from their own retry keys, so a
        // member keeps its identity whichever publication carries it.
        let keyed = matches!(internal_class, Some(InternalContentClass::Execution));
        let group = items.len() > 1;
        if items.is_empty()
            || (group && (!keyed || expected_generation.is_some()))
            || items
                .iter()
                .enumerate()
                .any(|(index, item)| items[..index].iter().any(|prior| prior.path == item.path))
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "a group commit needs distinct execution paths",
            ));
        }
        if volume.provider() != &self.provider
            || items
                .iter()
                .any(|item| item.input.as_ref().len() as u64 > maximum_bytes)
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content provider or size limit mismatch",
            ));
        }
        // Refuse an invalid path before anything is allocated for it.
        for item in items {
            crate::executor::with_history_io(io, |work| {
                crate::conversation::validate_content_path(item.path, work)
            })?;
        }
        let publication = ContentPublication {
            volume,
            idempotency_key,
            internal_class,
            expected_generation,
            maximum_bytes,
            io,
        };
        let mut operation = None;
        let mut staged = crate::model::vec_with_capacity(items.len(), io)?;
        for item in items {
            staged.push(self.stage_content_item(&publication, item, &mut operation)?);
        }
        let operation = operation.ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content publication has no items",
            )
        })?;
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let (opened, prior) = crate::conversation::BoundedContentFuture::new(io, || async {
            let owned;
            let io = if let Some(io) = io {
                io
            } else {
                owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &owned
            };
            let opened = self.open(&workspace, Some(io)).await?;
            let prior = io.sdk(
                opened
                    .operation_generation(operation, io, io.cancellation())
                    .await,
            )?;
            Ok::<_, Error>((opened, prior))
        })?
        .await?;
        let mut paths = crate::model::vec_with_capacity(staged.len(), io)?;
        for item in &staged {
            paths.push(item.provisional.path());
        }
        let (authorized_generation, policy_fence, head) = self
            .authorize_content_write(&opened, grant, &paths, prior.is_some(), ceiling, io)
            .await?;
        drop(paths);
        let generation_hold = io
            .map(|io| io.guard_owned(authorized_generation.logical_owned_bytes()?))
            .transpose()?;
        let authorized_generation = (authorized_generation, generation_hold);
        if let Some(prior) = prior {
            let generation = (self).generation_ref_id(prior.id(), io)?;
            fork_preparer_io::retain(&prior, io).await?;
            let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
            let mut references = crate::model::vec_with_capacity(items.len(), io)?;
            for (item, staged) in items.iter().zip(&staged) {
                let member = if keyed {
                    let own = (operation, Some(&prior));
                    self.published_member(&publication, &opened, &prior, item, own)
                        .await?
                } else {
                    None
                };
                references.push(match member {
                    Some(member) => member,
                    None => {
                        self.replayed_content_item(&publication, &prior, item, staged, &version)
                            .await?
                    }
                });
            }
            grant.recheck(io)?;
            return Ok(references);
        }
        // An execution path is unique to its member's own retry key, so a
        // member already present was staged by an earlier publication, which
        // may have grouped it differently: it is that publication's member,
        // proven identical, and only the absent members are published.
        let (head, present) = head.ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "content authorization returned no head",
            )
        })?;
        let mut resolved = crate::model::vec_with_capacity(items.len(), io)?;
        for (item, present) in items.iter().zip(&present) {
            resolved.push(
                if *present
                    && keyed
                    && expected_generation.is_none()
                    && !crate::executor::model_group_faults::adoption_refused()
                {
                    let own = (operation, None);
                    self.published_member(&publication, &opened, &head, item, own)
                        .await?
                } else {
                    None
                },
            );
        }
        drop((head, present));
        if resolved.iter().all(Option::is_some) {
            grant.recheck(io)?;
            return Ok(resolved.into_iter().flatten().collect());
        }
        if expected_generation.is_some_and(|expected| expected != &authorized_generation.0) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "workspace generation changed before content authorization",
            ));
        }
        let policy_fence = if expected_generation.is_some() {
            policy_fence.pin_generation()
        } else {
            policy_fence
        };
        let mut mutations = crate::model::vec_with_capacity(2 + 4 * staged.len(), io)?;
        for ((item, staged), _) in items
            .iter()
            .zip(&staged)
            .zip(&resolved)
            .filter(|(_, resolved)| resolved.is_none())
        {
            let absolute = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("/{}", staged.provisional.path()),
                    work,
                )
            })?;
            if let Some((parent, _)) = absolute.rsplit_once('/')
                && !parent.is_empty()
            {
                mutations.push(WorkspaceMutation::CreateDirectory {
                    path: stock_string_copy(parent, io)?,
                });
            }
            let owned_bytes = item.input.mutation_bytes(io)?;
            mutations.push(WorkspaceMutation::PutFile {
                path: absolute,
                bytes: owned_bytes,
            });
        }
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: stock_string_copy("/.system/harness-uploads", io)?,
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: stock_string_copy("/.system/harness-file-metadata", io)?,
        });
        let mut descriptors = crate::model::vec_with_capacity(staged.len(), io)?;
        for (staged, _) in staged
            .into_iter()
            .zip(&resolved)
            .filter(|(_, resolved)| resolved.is_none())
        {
            let StagedContentItem {
                descriptor,
                metadata_path,
                metadata_bytes,
                receipt_path,
                receipt_bytes,
                ..
            } = staged;
            descriptors.push(descriptor);
            mutations.push(WorkspaceMutation::PutFile {
                path: crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
                })?,
                bytes: metadata_bytes,
            });
            mutations.push(WorkspaceMutation::PutFile {
                path: crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
                })?,
                bytes: receipt_bytes,
            });
        }
        let generation = self
            .apply_authorized_generation(
                &workspace,
                (policy_fence, grant),
                &mutations,
                IdempotencyKey(stock_string_copy(idempotency_key.as_str(), io)?),
                io,
                None,
            )?
            .await?;
        let generation_reference = (self).generation_ref_id(generation.id(), io)?;
        let version = fork_preparer_io::hex_key(generation_reference.as_resource().key(), io)?;
        {
            match io {
                Some(io) => fork_preparer_io::retain(&generation, Some(io)).await?,
                None => retain_published_generation(&generation).await?,
            }
        }
        let mut references = crate::model::vec_with_capacity(items.len(), io)?;
        let mut descriptors = descriptors.iter();
        for (item, resolved) in items.iter().zip(resolved) {
            if let Some(reference) = resolved {
                references.push(reference);
                continue;
            }
            let descriptor = descriptors.next().ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "content publication lost a member",
                )
            })?;
            let reference = content_reference(volume, item, descriptor, &version, io)?;
            self.read_retained_file(&generation, &reference, maximum_bytes, io)
                .await?;
            references.push(reference);
        }
        grant.recheck(io)?;
        Ok(references)
    }

    /// Returns the member an earlier publication staged at this execution
    /// item's path, proven to be this item: its metadata index names that
    /// publication's retry key, and its receipt, index and content match this
    /// item staged under that key. `None` when no harness publication staged
    /// the path, so a raw file there proves nothing and is published over.
    /// `own` is this request's filesystem key and, on a replay, its
    /// publication, already retained.
    async fn published_member<B: ContentInput + ?Sized>(
        &self,
        publication: &ContentPublication<'_>,
        opened: &Workspace<A, O>,
        generation: &Generation<A, O>,
        item: &ContentItem<'_, B>,
        own: (acyclic_fs::IdempotencyKey, Option<&Generation<A, O>>),
    ) -> Result<Option<FileRef>> {
        let io = publication.io;
        let mismatch = || {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "upload metadata index does not match",
            )
        };
        let metadata_path = content_metadata_path_io(item.path, false, io)?;
        let indexed_path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
        })?;
        let indexed =
            match read_generation(generation, &indexed_path, CONTENT_RECORD_BYTES, io).await {
                Ok(indexed) => indexed,
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
        let indexed: FileRef = decode_record(&indexed, io)?;
        let mut origin = [0; 16];
        hex::decode_to_slice(indexed.version(), &mut origin).map_err(|_| mismatch())?;
        drop(indexed);
        let origin = acyclic_fs::IdempotencyKey::from_bytes(origin);
        let restaged = self.stage_content_item(publication, item, &mut Some(origin))?;
        let owned;
        let (prior, adopted) = if origin == own.0
            && let Some(retained) = own.1
        {
            (retained, false)
        } else {
            let found = crate::conversation::BoundedContentFuture::new(io, || async {
                let unmeasured;
                let io = if let Some(io) = io {
                    io
                } else {
                    unmeasured =
                        PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &unmeasured
                };
                io.sdk(
                    opened
                        .operation_generation(origin, io, io.cancellation())
                        .await,
                )
            })?
            .await?;
            let Some(found) = found else {
                return Ok(None);
            };
            owned = found;
            fork_preparer_io::retain(&owned, io).await?;
            (&owned, true)
        };
        let version = fork_preparer_io::hex_key(
            self.generation_ref_id(prior.id(), io)?.as_resource().key(),
            io,
        )?;
        let member = self
            .replayed_content_item(publication, prior, item, &restaged, &version)
            .await?;
        if adopted {
            crate::executor::model_group_faults::record_adoption();
        }
        Ok(Some(member))
    }

    /// Validates one item and encodes its metadata index and upload receipt.
    fn stage_content_item<B: ContentInput + ?Sized>(
        &self,
        publication: &ContentPublication<'_>,
        item: &ContentItem<'_, B>,
        operation: &mut Option<acyclic_fs::IdempotencyKey>,
    ) -> Result<StagedContentItem> {
        let ContentPublication {
            volume,
            idempotency_key,
            internal_class,
            expected_generation,
            io,
            ..
        } = *publication;
        let bytes = item.input.as_ref();
        let descriptor = crate::executor::with_history_io(io, |work| {
            FileDescriptor::from_bytes(bytes, item.media_type, work)
        })?;
        let operation = match *operation {
            Some(operation) => operation,
            None => *operation.insert(fork_preparer_io::operation_key(idempotency_key, io)?),
        };
        let provisional = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                volume.copy(work)?,
                stock_string_copy(item.path, io)?,
                fork_preparer_io::hex_key(&operation.into_bytes(), io)?,
                FileDescriptor::new(
                    *descriptor.sha256(),
                    descriptor.byte_length(),
                    stock_string_copy(descriptor.media_type(), io)?,
                )?,
                stock_string_copy(item.display_name, io)?,
                work,
            )
        })?;
        match internal_class {
            Some(class)
                if volume.class() == VolumeClass::AgentPrivate
                    && provisional.path().starts_with(class.prefix()) => {}
            Some(_) => {
                #[cfg(not(target_arch = "wasm32"))]
                if io.and_then(PreparationIo::original_native_work).is_some() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "journal path or volume is not assigned to this host",
                    });
                }
                return Err(Error::Unauthorized(
                    "journal path or volume is not assigned to this host".into(),
                ));
            }
            None if provisional.path() == ".system"
                || provisional.path().starts_with(".system/")
                || provisional.path() == "turns/.canonical"
                || provisional.path().starts_with("turns/.canonical/") =>
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "internal storage paths are reserved",
                ));
            }
            None => {}
        }
        let receipt_identity = if internal_class.is_some() {
            provisional.path()
        } else {
            idempotency_key.as_str()
        };
        let receipt_path =
            content_identity_path_io(receipt_identity, false, ".system/harness-uploads/", io)?;
        let metadata_path = content_metadata_path_io(provisional.path(), false, io)?;
        let metadata_bytes = match io {
            Some(io) => io.encode(&provisional, io.remaining()?.bytes_encoded, false)?,
            None => acyclic_stream::OwnedBytes::from(
                serde_json::to_vec(&provisional)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
        };
        let receipt_bytes = match io {
            Some(io) => io.encode(
                &(&provisional, expected_generation),
                io.remaining()?.bytes_encoded,
                true,
            )?,
            None => acyclic_stream::OwnedBytes::from(crate::contract::canonical_json_bytes(&(
                &provisional,
                expected_generation,
            ))?),
        };
        Ok(StagedContentItem {
            descriptor,
            provisional,
            receipt_path,
            metadata_path,
            metadata_bytes,
            receipt_bytes,
        })
    }

    /// Returns one item of a replayed publication after proving it unchanged.
    async fn replayed_content_item<B: ContentInput + ?Sized>(
        &self,
        publication: &ContentPublication<'_>,
        prior: &Generation<A, O>,
        item: &ContentItem<'_, B>,
        staged: &StagedContentItem,
        version: &str,
    ) -> Result<FileRef> {
        let ContentPublication {
            volume,
            maximum_bytes,
            io,
            ..
        } = *publication;
        let receipt_path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("/{}", staged.receipt_path), work)
        })?;
        let original_receipt =
            read_generation(prior, &receipt_path, CONTENT_RECORD_BYTES, io).await?;
        if original_receipt.as_ref() != staged.receipt_bytes.as_ref() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "upload identity belongs to a different request",
            ));
        }
        drop(original_receipt);
        let indexed_path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("/{}", staged.metadata_path), work)
        })?;
        let indexed =
            read_generation(prior, &indexed_path, staged.metadata_bytes.len() as u64, io).await?;
        if indexed.as_ref() != staged.metadata_bytes.as_slice() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "upload metadata index does not match",
            ));
        }
        let reference = content_reference(volume, item, &staged.descriptor, version, io)?;
        let committed = self
            .read_retained_file(prior, &reference, maximum_bytes, io)
            .await?;
        if committed.as_ref() != item.input.as_ref() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "upload identity belongs to different content",
            ));
        }
        Ok(reference)
    }
    async fn read_retained_file(
        &self,
        generation: &Generation<A, O>,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        pinned_read_tests::retained::sample_items(false, io);
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if pinned_read_tests::retained::reopen() {
            let result = self.read_pinned_io(reference, maximum_bytes, io).await;
            pinned_read_tests::retained::sample_items(true, io);
            return result;
        }
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            reference.validate(&mut Some(&mut io.history_io()))?;
        }
        if reference.volume().provider() != &self.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "content provider or size limit mismatch",
                });
            }
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        let (_path, path) = {
            let path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
            })?;
            let hold = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            (hold, path)
        };
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("read", maximum_bytes));
        let bytes = read_generation(generation, &path, maximum_bytes, io).await?;
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        pinned_read_tests::retained::sample_items(true, io);
        Ok(bytes)
    }
    async fn read_pinned_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        // Keep the Result temporary across await for the original admitted caller frame.
        (if io.is_none() {
            crate::conversation::BoundedContentFuture::new(None, || {
                self.read_pinned_unmetered(reference, maximum_bytes, pinned_read::bytes_only)
            })
        } else {
            crate::conversation::BoundedContentFuture::new(io, || {
                self.read_pinned_metered(reference, maximum_bytes, io)
            })
        })?
        .await
    }

    async fn read_pinned_receipt_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<PinnedRead<A, O>> {
        if io.is_none() {
            crate::conversation::BoundedContentFuture::new(None, || {
                self.read_pinned_unmetered(reference, maximum_bytes, PinnedRead::from_read)
            })?
            .await
        } else {
            // Metered reads already pin before descriptor verification. Project
            // their bytes after awaiting the unchanged admitted factory.
            Ok(PinnedRead::retained(
                self.read_pinned_io(reference, maximum_bytes, io).await?,
            ))
        }
    }

    fn read_pinned_unmetered<'a, T, F>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        project: F,
    ) -> impl std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a
    where
        F: FnOnce(acyclic_stream::OwnedBytes, Option<Generation<A, O>>) -> T
            + acyclic_stream::ProviderTask
            + 'a,
    {
        async move {
            if reference.volume().provider() != &self.provider
                || reference.descriptor().byte_length() > maximum_bytes
            {
                return Err(Error::Invalid(
                    "content provider or size limit mismatch".into(),
                ));
            }
            let generation = self.file_generation(reference, None)?;
            let workspace =
                fork_preparer_io::content_workspace(&self.provider, reference.volume(), None)?;
            let path = format!("/{}", reference.path());
            // Keep the authenticated immutable handle from the read. A
            // retaining caller can pin it without opening the workspace
            // and authenticating this same generation a second time.
            #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
            acyclic_differential::record(("read", maximum_bytes));
            let owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            let opened = self.open(&workspace, Some(&owned)).await?;
            let pinned = self.generation(&opened, &generation, Some(&owned)).await?;
            let bytes = owned.sdk(
                pinned
                    .read(&path, maximum_bytes, &owned, owned.cancellation())
                    .await,
            )?;
            owned.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
            reference.descriptor().verify(&bytes, &mut None)?;
            Ok(project(bytes, Some(pinned)))
        }
    }

    fn read_pinned_metered<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a fork_preparer_io::PreparationIo>,
    ) -> impl std::future::Future<Output = Result<acyclic_stream::OwnedBytes>>
    + acyclic_stream::ProviderTask
    + 'a {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = io.and_then(PreparationIo::original_native_work) {
                let scratch = PreparationIo::from_native_work(original)?;
                let io = &scratch;
                reference.validate(&mut Some(&mut io.history_io()))?;
                if reference.volume().provider() != &self.provider
                    || reference.descriptor().byte_length() > maximum_bytes
                {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "content provider or size limit mismatch",
                    });
                }
                let generation = self.file_generation(reference, Some(io))?;
                let workspace = fork_preparer_io::content_workspace(
                    &self.provider,
                    reference.volume(),
                    Some(io),
                )?;
                let rendered = original
                    .display(&format_args!("/{}", reference.path()))
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
                let path_custody = rendered.1;
                let path = rendered.0;
                let opened = (self).open(&workspace, Some(io)).await?;
                let pinned = self.generation(&opened, &generation, Some(io)).await?;
                #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
                acyclic_differential::record(("read", maximum_bytes));
                let bytes = pinned
                    .read(&path, maximum_bytes, original, io.cancellation())
                    .await
                    .map_err(|source| Error::NativeWorkspace {
                        source: Box::new(source),
                        custody: None,
                    })?;
                fork_preparer_io::retain(&pinned, Some(io)).await?;
                crate::executor::with_history_io(Some(io), |work| {
                    reference.descriptor().verify(&bytes, work)
                })?;
                drop(path);
                drop(path_custody);
                return Ok(bytes);
            }
            if reference.volume().provider() != &self.provider
                || reference.descriptor().byte_length() > maximum_bytes
            {
                return Err(Error::Invalid(
                    "content provider or size limit mismatch".into(),
                ));
            }
            if let Some(io) = io {
                io.retain_owned((reference.path().len() + 1) as u64)?;
            }
            let _path_hold = io
                .map(|io| io.guard_owned((reference.path().len() + 1) as u64))
                .transpose()?;
            let generation = self.file_generation(reference, io)?;
            let generation_hold = io
                .map(|io| io.guard_owned(generation.logical_owned_bytes()?))
                .transpose()?;
            let workspace =
                fork_preparer_io::content_workspace(&self.provider, reference.volume(), io)?;
            let _workspace_hold = io
                .map(|io| io.guard_owned(workspace.logical_owned_bytes()?))
                .transpose()?;
            let path = format!("/{}", reference.path());
            let bytes = match io {
                Some(io) => {
                    let opened = (self).open(&workspace, Some(io)).await?;
                    let pinned = self.generation(&opened, &generation, Some(io)).await?;
                    #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
                    acyclic_differential::record(("read", maximum_bytes));
                    let bytes = io.receive(
                        pinned
                            .read(&path, maximum_bytes, io.remaining()?, io.cancellation())
                            .await,
                    )?;
                    let capacity = bytes.retained_capacity().ok_or_else(|| {
                        stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "content producer does not expose its owned capacity",
                        )
                    })?;
                    io.retain_owned(capacity as u64)?;
                    fork_preparer_io::retain(&pinned, Some(io)).await?;
                    bytes
                }
                None => {
                    self.read(&workspace, Some(&generation), &path, maximum_bytes, None)
                        .await?
                }
            };
            crate::executor::with_history_io(io, |work| {
                reference.descriptor().verify(&bytes, work)
            })?;
            drop(generation);
            drop(generation_hold);
            Ok(bytes)
        }
    }
}

mod pinned_read;
use pinned_read::PinnedRead;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod pinned_read_tests;

mod stock_turn;
pub use stock_turn::StockTurnMachine;
#[doc(hidden)]
pub mod child_agent;
pub use child_agent::ChildAgentMachine;

use crate::executor::{stock_boundary, stock_string_copy};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) mod workspace_policy;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) mod workspace_publication;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use workspace_publication::{
    CheckoutRoot, WorkspacePublicationCoordinator, WorkspacePublicationPublisher,
    WorkspaceRootMaterializer, workspace_publication_coordinator,
};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod agent_publication;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use agent_publication::{plan_agent_merge, record_agent_publication};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod checkout_publication;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use checkout_publication::CheckoutPublication;

pub use host::DirectoryOptions;

/// One content publication's request: what every one of its items shares.
#[derive(Clone, Copy)]
struct ContentPublication<'p> {
    volume: &'p VolumeRef,
    idempotency_key: &'p IdempotencyKey,
    internal_class: Option<InternalContentClass>,
    expected_generation: Option<&'p GenerationRef>,
    maximum_bytes: u64,
    io: Option<&'p PreparationIo>,
}

/// The most bytes one upload receipt or metadata index occupies.
const CONTENT_RECORD_BYTES: u64 = 6
    * (crate::conversation::MAX_PATH_BYTES as u64
        + 32 * crate::conversation::MAX_LABEL_BYTES as u64
        + 1024);

/// One file of a content publication.
pub(super) struct ContentItem<'a, B: ?Sized> {
    pub path: &'a str,
    pub input: &'a B,
    pub media_type: &'a str,
    pub display_name: &'a str,
}

/// An item's validated identity and its encoded index records.
struct StagedContentItem {
    descriptor: FileDescriptor,
    provisional: FileRef,
    receipt_path: String,
    metadata_path: String,
    metadata_bytes: acyclic_stream::OwnedBytes,
    receipt_bytes: acyclic_stream::OwnedBytes,
}

/// The published reference to one item of a generation.
fn content_reference<B: ?Sized>(
    volume: &VolumeRef,
    item: &ContentItem<'_, B>,
    descriptor: &FileDescriptor,
    version: &str,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    crate::executor::with_history_io(io, |work| {
        FileRef::new(
            volume.copy(work)?,
            stock_string_copy(item.path, io)?,
            stock_string_copy(version, io)?,
            FileDescriptor::new(
                *descriptor.sha256(),
                descriptor.byte_length(),
                stock_string_copy(descriptor.media_type(), io)?,
            )?,
            stock_string_copy(item.display_name, io)?,
            work,
        )
    })
}
