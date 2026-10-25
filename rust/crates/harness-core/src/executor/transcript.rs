//! Immutable dispatched-request segments and stable inline projections.
use super::*;

mod digests;
mod projection;

pub(crate) use projection::load_projection;

/// One file per continuation call: the call's own header (unless shared)
/// and new messages, linked to the earlier calls' manifests.
pub(crate) const REQUEST_MEDIA: &str = "application/vnd.acyclic.request-manifest+json";

#[cfg(all(
    test,
    feature = "filesystem",
    not(target_arch = "wasm32"),
    not(acyclic_harness_loom)
))]
mod tests;

#[cfg(all(
    test,
    feature = "filesystem",
    not(target_arch = "wasm32"),
    not(acyclic_harness_loom)
))]
mod metadata_tests;

/// A continuation request, published as exactly one file. It carries its
/// header inline unless an earlier manifest already holds the same header,
/// and its new messages after a prefix of earlier manifests. Every reference
/// names an already published manifest, so one call is one publication.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestManifest {
    header: Option<RequestHeader>,
    shared_header: Option<FileRef>,
    header_digest: [u8; 32],
    messages: MessageSegment,
    count: u32,
    content_digest: [u8; 32],
    request_digest: [u8; 32],
    admitted_limits: Limits,
    proof: Option<([u8; 32], [u8; 32])>,
}

#[cfg(test)]
thread_local! {
    /// Negative control: publishes the header on its own first, as the
    /// former separate-part format did.
    pub(crate) static STAGE_HEADER_APART: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[derive(Serialize)]
struct ManifestView<'a> {
    header: Option<&'a HeaderView<'a>>,
    shared_header: Option<&'a FileRef>,
    header_digest: [u8; 32],
    messages: SegmentView<'a>,
    count: u32,
    content_digest: [u8; 32],
    request_digest: [u8; 32],
    admitted_limits: Limits,
    proof: Option<([u8; 32], [u8; 32])>,
}

/// The attested content of a manifest: every field but the proof.
#[cfg(feature = "filesystem")]
fn manifest_payload_digest<H: Serialize, M: Serialize>(
    (header, shared_header): (Option<&H>, Option<&FileRef>),
    header_digest: [u8; 32],
    messages: &M,
    (count, content_digest): (u32, [u8; 32]),
    (request_digest, admitted_limits): ([u8; 32], Limits),
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    preparation_digest(
        &(
            header,
            shared_header,
            header_digest,
            messages,
            count,
            content_digest,
            request_digest,
            admitted_limits,
        ),
        io,
    )
}

#[cfg(feature = "filesystem")]
fn request_proof(
    digest: [u8; 32],
    verifier: &crate::core::AuthorityVerifier,
    io: Option<&PreparationIo>,
) -> Result<([u8; 32], [u8; 32])> {
    let signature = with_history_io(io, |work| verifier.model_request_proof(digest, work))?;
    let mut first = [0; 32];
    let mut second = [0; 32];
    first.copy_from_slice(&signature[..32]);
    second.copy_from_slice(&signature[32..]);
    Ok((first, second))
}

impl RequestManifest {
    /// The prefix a later manifest links to: this manifest when it adds
    /// messages, else its own parent, so no link has an empty suffix.
    fn prefix(
        self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<Option<MessagePrefix>> {
        if self.messages.suffix.is_empty() {
            return Ok(self.messages.parent);
        }
        Ok(Some(MessagePrefix {
            chain_digest: chain_digest(self.messages.parent.as_ref(), reference, io)?,
            reference: crate::model::clone_file(reference, io)?,
            count: self.count,
            content_digest: self.content_digest,
        }))
    }
}

/// Admission is monotone in these bounds. Control-loop limits do not affect
/// `ModelRequest` validation and therefore are not part of this comparison.
#[cfg(feature = "filesystem")]
pub(crate) fn limits_admit(admitted: Limits, current: Limits) -> bool {
    admitted.file_bytes <= current.file_bytes
        && admitted.path_bytes <= current.path_bytes
        && admitted.attachments <= current.attachments
        && admitted.render_bytes <= current.render_bytes
        && admitted.context_messages <= current.context_messages
}

#[cfg(feature = "filesystem")]
impl RequestManifest {
    fn payload_digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        manifest_payload_digest(
            (self.header.as_ref(), self.shared_header.as_ref()),
            self.header_digest,
            &self.messages,
            (self.count, self.content_digest),
            (self.request_digest, self.admitted_limits),
            io,
        )
    }

    pub(crate) fn authenticates(
        &self,
        verifier: &crate::core::AuthorityVerifier,
        limits: Limits,
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        if self.request_digest != digest || !limits_admit(self.admitted_limits, limits) {
            return Ok(false);
        }
        let Some((first, second)) = self.proof else {
            return Ok(false);
        };
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(&first);
        signature[32..].copy_from_slice(&second);
        let digest = self.payload_digest(io)?;
        with_history_io(io, |work| {
            verifier.authenticates_model_request(digest, &signature, work)
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestHeader {
    model: Model,
    tools: Vec<crate::tool::ToolDefinition>,
    max_output_tokens: Option<u32>,
}

#[derive(Serialize)]
struct HeaderView<'a> {
    model: &'a Model,
    tools: &'a [crate::tool::ToolDefinition],
    max_output_tokens: Option<u32>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagePrefix {
    reference: FileRef,
    count: u32,
    content_digest: [u8; 32],
    chain_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageSegment {
    parent: Option<MessagePrefix>,
    suffix: Vec<ModelMessage>,
}

#[derive(Serialize)]
struct SegmentView<'a> {
    parent: Option<&'a MessagePrefix>,
    suffix: &'a [ModelMessage],
}

#[derive(Serialize)]
pub(super) struct ProjectionView<'a> {
    context: ContextView<'a>,
    canonical: &'a Option<FileRef>,
    checkpoint: &'a Option<FileRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    working: &'a Option<FileRef>,
}

#[derive(Serialize)]
struct ContextView<'a> {
    messages: &'a [ModelMessage],
    metadata: &'a std::collections::BTreeMap<String, FileRef>,
    current_input_index: Option<u32>,
}

#[cfg(feature = "filesystem")]
impl PreparationRecord for RequestManifest {
    fn container_collection_slots() -> bool {
        // As for a context: message and part headers have object encodings,
        // and arbitrary Values keep their schema-based inventory.
        true
    }

    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::RequestManifest
    }

    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        // An inline header keeps the conservative decode custody; a manifest
        // that shares its header retains exactly its links and messages.
        if self.header.is_some() {
            return Ok(None);
        }
        let mut bytes = crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            crate::core::owned_buffer::<ModelMessage>(self.messages.suffix.capacity())?,
        ])?;
        for file in self.shared_header.iter().chain(
            self.messages
                .parent
                .as_ref()
                .map(|parent| &parent.reference),
        ) {
            bytes = crate::core::owned_sum([bytes, file.logical_owned_bytes()?])?;
        }
        self.messages
            .suffix
            .iter()
            .try_fold(bytes, |bytes, message| {
                crate::core::owned_sum([bytes, message.content.retained_payload_bytes()?])
            })
            .map(Some)
    }
}
#[cfg(feature = "filesystem")]
impl PreparationRecord for RequestHeader {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ModelRequest
    }
}

fn invalid(io: Option<&PreparationIo>) -> Error {
    stock_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "invalid segmented model request",
    )
}

fn chain_digest(
    parent: Option<&MessagePrefix>,
    suffix: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    // Fixed-size chaining commits to the linked manifest's exact bytes and
    // the entire preceding chain without re-hashing any historical payload.
    let mut frame = [0; 65];
    if let Some(parent) = parent {
        frame[0] = 1;
        frame[1..33].copy_from_slice(&parent.chain_digest);
    }
    frame[33..].copy_from_slice(suffix.descriptor().sha256());
    crate::model::hash_bytes(&frame, io)
}

pub(crate) async fn load_manifest(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<RequestManifest> {
    validate_journal(journal, io)?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != REQUEST_MEDIA
    {
        return Err(invalid(io));
    }
    let bytes = journal.load(reference, io)?.await?;
    #[cfg(feature = "filesystem")]
    let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    retain_preparation(super::payload::decode_json::<RequestManifest>(&bytes, io)?)
}

async fn stage_manifest(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    manifest: &ManifestView<'_>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    validate_journal(journal, io)?;
    #[cfg(feature = "filesystem")]
    let _hold;
    let bytes: acyclic_stream::OwnedBytes = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => {
            let bytes = io.encode(manifest, io.remaining()?.bytes_encoded, true)?;
            _hold = io.guard_bytes(&bytes)?;
            bytes
        }
        _ => crate::contract::canonical_json_bytes(manifest)?.into(),
    };
    let reference = journal
        .stage(operation, key, &bytes, REQUEST_MEDIA, io)?
        .await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != REQUEST_MEDIA
    {
        return Err(invalid(io));
    }
    with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    Ok(reference)
}

struct RequestStage<'a> {
    operation: OperationId,
    key: &'a str,
    prepared: &'a crate::model::PreparedModelRequest,
    previous: Option<&'a FileRef>,
}

async fn previous_manifest(
    journal: &dyn ExecutionJournal,
    previous: Option<&FileRef>,
    limits: Limits,
    verifier: Option<&crate::core::AuthorityVerifier>,
    io: Option<&PreparationIo>,
) -> Result<Option<RequestManifest>> {
    let _ = verifier;
    Ok(match previous {
        Some(reference) if reference.descriptor().media_type() == REQUEST_MEDIA => {
            let manifest = load_manifest(journal, reference, io).await?;
            #[cfg(feature = "filesystem")]
            let authenticated = verifier
                .map(|verifier| {
                    manifest.authenticates(verifier, limits, manifest.request_digest, io)
                })
                .transpose()?
                .unwrap_or(false);
            #[cfg(not(feature = "filesystem"))]
            let authenticated = false;
            if !authenticated {
                // An untrusted descriptor is not evidence that its claimed
                // prefix was admitted. Reconstruct and validate before reuse.
                load_request(journal, reference, limits, io).await?;
            }
            Some(manifest)
        }
        _ => None,
    })
}

async fn stage_request_inner(
    journal: &dyn ExecutionJournal,
    staging: RequestStage<'_>,
    verifier: Option<&crate::core::AuthorityVerifier>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let RequestStage {
        operation,
        key,
        prepared,
        previous,
    } = staging;
    let request = prepared.request();
    // Without an existing prefix there is nothing to share: the initial call
    // is one private file of the whole request.
    let Some(previous_ref) = previous else {
        return stage_json(journal, operation, key, request, io).await;
    };
    let header = HeaderView {
        model: &request.model,
        tools: &request.tools,
        max_output_tokens: request.max_output_tokens,
    };
    let header_digest = preparation_digest(&header, io)?;
    #[cfg(test)]
    if STAGE_HEADER_APART.get() {
        let key = format!("{key}:header");
        stage_json(journal, operation, &key, &header, io).await?;
    }
    let previous = previous_manifest(
        journal,
        Some(previous_ref),
        prepared.admitted_limits(),
        verifier,
        io,
    )
    .await?;
    let (shared_header, parent) = match previous {
        Some(previous) => {
            let shared = if previous.header_digest == header_digest {
                Some(match &previous.shared_header {
                    Some(shared) => crate::model::clone_file(shared, io)?,
                    None => crate::model::clone_file(previous_ref, io)?,
                })
            } else {
                None
            };
            (shared, previous.prefix(previous_ref, io)?)
        }
        None => (None, None),
    };
    let messages = &request.messages;
    let previous_count = parent.as_ref().map(|parent| parent.count as usize);
    let (content_digest, prefix_digest) =
        match prepared.cached_message_digests(previous_count, io)? {
            Some(digests) => digests,
            None => digests::messages(messages, previous_count, io)?,
        };
    let parent = parent.filter(|parent| Some(parent.content_digest) == prefix_digest);
    let count = u32::try_from(messages.len()).map_err(|_| invalid(io))?;
    let parent_count = parent.as_ref().map_or(0, |parent| parent.count as usize);
    let suffix = messages.get(parent_count..).ok_or_else(|| invalid(io))?;
    let (request_digest, admitted_limits) = (prepared.request_digest(), prepared.admitted_limits());
    let inline_header = shared_header.is_none().then_some(&header);
    let segment = SegmentView {
        parent: parent.as_ref(),
        suffix,
    };
    #[cfg(feature = "filesystem")]
    let proof = verifier
        .map(|verifier| {
            let digest = manifest_payload_digest(
                (inline_header, shared_header.as_ref()),
                header_digest,
                &segment,
                (count, content_digest),
                (request_digest, admitted_limits),
                io,
            )?;
            request_proof(digest, verifier, io)
        })
        .transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let proof = {
        let _ = verifier;
        None
    };
    let manifest = ManifestView {
        header: inline_header,
        shared_header: shared_header.as_ref(),
        header_digest,
        messages: segment,
        count,
        content_digest,
        request_digest,
        admitted_limits,
        proof,
    };
    stage_manifest(journal, operation, key, &manifest, io).await
}

async fn load_request_inner(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::PreparedModelRequest> {
    let manifest = load_manifest(journal, reference, io).await?;
    if manifest.count == 0 || manifest.count as usize > limits.context_messages {
        return Err(invalid(io));
    }
    let header = match (manifest.header, &manifest.shared_header) {
        (Some(header), None) => header,
        (None, Some(shared)) => {
            let holder = load_manifest(journal, shared, io).await?;
            match (holder.header, holder.shared_header) {
                (Some(header), None) => header,
                _ => return Err(invalid(io)),
            }
        }
        _ => return Err(invalid(io)),
    };
    if preparation_digest(&header, io)? != manifest.header_digest {
        return Err(invalid(io));
    }
    let expected_count = manifest.count as usize;
    let expected_content = manifest.content_digest;
    let parent_count =
        |parent: &Option<MessagePrefix>| parent.as_ref().map_or(0, |parent| parent.count as usize);
    if parent_count(&manifest.messages.parent).checked_add(manifest.messages.suffix.len())
        != Some(expected_count)
    {
        return Err(invalid(io));
    }
    let mut cursor = manifest.messages.parent;
    let mut segments = Vec::new();
    stock_vec_push(&mut segments, manifest.messages.suffix, io)?;
    while let Some(prefix) = cursor {
        let (count, segment) = load_link(journal, &prefix.reference, io).await?;
        if segment.suffix.is_empty()
            || count != prefix.count
            || parent_count(&segment.parent).checked_add(segment.suffix.len())
                != Some(prefix.count as usize)
            || chain_digest(segment.parent.as_ref(), &prefix.reference, io)? != prefix.chain_digest
        {
            return Err(invalid(io));
        }
        cursor = segment.parent;
        stock_vec_push(&mut segments, segment.suffix, io)?;
    }
    let mut messages = Vec::new();
    stock_vec_reserve(&mut messages, expected_count, io)?;
    for segment in segments.into_iter().rev() {
        for message in segment {
            stock_vec_push(&mut messages, message, io)?;
        }
    }
    if digests::messages(&messages, None, io)?.0 != expected_content {
        return Err(invalid(io));
    }
    let prepared = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            model: header.model,
            tools: header.tools,
            max_output_tokens: header.max_output_tokens,
            messages,
        },
        limits,
        io,
    )?;
    if prepared.request_digest() != manifest.request_digest {
        return Err(invalid(io));
    }
    Ok(prepared)
}

/// One earlier manifest's message count and link, keeping only its funded
/// cursor and messages rather than its decoder or header.
async fn load_link(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<(u32, MessageSegment)> {
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(io)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    let manifest = load_manifest(journal, reference, io).await?;
    #[cfg(feature = "filesystem")]
    {
        if io.is_none() {
            return Ok((manifest.count, manifest.messages));
        }
        // Serde's admission reserves scratch for its largest possible typed
        // alternative. Release it after each link, retaining only funded
        // message and cursor copies rather than a decoder per historical turn.
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let parent = manifest
            .messages
            .parent
            .as_ref()
            .map(|parent| {
                Ok::<_, Error>(MessagePrefix {
                    reference: crate::model::clone_file(&parent.reference, io)?,
                    count: parent.count,
                    content_digest: parent.content_digest,
                    chain_digest: parent.chain_digest,
                })
            })
            .transpose()?;
        let suffix = crate::model::clone_messages(&manifest.messages.suffix, io)?;
        let count = manifest.count;
        drop(manifest);
        let retained = io.map_or(Ok(0), |io| {
            io.owned_bytes()?
                .checked_sub(before)
                .ok_or_else(|| invalid(Some(io)))
        })?;
        scope.finish(retained)?;
        Ok((count, MessageSegment { parent, suffix }))
    }
    #[cfg(not(feature = "filesystem"))]
    Ok((manifest.count, manifest.messages))
}

pub(super) async fn stage_request(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    prepared: &crate::model::PreparedModelRequest,
    previous: Option<&FileRef>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    if let Some(staging) =
        journal.stage_prepared_model_request(operation, key, prepared, previous, io)?
    {
        return staging.await;
    }
    stage_scoped_request(
        journal,
        RequestStage {
            operation,
            key,
            prepared,
            previous,
        },
        None,
        io,
    )
    .await
}

#[cfg(any(test, feature = "filesystem"))]
pub(crate) async fn stage_attested_request(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    prepared: &crate::model::PreparedModelRequest,
    previous: Option<&FileRef>,
    verifier: &crate::core::AuthorityVerifier,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    stage_scoped_request(
        journal,
        RequestStage {
            operation,
            key,
            prepared,
            previous,
        },
        Some(verifier),
        io,
    )
    .await
}

async fn stage_scoped_request(
    journal: &dyn ExecutionJournal,
    staging: RequestStage<'_>,
    verifier: Option<&crate::core::AuthorityVerifier>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(io)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    let reference = stage_request_inner(journal, staging, verifier, io).await?;
    #[cfg(feature = "filesystem")]
    scope.finish(reference.logical_owned_bytes()?)?;
    Ok(reference)
}

pub(crate) async fn load_request(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::PreparedModelRequest> {
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(io)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    let request = load_request_inner(journal, reference, limits, io).await?;
    #[cfg(feature = "filesystem")]
    {
        let retained = io.map_or(Ok(0), |io| request.preparation_owned_bytes(io))?;
        scope.finish(retained)?;
    }
    Ok(request)
}

pub(super) async fn stage_projection(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    projection: &ResponseProjection,
    prepared: &crate::model::PreparedModelRequest,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let view = projection_view(projection, prepared, io)?;
    stage_json(journal, operation, key, &view, io).await
}

/// The persisted form of a projection, validated against its request.
pub(super) fn projection_view<'a>(
    projection: &'a ResponseProjection,
    prepared: &'a crate::model::PreparedModelRequest,
    io: Option<&PreparationIo>,
) -> Result<ProjectionView<'a>> {
    // This is the sole persisted projection schema, including main's inline
    // context. Validate the combined value before either its event or request
    // can be published; per-message admission is insufficient.
    let messages = &prepared.request().messages;
    crate::context::validate_projected_messages(
        &projection.context,
        messages,
        prepared.admitted_limits(),
        io,
    )?;
    Ok(ProjectionView {
        context: ContextView {
            messages,
            metadata: &projection.context.metadata,
            current_input_index: projection.context.current_input_index,
        },
        canonical: &projection.canonical,
        checkpoint: &projection.checkpoint,
        working: &projection.working,
    })
}

/// Read checkpoint metadata without reconstructing the historical request.
pub(super) async fn working_reference(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    original: Option<&PreparationIo>,
) -> Result<Option<FileRef>> {
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(original)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    #[cfg(not(feature = "filesystem"))]
    let io = original;
    let stored =
        retain_preparation(load_json::<ResponseProjection>(journal, reference, io).await?)?;
    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    let working = stored
        .working
        .as_ref()
        .map(|file| crate::model::clone_file(file, io))
        .transpose()?;
    drop(stored);
    #[cfg(feature = "filesystem")]
    {
        let retained = io.map_or(Ok(0), |io| {
            io.owned_bytes()?
                .checked_sub(before)
                .ok_or_else(|| invalid(Some(io)))
        })?;
        scope.finish(retained)?;
    }
    Ok(working)
}

impl StockExecution<'_> {
    pub(super) async fn stage_response_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        key: &str,
        prepared: &crate::model::PreparedModelRequest,
    ) -> Result<FileRef> {
        let previous = if let Some(previous) = step.checked_sub(1) {
            self.model_records(
                journal,
                input.operation_id,
                previous,
                ModelPurpose::Response,
            )
            .await?
            .1
        } else {
            Vec::new()
        };
        let previous = previous.iter().find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                request,
                purpose: ModelPurpose::Response,
                ..
            } => Some(request),
            _ => None,
        });
        stage_request(
            journal,
            input.operation_id,
            key,
            prepared,
            previous,
            self.io,
        )
        .await
    }
}
