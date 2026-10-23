//! Heap layout of a decoded checkpoint, one node per JSON position.
//!
//! Every collection, box and tagged enum of `Snapshot<ConversationStateWire>`
//! is described here with the `size_of` of the slot its decode allocates.
//! Records list only fields that hold containers; any other container is
//! undescribed and charged at the largest logical slot.
use super::*;
use crate::conversation::Attachment;
use crate::executor::json::{Alloc, JsonLayout};
use crate::fork::{CapturedResource, ForkOmission, ReferenceGrant, SharedGrant};
use uuid::Uuid;

type L = JsonLayout;

/// `[u8; 32]` digests, tuples of scalars and other fixed arrays.
static FIXED: L = L::tuple(&[]);
/// Records whose fields are all scalars.
static SCALARS: L = L::record(&[]);
static BYTES: L = L::list(Alloc::vec::<u8>(), &[&SCALARS]);
static STRINGS: L = L::list(Alloc::vec::<String>(), &[&SCALARS]);
static UUIDS: L = L::list(Alloc::vec::<Uuid>(), &[&SCALARS]);
static VALUE: L = L::map(Alloc::tree::<String, Value>(), &[("*", &VALUE)])
    .or_list(Alloc::vec::<Value>(), &[&VALUE]);

static AUTHORITY: L = L::record(&[]);
static CAPABILITIES: L = L::list(Alloc::tree::<String, ()>(), &[&SCALARS]);
static EVENT_REFERENCE: L = L::record(&[("authority", &AUTHORITY)]);

static PROVIDER: L = L::record(&[]);
static VOLUME_OWNER: L = L::tagged(&[]);
static VOLUME: L = L::record(&[("provider", &PROVIDER), ("owner", &VOLUME_OWNER)]);
static DESCRIPTOR: L = L::record(&[("sha256", &FIXED)]);
static FILE: L = L::record(&[("volume", &VOLUME), ("descriptor", &DESCRIPTOR)]);
static BOXED_FILE: L = L::boxed(
    Alloc::boxed::<FileRef>(),
    &[("volume", &VOLUME), ("descriptor", &DESCRIPTOR)],
);
static FILES: L = L::list(Alloc::vec::<FileRef>(), &[&FILE]);
static RESOURCE: L = L::record(&[("provider", &PROVIDER), ("key", &BYTES)]);

/// `ExtensionRecord` and `ExtensionConfiguration`.
const EXTENSION_FIELDS: &[(&str, &L)] = &[
    ("extension", &SCALARS),
    ("schema_digest", &FIXED),
    ("implementation_digest", &FIXED),
    ("content", &FILE),
];
static EXTENSION: L = L::record(EXTENSION_FIELDS);
static DEPENDENCIES: L = L::list(Alloc::vec::<ExtensionDependency>(), &[&SCALARS]);
static CONFIGURATIONS: L = L::list(Alloc::vec::<ExtensionConfiguration>(), &[&EXTENSION]);
static MIGRATION: L = L::record(&[("previous", &EVENT_REFERENCE), ("record", &EXTENSION)]);

static EFFECT_STATUS: L = L::tagged(&[("result", &FILE)]);
static ATTESTATION: L = L::record(&[
    ("request_digest", &FIXED),
    ("result_schema_digest", &FIXED),
    ("status", &EFFECT_STATUS),
    ("proof", &FIXED),
]);

/// Every `ResourceRevision` variant's reference.
static REVISION_REFERENCE: L = L::record(&[
    ("provider", &PROVIDER),
    ("key", &BYTES),
    ("owner", &VOLUME_OWNER),
    ("volume", &VOLUME),
    ("generation", &RESOURCE),
    ("paths", &STRINGS),
    ("implementation_digest", &FIXED),
    ("reference", &RESOURCE),
]);
static REVISION: L = L::tagged(&[("reference", &REVISION_REFERENCE)]);
static CAPTURED: L = L::record(&[("source", &REVISION), ("revision", &REVISION)]);
static CAPTURE: L = L::tagged(&[("value", &CAPTURED)]);
static SELECTION: L = L::record(&[("revision", &REVISION)]);
static OMISSION: L = L::record(&[("selection", &SELECTION), ("outcome", &CAPTURE)]);
static SCOPE: L = L::record(&[
    ("capabilities", &CAPABILITIES),
    ("parent_proof", &FIXED),
    ("proof", &FIXED),
]);
static SOURCE_AUTHORIZATION: L = L::record(&[
    ("owner", &AUTHORITY),
    ("caller_admission_digest", &FIXED),
    ("caller_fence", &SCALARS),
    ("project", &VOLUME),
    ("generation", &RESOURCE),
    ("native_snapshot", &VALUE),
    ("policy_scope", &SCOPE),
]);
static SUMMARY_SELECTION: L = L::record(&[
    ("checkpoint", &FILE),
    ("limits", &SCALARS),
    ("history_limits", &SCALARS),
]);
static SUMMARY: L = L::record(&[
    ("selection", &SUMMARY_SELECTION),
    ("context", &FILE),
    ("payloads", &FILES),
    ("references", &FILES),
]);
static SHARED_GRANT: L = L::record(&[("volume", &VOLUME), ("operations", &OPERATIONS)]);
static OPERATIONS: L = L::list(Alloc::tree::<VolumeOperation, ()>(), &[&SCALARS]);
static REFERENCE_GRANT: L = L::record(&[("file", &FILE), ("attachment_manifest", &FILE)]);
static BOUNDARY: L = L::record(&[("provider", &PROVIDER), ("evidence", &BYTES)]);
const SEED_FIELDS: &[(&str, &L)] = &[
    ("parent", &AUTHORITY),
    ("authorized_source", &SOURCE_AUTHORIZATION),
    ("child", &AUTHORITY),
    ("attached_agents", &AGENTS),
    ("resources", &CAPTURED_RESOURCES),
    ("omissions", &OMISSIONS),
    ("child_private_volume", &VOLUME),
    ("child_private_generation", &RESOURCE),
    ("child_result_volume", &VOLUME),
    ("child_result_generation", &RESOURCE),
    ("inherited_context", &FILES),
    ("summary", &SUMMARY),
    ("shared_grants", &SHARED_GRANTS),
    ("reference_grants", &REFERENCE_GRANTS),
    ("history_witness", &FIXED),
    ("attachment_manifests", &FILES),
    ("boundary", &BOUNDARY),
];
static AGENTS: L = L::list(Alloc::vec::<AgentId>(), &[&SCALARS]);
static CAPTURED_RESOURCES: L = L::list(Alloc::vec::<CapturedResource>(), &[&CAPTURED]);
static OMISSIONS: L = L::list(Alloc::vec::<ForkOmission>(), &[&OMISSION]);
static SHARED_GRANTS: L = L::list(Alloc::vec::<SharedGrant>(), &[&SHARED_GRANT]);
static REFERENCE_GRANTS: L = L::list(Alloc::vec::<ReferenceGrant>(), &[&REFERENCE_GRANT]);
static SEED: L = L::record(SEED_FIELDS);
static BOXED_SEED: L = L::boxed(Alloc::boxed::<ForkSeed>(), SEED_FIELDS);

static ATTACHMENT: L = L::record(&[("file", &FILE)]);
static ATTACHMENTS: L = L::tagged(&[("items", &ATTACHMENT_ITEMS), ("manifest", &FILE)]);
static ATTACHMENT_ITEMS: L = L::list(Alloc::vec::<Attachment>(), &[&ATTACHMENT]);
static MESSAGE_EXTENSIONS: L = L::map(Alloc::tree::<String, FileRef>(), &[("*", &FILE)]);
const MESSAGE_FIELDS: &[(&str, &L)] = &[
    ("content", &FILE),
    ("attachments", &ATTACHMENTS),
    ("extensions", &MESSAGE_EXTENSIONS),
];
static MESSAGE: L = L::record(MESSAGE_FIELDS);
static BOXED_MESSAGE: L = L::boxed(Alloc::boxed::<ConversationMessage>(), MESSAGE_FIELDS);
static MESSAGES: L = L::list(Alloc::vec::<ConversationMessage>(), &[&MESSAGE]);
static PROOF: L = L::record(&[("provider", &PROVIDER), ("statement", &VALUE)]);
static BOXED_RECEIPT: L = L::boxed(
    Alloc::boxed::<ProjectMergeReceipt>(),
    &[
        ("child", &AUTHORITY),
        ("source_project", &VOLUME),
        ("source_generation", &RESOURCE),
        ("target_project", &VOLUME),
        ("expected_target_generation", &RESOURCE),
        ("result_generation", &RESOURCE),
        ("provider_operation_id", &BYTES),
        ("provider_proof", &PROOF),
        ("notice", &MESSAGE),
    ],
);

static CONTEXT_SELECTION: L = L::record(&[("message_ids", &UUIDS), ("checkpoint", &FILE)]);
static APPROVAL: L = L::record(&[("action_digest", &FIXED)]);
static TICKET: L = L::record(&[("request", &FILE), ("approval", &APPROVAL)]);
static OUTCOME: L = L::tagged(&[("answer", &BOXED_FILE)]);
static RESOLUTION: L = L::record(&[("outcome", &OUTCOME), ("detail", &FILE)]);

/// `previous` is a dependency list in a selection and an optional
/// configuration in a configuration event.
static PREVIOUS: L =
    L::record(EXTENSION_FIELDS).or_list(Alloc::vec::<ExtensionDependency>(), &[&SCALARS]);
/// Every `EventPayload` variant's fields.
static PAYLOAD: L = L::tagged(&[
    ("record", &EXTENSION),
    ("migration", &MIGRATION),
    ("previous", &PREVIOUS),
    ("selected", &DEPENDENCIES),
    ("configurations", &CONFIGURATIONS),
    ("request", &FILE),
    ("request_digest", &FIXED),
    ("result_schema", &FILE),
    ("result_schema_digest", &FIXED),
    ("observation", &ATTESTATION),
    ("seed", &BOXED_SEED),
    ("receipt", &BOXED_RECEIPT),
    ("message", &BOXED_MESSAGE),
    ("selection", &CONTEXT_SELECTION),
    ("ticket", &TICKET),
    ("resolution", &RESOLUTION),
]);
static RECORDED_SCOPE: L = L::record(&[("capabilities", &CAPABILITIES)]);
static EVENT: L = L::record(&[
    ("intent_digest", &FIXED),
    ("scope", &RECORDED_SCOPE),
    ("attestation", &FIXED),
    ("causal_parent", &EVENT_REFERENCE),
    ("payload", &PAYLOAD),
]);
static EVENTS: L = L::list(Alloc::vec::<Event>(), &[&EVENT]);

static EXTENSION_STATE: L = L::tuple(&[&EVENT_REFERENCE, &EXTENSION]);
static EXTENSION_STATES: L = L::map(
    Alloc::tree::<String, (EventReference, ExtensionRecord)>(),
    &[("*", &EXTENSION_STATE)],
);
static CONFIGURED: L = L::list(
    Alloc::entries::<ExtensionDependency, ExtensionConfiguration>(),
    &[&CONFIGURED_ENTRY],
);
static CONFIGURED_ENTRY: L = L::tuple(&[&SCALARS, &EXTENSION]);
static EFFECT: L = L::record(&[
    ("request", &FILE),
    ("request_digest", &FIXED),
    ("result_schema", &FILE),
    ("result_schema_digest", &FIXED),
    ("status", &EFFECT_STATUS),
    ("attempts", &ATTEMPTS),
]);
static ATTEMPTS: L = L::list(Alloc::vec::<EffectAttemptId>(), &[&SCALARS]);
static EFFECTS: L = L::map(Alloc::tree::<EffectId, EffectState>(), &[("*", &EFFECT)]);
static TERMINAL_EFFECTS: L = L::list(Alloc::vec::<EffectId>(), &[&SCALARS]);
static FORKS: L = L::list(Alloc::entries::<Authority, ForkSeed>(), &[&FORK]);
static FORK: L = L::tuple(&[&AUTHORITY, &SEED]);
static MERGES: L = L::list(Alloc::tree::<(String, Vec<u8>), ()>(), &[&MERGE]);
static MERGE: L = L::tuple(&[&SCALARS, &BYTES]);
static ARCHIVED_PREFIX: L = L::record(&[("history_digest", &FIXED), ("checkpoint", &FILE)]);
static CONVERSATION: L = L::record(&[
    ("messages", &MESSAGES),
    ("archived_prefix", &ARCHIVED_PREFIX),
]);
static INTERACTION: L = L::tuple(&[&TICKET, &RESOLUTION]);
static INTERACTIONS: L = L::map(
    Alloc::tree::<Uuid, (InteractionTicket, Option<InteractionResolution>)>(),
    &[("*", &INTERACTION)],
);
static BINDINGS: L = L::list(Alloc::vec::<(ExtensionDependency, [u8; 32])>(), &[&BINDING]);
static BINDING: L = L::tuple(&[&SCALARS, &FIXED]);
static PROJECTION: L = L::record(&[
    ("active_extensions", &DEPENDENCIES),
    ("extension_states", &EXTENSION_STATES),
    ("configured_extensions", &CONFIGURED),
    ("active_configurations", &CONFIGURATIONS),
    ("effects", &EFFECTS),
    ("indexed_terminal_effects", &TERMINAL_EFFECTS),
    ("forks", &FORKS),
    ("published_merges", &MERGES),
    ("conversation", &CONVERSATION),
    ("latest_context_checkpoint", &FILE),
    ("interactions", &INTERACTIONS),
    ("bindings", &BINDINGS),
]);
/// Layout of `Snapshot<ConversationStateWire>`, the type a checkpoint decodes.
pub(crate) static SNAPSHOT_LAYOUT: L = L::record(&[
    ("authority", &AUTHORITY),
    ("events", &EVENTS),
    ("projection", &PROJECTION),
    ("state_digest", &FIXED),
    ("attestation", &FIXED),
]);

#[cfg(test)]
#[path = "layout_tests.rs"]
mod tests;
