//! Volume-file audit of runtime decisions; compilation never produces events.
use super::{Action, Capability, Decision, Denial, Principal};
use serde::Serialize;
use std::sync::Arc;
#[cfg(all(feature = "fuzzing", not(acyclic_fs_loom)))]
pub mod fuzzing;
#[cfg(all(test, acyclic_fs_loom))]
mod model;
mod queue;
mod startup;
#[cfg(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32")))]
mod tests;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod writer;

/// Recording failed. No authorization outcome may be used after this error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("filesystem decision audit unavailable")]
pub struct AuditError;

/// A bounded deployment writer. Dropping its final owner drains admitted records.
pub(crate) struct AuditLog {
    id: uuid::Uuid,
    queue: Arc<queue::Queue<Pending>>,
}
impl Drop for AuditLog {
    fn drop(&mut self) {
        self.queue.close(false);
    }
}
impl AuditLog {
    /// Wait for decisions admitted before this call to become volume files.
    /// Explicit durability barrier, never called by an individual decision.
    pub(crate) fn flush(&self) -> Result<(), AuditError> {
        self.queue.flush()
    }
}

pub(crate) struct Binding {
    pub(crate) log: Arc<AuditLog>,
    pub(crate) identity: Arc<Identity>,
}
pub(crate) struct Identity {
    pub(crate) principal: Principal,
    pub(crate) capability_chain: Capability,
    pub(crate) credential_fingerprint: Option<[u8; 32]>,
    pub(crate) volume_id: crate::VolumeId,
    pub(crate) config: crate::VolumeConfig,
    pub(crate) rules: Arc<[String]>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) _custody: Option<crate::NativeAllocation>,
}
pub(crate) struct Path {
    parent: Option<Arc<Path>>,
    encoding: crate::kernel::NameEncoding,
    bytes: Vec<u8>,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<crate::NativeAllocation>,
}
impl Path {
    #[cfg(not(target_arch = "wasm32"))]
    fn fund(
        work: Option<&crate::NativeMaterializationWork>,
        length: usize,
    ) -> Result<Option<crate::NativeAllocation>, super::InvalidPolicy> {
        work.map(|work| {
            work.claim(
                (size_of::<Self>() + 2 * size_of::<usize>() + length) as u64,
                2,
                crate::WorkCounters {
                    bytes_copied: length as u64,
                    ..crate::WorkCounters::UNCHARGED
                },
            )
            .map_err(|_| super::InvalidPolicy("audit path storage was not admitted"))
        })
        .transpose()
    }
    pub(crate) fn unicode(
        parent: &Option<Arc<Self>>,
        encoding: crate::kernel::NameEncoding,
        text: &str,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Arc<Self>, super::InvalidPolicy> {
        let length = if encoding == crate::kernel::NameEncoding::WindowsUtf16Le {
            text.encode_utf16().count() * 2
        } else {
            text.len()
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = Self::fund(work, length)?;
        let mut bytes = Vec::with_capacity(length);
        if encoding == crate::kernel::NameEncoding::WindowsUtf16Le {
            bytes.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
        } else {
            bytes.extend_from_slice(text.as_bytes());
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &mut custody {
            custody
                .reconcile_capacity(
                    (size_of::<Self>() + 2 * size_of::<usize>() + bytes.capacity()) as u64,
                )
                .map_err(|_| super::InvalidPolicy("audit path storage exceeded admission"))?;
        }
        Ok(Arc::new(Self {
            parent: parent.clone(),
            encoding,
            bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _custody: custody,
        }))
    }
    pub(crate) fn native(
        parent: &Option<Arc<Self>>,
        name: &crate::kernel::LogicalName,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Arc<Self>, super::InvalidPolicy> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = Self::fund(work, name.as_bytes().len())?;
        let bytes = name.as_bytes().to_vec();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &mut custody {
            custody
                .reconcile_capacity(
                    (size_of::<Self>() + 2 * size_of::<usize>() + bytes.capacity()) as u64,
                )
                .map_err(|_| super::InvalidPolicy("audit path storage exceeded admission"))?;
        }
        Ok(Arc::new(Self {
            parent: parent.clone(),
            encoding: name.encoding(),
            bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _custody: custody,
        }))
    }
}

struct Pending {
    writer_id: uuid::Uuid,
    sequence: u64,
    binding: Arc<Identity>,
    path: Option<Arc<Path>>,
    all_paths: bool,
    action: Action,
    now: u64,
    version: u64,
    generation: u64,
    head: u64,
    policy_revision: Option<crate::GenerationId>,
    data_revision: Option<crate::GenerationId>,
    rule: Option<usize>,
    denial: Option<Denial>,
}
impl Binding {
    pub(crate) fn record(
        self: &Arc<Self>,
        view: &super::PathTrie,
        path: Option<&Arc<Path>>,
        action: Action,
        now: u64,
        decision: Decision<'_>,
        rule: Option<usize>,
        all_paths: bool,
    ) -> Result<(), AuditError> {
        self.log.queue.push(Pending {
            writer_id: self.log.id,
            sequence: 0,
            binding: Arc::clone(&self.identity),
            path: path.cloned(),
            all_paths,
            action,
            now,
            version: decision.policy_version,
            generation: view.generation,
            head: view.head_position(),
            policy_revision: view.policy_revision.map(|(generation, _)| generation),
            data_revision: view.data_generation(),
            rule,
            denial: decision.denial,
        })
    }
}
impl Serialize for Pending {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut record = serializer.serialize_struct("Decision", 13)?;
        record.serialize_field("writer_id", &self.writer_id)?;
        record.serialize_field("sequence", &self.sequence)?;
        record.serialize_field("schema_version", &1u32)?;
        record.serialize_field("principal", &self.binding.principal)?;
        record.serialize_field("action", &self.action)?;
        record.serialize_field("resource", &Resource(self))?;
        record.serialize_field("policy_version", &self.version)?;
        record.serialize_field("capability_chain", &self.binding.capability_chain)?;
        record.serialize_field(
            "credential_fingerprint",
            &self.binding.credential_fingerprint,
        )?;
        record.serialize_field("allowed", &self.denial.is_none())?;
        record.serialize_field("denial", &self.denial)?;
        record.serialize_field(
            "rule_id",
            &self.rule.and_then(|i| self.binding.rules.get(i)),
        )?;
        record.serialize_field("time", &self.now)?;
        record.end()
    }
}
struct Resource<'a>(&'a Pending);
impl Serialize for Resource<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut names = Vec::new();
        let mut path = self.0.path.as_deref();
        while let Some(node) = path {
            names.push((node.encoding.tag(), &node.bytes));
            path = node.parent.as_deref();
        }
        names.reverse();
        let mut resource = serializer.serialize_struct("Resource", 7)?;
        resource.serialize_field(
            "volume",
            &uuid::Uuid::from_bytes(self.0.binding.volume_id.into_bytes()),
        )?;
        resource.serialize_field("generation", &self.0.generation)?;
        resource.serialize_field("head", &self.0.head)?;
        resource.serialize_field("policy_revision", &self.0.policy_revision)?;
        resource.serialize_field("data_revision", &self.0.data_revision)?;
        resource.serialize_field("native_path", &names)?;
        resource.serialize_field("path_pattern", &self.0.all_paths.then_some("**"))?;
        resource.end()
    }
}

#[cfg(target_arch = "wasm32")]
impl<A, O> crate::Workspace<A, O> {
    /// Browser adapters require a server-side volume audit writer.
    pub fn audit_view(
        &self,
        _: crate::access::PathTrie,
        _: Principal,
        _: Capability,
        _: Option<[u8; 32]>,
    ) -> Result<crate::access::PathTrie, AuditError> {
        Err(AuditError)
    }
}
