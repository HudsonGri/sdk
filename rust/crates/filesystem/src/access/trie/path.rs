use super::*;
use crate::access::audit;
use std::sync::Arc;

/// A path bound to its immutable compiled view. Its private state cannot be
/// forged or transferred to a different policy, principal or capability.
#[derive(Clone)]
pub struct PreparedPath<'a> {
    pub(super) trie: &'a PathTrie,
    pub(super) state: usize,
    pub(super) bytes: usize,
    pub(super) depth: usize,
    pub(super) capture: bool,
    pub(super) audit_path: Option<Arc<audit::Path>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) work: Option<&'a crate::NativeMaterializationWork>,
}

/// Immutable permission state retained by an open native handle.
#[derive(Clone)]
pub struct PinnedPath {
    pub(super) view: std::sync::Arc<PathTrie>,
    pub(super) audit_path: Option<Arc<audit::Path>>,
    pub(super) state: usize,
}

impl PinnedPath {
    pub(crate) fn volume(&self) -> &str {
        self.view.volume()
    }

    pub(crate) fn policy_view(&self) -> &PathTrie {
        &self.view
    }

    /// Check the pinned policy, including its original expiry, without allocation.
    #[must_use]
    pub fn evaluate(&self, action: Action, now: u64) -> Decision<'_> {
        self.view.decision(self.state, action, now)
    }

    /// Authorize and enqueue exactly one audit record before returning the outcome.
    pub fn decide(&self, action: Action, now: u64) -> Result<Decision<'_>, audit::AuditError> {
        self.view
            .authorize_state(self.state, &self.audit_path, action, now)
    }

    pub(in crate::access) fn action_mask(&self, now: u64) -> u16 {
        self.view
            .state_at(self.state, now)
            .map_or(0, |state| state.actions)
    }
}

impl<'a> PreparedPath<'a> {
    /// Prepare a child during namespace lookup. Only this new name is scanned;
    /// previously prepared ancestors are never scanned again.
    pub fn child(&self, name: &str) -> Result<Self, InvalidPolicy> {
        if name.is_empty() || name.contains('/') {
            return Err(InvalidPolicy("expected one path component"));
        }
        super::super::names::validate(name, self.trie.config)?;
        let mut child = self.advance(
            super::super::names::encoded_len(name, self.trie.config),
            |sink| {
                super::super::names::write_key(name, self.trie.config, sink)
                    .map_err(|_| InvalidPolicy("invalid compiled path"))
            },
        )?;
        if self.capture && self.trie.audit.is_some() {
            let encoding = super::super::names::encoding(self.trie.config);
            child.audit_path = Some(audit::Path::unicode(
                &self.audit_path,
                encoding,
                name,
                #[cfg(not(target_arch = "wasm32"))]
                self.work,
            )?);
        }
        Ok(child)
    }

    fn advance(
        &self,
        length: usize,
        emit: impl FnOnce(&mut PathWriter<'_>) -> Result<(), InvalidPolicy>,
    ) -> Result<Self, InvalidPolicy> {
        let bytes = length
            .checked_add(1)
            .and_then(|length| self.bytes.checked_add(length))
            .ok_or(InvalidPolicy("path length overflow"))?;
        let depth = self.depth + 1;
        if bytes > self.trie.config.limits.maximum_path_bytes as usize
            || depth > self.trie.config.limits.maximum_path_depth as usize
        {
            return Err(InvalidPolicy("path exceeds volume limits"));
        }
        let mut state = self.state;
        if self.depth != 0 {
            state = self.trie.step(state, '/')?;
        }
        let mut sink = PathWriter {
            trie: self.trie,
            state,
        };
        emit(&mut sink)?;
        Ok(Self {
            state: sink.state,
            bytes,
            depth,
            ..self.clone()
        })
    }

    /// Prepare an authenticated native name without losing its encoding identity.
    /// Unsupported profile/encoding pairs and non-Unicode names fail closed.
    pub fn child_name(&self, name: &crate::kernel::LogicalName) -> Result<Self, InvalidPolicy> {
        use crate::kernel::NameEncoding;
        if name.encoding() != super::super::names::encoding(self.trie.config)
            || name.as_bytes().len() > self.trie.config.limits.maximum_component_bytes as usize
        {
            return Err(InvalidPolicy(
                "name encoding does not match the access view profile",
            ));
        }
        match name.encoding() {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                let text = std::str::from_utf8(name.as_bytes())
                    .map_err(|_| InvalidPolicy("non-Unicode access path is unsupported"))?;
                self.child(text)
            }
            NameEncoding::WindowsUtf16Le => {
                let units = crate::kernel::types::utf16le_units(name.as_bytes())
                    .ok_or(InvalidPolicy("invalid UTF-16 access name"))?;
                let mut child = self.advance(name.as_bytes().len(), |sink| {
                    for ch in char::decode_utf16(units) {
                        let ch = ch
                            .map_err(|_| InvalidPolicy("non-Unicode access path is unsupported"))?;
                        sink.state = sink.trie.step(sink.state, ch)?;
                    }
                    Ok(())
                })?;
                if self.capture && self.trie.audit.is_some() {
                    child.audit_path = Some(audit::Path::native(
                        &self.audit_path,
                        name,
                        #[cfg(not(target_arch = "wasm32"))]
                        self.work,
                    )?);
                }
                Ok(child)
            }
        }
    }

    /// Constant-time, allocation-free decision, including the pinned deadline.
    #[must_use]
    pub fn evaluate(&self, action: Action, now: u64) -> Decision<'a> {
        self.trie.decision(self.state, action, now)
    }

    /// Authorize and enqueue exactly one audit record before returning the outcome.
    pub fn decide(&self, action: Action, now: u64) -> Result<Decision<'a>, audit::AuditError> {
        self.trie
            .authorize_state(self.state, &self.audit_path, action, now)
    }

    pub(in crate::access) fn action_mask(&self, now: u64) -> u16 {
        self.trie
            .state_at(self.state, now)
            .map_or(0, |state| state.actions)
    }
}

impl PathTrie {
    pub(in crate::access) fn prepare_for_preview(
        &self,
        path: &str,
    ) -> Result<PreparedPath<'_>, InvalidPolicy> {
        let mut prepared = self.prepare("")?;
        prepared.capture = false;
        if !path.is_empty() {
            for name in path.split('/') {
                prepared = prepared.child(name)?;
            }
        }
        Ok(prepared)
    }

    /// Prepare exact encoded names instead of a potentially lossy path projection.
    pub fn prepare_namespace(
        &self,
        path: &crate::kernel::NamespacePath,
    ) -> Result<PreparedPath<'_>, InvalidPolicy> {
        let mut prepared = self.prepare("")?;
        for name in path.components() {
            prepared = prepared.child_name(name)?;
        }
        Ok(prepared)
    }

    /// Prepare exact names with audit-path storage charged to the original request.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_namespace_with_work<'a>(
        &'a self,
        path: &crate::kernel::NamespacePath,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<PreparedPath<'a>, InvalidPolicy> {
        let mut prepared = self.prepare("")?;
        prepared.work = Some(work);
        for name in path.components() {
            prepared = prepared.child_name(name)?;
        }
        Ok(prepared)
    }

    /// Prepare a textual path while retaining audit capture on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_with_work<'a>(
        &'a self,
        path: &str,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<PreparedPath<'a>, InvalidPolicy> {
        let mut prepared = self.prepare("")?;
        prepared.work = Some(work);
        if !path.is_empty() {
            for name in path.split('/') {
                prepared = prepared.child(name)?;
            }
        }
        Ok(prepared)
    }

    /// Runtime decision with audit-path capture funded by the original request.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn decide_with_work(
        &self,
        action: Action,
        path: &str,
        now: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Decision<'_>, audit::AuditError> {
        let path = self
            .prepare_with_work(path, work)
            .map_err(|_| audit::AuditError)?;
        self.authorize_state(path.state, &path.audit_path, action, now)
    }

    /// Pin a native handle and retain its funded audit path through persistence.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn pin_with_work(
        self: &std::sync::Arc<Self>,
        path: &crate::kernel::NamespacePath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<PinnedPath, InvalidPolicy> {
        let prepared = self.prepare_namespace_with_work(path, work)?;
        Ok(PinnedPath {
            state: prepared.state,
            view: self.clone(),
            audit_path: prepared.audit_path,
        })
    }

    /// Pin the current compiled state for an opened file's lifetime.
    pub fn pin(
        self: &std::sync::Arc<Self>,
        path: &crate::kernel::NamespacePath,
    ) -> Result<PinnedPath, InvalidPolicy> {
        let prepared = self.prepare_namespace(path)?;
        Ok(PinnedPath {
            state: prepared.state,
            view: std::sync::Arc::clone(self),
            audit_path: prepared.audit_path,
        })
    }
}
