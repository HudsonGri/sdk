use super::*;

#[cfg(acyclic_harness_loom)]
loom::thread_local! {
    static LOSE_NATIVE_CUSTODY: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

#[cfg(all(test, acyclic_harness_loom))]
#[path = "summary_models.rs"]
mod models;

#[derive(Default)]
pub(super) struct ExecutionSummary {
    pub(super) operation: Option<OperationId>,
    pub(super) tail: u64,
    pub(super) pending: bool,
    pub(super) retries: BTreeMap<[u8; 32], (u64, [u8; 32])>,
    pub(super) contexts: BTreeMap<u32, bool>,
    pub(super) models: BTreeMap<(u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>>,
    pub(super) tools: BTreeMap<(u32, [u8; 32]), bool>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) native: Option<acyclic_fs::NativeAllocation>,
}

impl ExecutionSummary {
    pub(super) fn require_next(
        &self,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<Option<[u8; 32]>> {
        let hashed = match event {
            ExecutionEvent::ToolStarted { call_id, .. } if self.tail > 0 && !call_id.is_empty() => {
                call_id.len() as u64
            }
            ExecutionEvent::ToolCompleted { call_id, .. }
            | ExecutionEvent::ToolFailed { call_id, .. } => call_id.len() as u64,
            _ => 0,
        };
        let work = acyclic_fs::WorkCounters {
            bytes_hashed: hashed,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(work)?;
        }
        let mut tool_key = None;
        let valid = match event {
            ExecutionEvent::Started { request_digest } => {
                self.tail == 0 && *request_digest != [0; 32]
            }
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.tail > 0
                    && !self.contexts.contains_key(step)
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.get(step) == Some(&false)
                    && matches!(
                        self.models
                            .get(&(*step, crate::executor::ModelPurpose::Summary)),
                        Some(None)
                    )
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => {
                self.tail > 0
                    && *request_digest != [0; 32]
                    && !self.models.contains_key(&(*step, *purpose))
            }
            ExecutionEvent::Model { step, purpose, .. } => self
                .models
                .get(&(*step, *purpose))
                .is_some_and(Option::is_some),
            ExecutionEvent::ToolStarted { step, call_id, .. } => {
                self.tail > 0 && !call_id.is_empty() && {
                    let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                    tool_key = Some(key);
                    !self.tools.contains_key(&(*step, key))
                }
            }
            ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => {
                let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                tool_key = Some(key);
                self.tools.get(&(*step, key)) == Some(&false)
            }
        };
        if let Some(io) = io {
            io.charge(work)?;
        }
        if valid {
            Ok(tool_key)
        } else {
            Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "execution observation lacks its dispatch or reuses a start",
            ))
        }
    }

    pub(super) fn validate_next(
        &self,
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        self.require_next(event, io)?;
        if let ExecutionEvent::Model { step, purpose, .. } = event {
            self.models
                .get(&(*step, *purpose))
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "model dispatch is already settled",
                    )
                })?
                .validate_next(
                    model.ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "model observation is missing",
                        )
                    })?,
                    limits,
                    io,
                )?;
        }
        Ok(())
    }

    pub(super) fn accept(
        &mut self,
        record: &ExecutionRecord,
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let recheck = io
            .and_then(PreparationIo::original_native_work)
            .map(|work| PreparationIo::from_native_work(&work.recheck_admitted()))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let digest_io = recheck.as_ref().or(io);
        #[cfg(target_arch = "wasm32")]
        let digest_io = io;
        let digest = crate::executor::with_history_io(digest_io, |work| {
            crate::executor::json::canonical_json_digest(&record.event, limits.file_bytes, work)
        })?;
        self.accept_fields(
            (record.sequence, &record.idempotency_key, &record.event),
            digest,
            model,
            limits,
            io,
        )
    }

    pub(super) fn accept_fields(
        &mut self,
        record: (u64, &str, &ExecutionEvent),
        digest: [u8; 32],
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = io
            .filter(|io| io.original_native_work().is_some())
            .map(|io| io.owned_bytes().map(|before| (io, before)))
            .transpose()?;
        let result = self.accept_inner(record, digest, model, limits, io);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((io, before)) = native.filter(|_| {
            #[cfg(acyclic_harness_loom)]
            {
                !LOSE_NATIVE_CUSTODY.with(std::cell::Cell::get)
            }
            #[cfg(not(acyclic_harness_loom))]
            {
                true
            }
        }) {
            let mut incoming = match io
                .owned_since(before)
                .and_then(|bytes| io.take_native_custody(bytes))
            {
                Ok(incoming) => incoming,
                Err(error) => {
                    *self = Self::default();
                    return Err(error);
                }
            };
            if let Some(custody) = &mut self.native {
                if let Err(cause) = custody.absorb(&mut incoming) {
                    *self = Self::default();
                    return Err(Error::NativeWork {
                        cause,
                        custody: Some(incoming),
                    });
                }
            } else {
                self.native = Some(incoming);
            }
        }
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn accept_inner(
        &mut self,
        (sequence, idempotency_key, event): (u64, &str, &ExecutionEvent),
        digest: [u8; 32],
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let key = crate::model::hash_bytes(idempotency_key.as_bytes(), io)?;
        if self.tail.checked_add(1) != Some(sequence)
            || idempotency_key.is_empty()
            || self.retries.contains_key(&key)
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "execution journal sequence or retry identity is invalid",
            ));
        }
        let tool_key = self.require_next(event, io)?;
        if let Some(io) = io {
            let extra = match event {
                ExecutionEvent::ContextPrepared { .. } => std::mem::size_of::<(u32, bool)>(),
                ExecutionEvent::ModelStarted { .. } => std::mem::size_of::<(
                    (u32, crate::executor::ModelPurpose),
                    Option<ModelEventAdmission>,
                )>(),
                ExecutionEvent::ToolStarted { .. } => {
                    std::mem::size_of::<((u32, [u8; 32]), bool)>()
                }
                _ => 0,
            };
            io.retain_owned((std::mem::size_of::<([u8; 32], (u64, [u8; 32]))>() + extra) as u64)?;
        }
        match event {
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.contexts.insert(*step, false);
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.insert(*step, true);
            }
            ExecutionEvent::ModelStarted { step, purpose, .. } => {
                self.models
                    .insert((*step, *purpose), Some(ModelEventAdmission::default()));
            }
            ExecutionEvent::Model { step, purpose, .. } => {
                let state = self.models.get_mut(&(*step, *purpose)).ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model dispatch is missing",
                    )
                })?;
                let admission = state.as_mut().ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "model dispatch is already settled",
                    )
                })?;
                admission.observe(
                    model.ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "model observation is missing",
                        )
                    })?,
                    limits,
                    io,
                )?;
                if admission.completed {
                    *state = None;
                }
            }
            ExecutionEvent::ToolStarted { step, .. }
            | ExecutionEvent::ToolCompleted { step, .. }
            | ExecutionEvent::ToolFailed { step, .. } => {
                self.tools.insert(
                    (
                        *step,
                        tool_key.ok_or_else(|| {
                            canonical_boundary(
                                io,
                                crate::wire::ErrorCode::Storage,
                                "tool observation has no authenticated identity",
                            )
                        })?,
                    ),
                    !matches!(event, ExecutionEvent::ToolStarted { .. }),
                );
            }
            ExecutionEvent::Started { .. } => {}
        }
        self.retries.insert(key, (sequence, digest));
        self.tail = sequence;
        Ok(())
    }

    pub(super) fn quiescent(&self, io: Option<&PreparationIo>) -> Result<bool> {
        for model in self.models.values() {
            crate::model::charge_items(io, 1)?;
            if model.is_some() {
                return Ok(false);
            }
        }
        for settled in self.tools.values() {
            crate::model::charge_items(io, 1)?;
            if !settled {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub(super) const OBSERVATION_VERSION: u32 = 3;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Observation {
    pub(super) version: u32,
    pub(super) operation_id: OperationId,
    pub(super) turn_operation: OperationId,
    pub(super) retry_digest: String,
    pub(super) event: ExecutionEvent,
}

impl PreparationRecord for Observation {
    fn diagnostic_label_bytes() -> u64 {
        const PRIVATE_LABEL_BYTES: u64 = {
            let labels = [
                "Observation",
                "version",
                "operation_id",
                "turn_operation",
                "retry_digest",
                "event",
                "ExecutionEvent",
                "kind",
                "started",
                "context_prepared",
                "context_compacted",
                "model_started",
                "model",
                "tool_started",
                "tool_completed",
                "tool_failed",
                "request_digest",
                "step",
                "projection",
                "accounting",
                "compaction",
                "purpose",
                "request",
                "call_id",
                "invocation",
                "result",
                "reason",
                "ModelPurpose",
                "response",
                "summary",
                "ToolFailureKind",
                "executor_rejected",
                "invalid_output",
                "projection_rejected",
                "publication_rejected",
            ];
            let mut bytes = 0;
            let mut remaining = labels.as_slice();
            while let Some((label, rest)) = remaining.split_first() {
                bytes += label.len() as u64 + 4;
                remaining = rest;
            }
            bytes
        };
        crate::executor::json::preparation_serde_diagnostic_label_bytes() + PRIVATE_LABEL_BYTES
    }
}
