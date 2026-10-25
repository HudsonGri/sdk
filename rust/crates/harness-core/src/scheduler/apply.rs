use super::*;
use std::mem::size_of;

impl Scheduler {
    /// Applies a borrowed committed event on the optional history ledger.
    pub fn apply(
        &mut self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.validate_event(event, work)?;
        if matches!(event, SchedulerEvent::Completed { .. })
            && self
                .operation(event.operation_id())
                .is_some_and(|state| state.phase == OperationPhase::Terminal)
        {
            return Ok(());
        }
        self.apply_event(event.clone_with_work(work)?, work)
    }
    #[inline(never)]
    fn apply_event(
        &mut self,
        event: SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        // Bytes observed for the copied event; the part the projection retains stays charged.
        let mut observed = if work.is_some() {
            event.logical_owned_bytes()?
        } else {
            0
        };
        let mut retained = 0;
        let mut child_slot = None;
        let mut cancellation = Vec::new();
        let prepared = (|| {
            retained = event.retained_clone_owned_bytes()?;
            if let SchedulerEvent::Declared { spec } = &event {
                let mut bytes = (size_of::<(OperationId, OperationState)>()
                    + size_of::<(OperationId, TreeEntry)>()) as u64;
                if spec.parent.is_some() {
                    bytes += size_of::<(String, OperationId)>() as u64;
                    if spec
                        .parent
                        .as_ref()
                        .is_some_and(|parent| !self.child_slots.contains_key(&parent.operation_id))
                    {
                        bytes += size_of::<(OperationId, BTreeMap<String, OperationId>)>() as u64;
                    }
                }
                if let Some(io) = work.as_deref_mut() {
                    io.admit_allocation(bytes)?;
                    io.retain_owned(bytes)?;
                    observed += bytes;
                }
                retained += bytes;
                if let Some(parent) = &spec.parent {
                    let slot = match work.as_deref_mut() {
                        Some(io) => io.clone_text(&parent.slot)?,
                        None => parent.slot.clone(),
                    };
                    if work.is_some() {
                        observed += slot.capacity() as u64;
                    }
                    retained += slot.capacity() as u64;
                    child_slot = Some(slot);
                }
            }
            if let SchedulerEvent::CancellationRequested {
                operation_id,
                recursive,
            } = &event
            {
                cancellation = self.cancellation_frontier(*operation_id, *recursive, work)?;
                if work.is_some() {
                    observed += (cancellation.capacity() * size_of::<OperationId>()) as u64;
                }
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            if let Some(io) = work.as_deref_mut() {
                let _ = io.release_owned(observed);
            }
            return Err(error);
        }
        let id = event.operation_id();
        let parent = match &event {
            SchedulerEvent::Declared { spec } => {
                spec.parent.as_ref().map(|parent| parent.operation_id)
            }
            _ => None,
        };
        let cancelled = matches!(event, SchedulerEvent::CancellationRequested { .. });
        let phase = if matches!(event, SchedulerEvent::Admitted { .. }) {
            OperationPhase::Admitted
        } else {
            OperationPhase::WaitingForCapacity
        };
        self.apply_projected_event(event, child_slot, &cancellation, phase, work)?;
        if !cancelled {
            for revised in Some(id).into_iter().chain(parent) {
                self.operations
                    .get_mut(&revised)
                    .ok_or_else(|| {
                        diagnostic(
                            work,
                            crate::wire::ErrorCode::Storage,
                            "validated operation is missing",
                        )
                    })?
                    .revision += 1;
            }
        }
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(observed.saturating_sub(retained))?;
        }
        Ok(())
    }
    fn cancel_validated(&mut self, id: OperationId) -> Result<()> {
        let state = self
            .operations
            .get_mut(&id)
            .ok_or_else(|| Error::Storage("validated operation is missing".into()))?;
        if state.phase == OperationPhase::Terminal {
            return Ok(());
        }
        let held = state.reservation.is_some();
        state.revision += 1;
        if !matches!(
            state.phase,
            OperationPhase::Running | OperationPhase::Reconciling
        ) && (state.phase == OperationPhase::Admitted || state.reservation.is_none())
        {
            state.phase = OperationPhase::Terminal;
            state.reservation = None;
            state.outcome = Some(Outcome::Cancelled);
            self.record_completion(id)?;
        } else {
            state.cancellation_requested = true;
        }
        self.update_reservation_count(id, held)
    }

    fn update_reservation_count(&mut self, id: OperationId, held: bool) -> Result<()> {
        let current = self
            .operation(id)
            .is_some_and(|state| state.reservation.is_some());
        if held == current {
            return Ok(());
        }
        let root = self
            .tree
            .get(&id)
            .ok_or_else(|| Error::Storage("operation tree entry is missing".into()))?
            .root;
        let entry = self
            .tree
            .get_mut(&root)
            .ok_or_else(|| Error::Storage("session tree root is missing".into()))?;
        entry.active = if current {
            entry.active.checked_add(1)
        } else {
            entry.active.checked_sub(1)
        }
        .ok_or_else(|| Error::Storage("session reservation accounting is inconsistent".into()))?;
        Ok(())
    }

    fn record_completion(&mut self, id: OperationId) -> Result<()> {
        if let Some(parent) = self
            .operation(id)
            .and_then(|state| state.spec.parent.as_ref())
            .map(|parent| parent.operation_id)
        {
            let last = self
                .tree
                .get(&parent)
                .ok_or_else(|| Error::Storage("completion parent is missing".into()))?
                .last_completed;
            if let Some(last) = last {
                self.tree
                    .get_mut(&last)
                    .ok_or_else(|| Error::Storage("completion predecessor is missing".into()))?
                    .next_completed = Some(id);
            }
            let entry = self
                .tree
                .get_mut(&parent)
                .ok_or_else(|| Error::Storage("completion parent is missing".into()))?;
            if entry.first_completed.is_none() {
                entry.first_completed = Some(id);
            }
            entry.last_completed = Some(id);
        }
        #[cfg(test)]
        self.completion_order.push(id);
        Ok(())
    }
    #[inline(never)]
    fn apply_projected_event(
        &mut self,
        event: SchedulerEvent,
        mut child_slot: Option<String>,
        cancellation: &[OperationId],
        phase: OperationPhase,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let id = event.operation_id();
        let held = self
            .operation(id)
            .is_some_and(|state| state.reservation.is_some());
        let cancellation_event = matches!(event, SchedulerEvent::CancellationRequested { .. });
        macro_rules! state {
            ($id:expr) => {
                self.operations.get_mut(&$id).ok_or_else(|| {
                    diagnostic(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "validated operation is missing",
                    )
                })?
            };
        }
        match event {
            SchedulerEvent::SessionConfigured { limits, .. } => {
                self.sessions.insert(id, limits);
            }
            SchedulerEvent::ModelDispatchClaimed { .. } => self.apply_model_claim(&event, work)?,

            SchedulerEvent::Declared { .. } => {
                self.apply_declaration(event, child_slot.take(), work)?;
            }

            SchedulerEvent::WaitingForCapacity { .. } => {
                state!(id).phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::Admitted { reservation, .. }
            | SchedulerEvent::PartiallyAdmitted { reservation, .. } => {
                let state = state!(id);
                state.reservation = Some(reservation);
                state.phase = phase;
            }
            SchedulerEvent::Rejected { reason, .. } => {
                let state = state!(id);
                state.reservation = None;
                state.phase = OperationPhase::Terminal;
                state.outcome = Some(Outcome::Failed { message: reason });
                self.record_completion(id)?;
            }
            SchedulerEvent::Started { .. } => state!(id).phase = OperationPhase::Running,
            SchedulerEvent::Checkpointed { checkpoint, .. } => {
                state!(id).checkpoint = Some(checkpoint);
            }
            SchedulerEvent::WaitingForChildren { .. } | SchedulerEvent::LeaseReleased { .. } => {
                let phase = if matches!(event, SchedulerEvent::WaitingForChildren { .. }) {
                    OperationPhase::WaitingForChildren
                } else {
                    OperationPhase::WaitingForCapacity
                };
                let state = state!(id);
                state.reservation = None;
                if state.cancellation_requested {
                    state.phase = OperationPhase::Terminal;
                    state.outcome = Some(Outcome::Cancelled);
                    self.record_completion(id)?;
                } else {
                    state.phase = phase;
                }
            }
            SchedulerEvent::WorkflowSuspended {
                fence,
                workflow_revision,
                waiting_command,
                ..
            } => {
                let state = state!(id);
                state.workflow = Some(WorkflowSuspension {
                    revision: workflow_revision,
                    waiting_command,
                    input: None,
                    released: Some(fence),
                });
                state.reservation = None;
                state.phase = OperationPhase::Suspended;
            }
            SchedulerEvent::WorkflowResumed { input, .. } => {
                Self::resume_workflow(state!(id), input, work)?;
            }

            SchedulerEvent::ReconciliationResumed { .. } => {
                let state = state!(id);
                state.phase = OperationPhase::Running;
                state.outcome = None;
            }
            SchedulerEvent::CancellationRequested { .. } => {
                for target in cancellation {
                    self.cancel_validated(*target)?;
                }
            }
            SchedulerEvent::Completed { outcome, .. } => {
                self.apply_completion(id, outcome, work)?;
            }

            SchedulerEvent::Orchestrated { .. } => self.apply_orchestration(event, work)?,
        }
        if !cancellation_event {
            self.update_reservation_count(id, held)?;
        }
        Ok(())
    }

    pub(super) fn apply_declaration(
        &mut self,
        event: SchedulerEvent,
        mut child_slot: Option<String>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let id = event.operation_id();
        let SchedulerEvent::Declared { spec } = event else {
            return Err(diagnostic(
                work,
                crate::wire::ErrorCode::Storage,
                "invalid scheduler mutation phase",
            ));
        };

        let spec = *spec;
        let (root, depth) = if let Some(parent) = &spec.parent {
            let (root, depth) = self.session_root(parent.operation_id, work)?;
            (
                root,
                depth
                    .checked_add(1)
                    .ok_or_else(|| diagnostic(work, Invalid, "task depth overflow"))?,
            )
        } else {
            (id, 0)
        };
        if let Some(parent) = &spec.parent {
            self.child_slots
                .entry(parent.operation_id)
                .or_default()
                .insert(
                    child_slot.take().ok_or_else(|| {
                        diagnostic(
                            work,
                            crate::wire::ErrorCode::Storage,
                            "prepared child slot is missing",
                        )
                    })?,
                    id,
                );
        }
        self.operations.insert(
            id,
            OperationState {
                spec,
                phase: OperationPhase::WaitingForDependencies,
                reservation: None,
                checkpoint: None,
                workflow: None,
                outcome: None,
                cancellation_requested: false,
                revision: 0,
            },
        );
        self.tree.insert(
            id,
            TreeEntry {
                root,
                depth,
                total: u64::from(root == id),
                active: 0,
                model_steps: 0,
                first_completed: None,
                last_completed: None,
                next_completed: None,
            },
        );
        if root != id {
            let entry = self
                .tree
                .get_mut(&root)
                .ok_or_else(|| diagnostic(work, Invalid, "session tree root is missing"))?;
            entry.total = entry
                .total
                .checked_add(1)
                .ok_or_else(|| diagnostic(work, Invalid, "task count overflow"))?;
        }
        Ok(())
    }

    fn apply_model_claim(
        &mut self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let id = event.operation_id();
        let SchedulerEvent::ModelDispatchClaimed {
            attempt_id,
            step,
            request_digest,
            ceiling,
            ..
        } = event
        else {
            return Err(diagnostic(
                work,
                crate::wire::ErrorCode::Storage,
                "invalid scheduler mutation phase",
            ));
        };

        let root = self.session_root(id, work)?.0;
        let claims = self
            .model_claims
            .entry(id)
            .or_insert_with(|| TaskModelClaims {
                ceiling: *ceiling,
                used: 0,
                attempts: BTreeMap::new(),
            });
        if claims
            .attempts
            .entry(*attempt_id)
            .or_default()
            .insert(*step, *request_digest)
            .is_none()
        {
            claims.used += 1;
            self.tree
                .get_mut(&root)
                .ok_or_else(|| diagnostic(work, Invalid, "session tree root is missing"))?
                .model_steps += 1;
        }
        Ok(())
    }

    fn apply_orchestration(
        &mut self,
        event: SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let id = event.operation_id();
        macro_rules! state {
            ($id:expr) => {
                self.operations.get_mut(&$id).ok_or_else(|| {
                    diagnostic(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "validated operation is missing",
                    )
                })?
            };
        }
        let SchedulerEvent::Orchestrated {
            outcome, cancel, ..
        } = event
        else {
            return Err(diagnostic(
                work,
                crate::wire::ErrorCode::Storage,
                "invalid scheduler mutation phase",
            ));
        };

        for child in cancel {
            self.cancel_validated(child)?;
        }
        let state = state!(id);
        state.phase = if matches!(outcome, Outcome::Indeterminate { .. }) {
            OperationPhase::Reconciling
        } else {
            OperationPhase::Terminal
        };
        state.outcome = Some(outcome);
        state.reservation = None;
        if state.phase == OperationPhase::Terminal {
            self.record_completion(id)?;
        }
        Ok(())
    }

    fn apply_completion(
        &mut self,
        id: OperationId,
        outcome: Outcome<FileRef>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let state = self.operations.get_mut(&id).ok_or_else(|| {
            diagnostic(
                work,
                crate::wire::ErrorCode::Storage,
                "validated operation is missing",
            )
        })?;
        state.phase = if matches!(outcome, Outcome::Indeterminate { .. }) {
            OperationPhase::Reconciling
        } else {
            state.reservation = None;
            OperationPhase::Terminal
        };
        state.outcome = Some(outcome);
        if state.phase == OperationPhase::Terminal {
            self.record_completion(id)?;
        }

        Ok(())
    }
    fn resume_workflow(
        state: &mut OperationState,
        input: FileRef,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        state
            .workflow
            .as_mut()
            .ok_or_else(|| {
                diagnostic(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "validated resume slot is missing",
                )
            })?
            .input = Some(input);
        state.phase = OperationPhase::WaitingForCapacity;

        Ok(())
    }
}
