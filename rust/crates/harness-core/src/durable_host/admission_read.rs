//! One canonical admission reader on the supplied IO context.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn admission_run_limits(
        &self,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::runtime::TaskRunLimits> {
        let admission = self.admission(operation, work).await?;
        let retained =
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + admission.logical_owned_bytes()?;
        let limits = admission.run_limits;
        drop(admission);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(retained)?;
        }
        Ok(limits)
    }

    /// The immutable admission of `operation`. While a live owner holds the validated
    /// record, a read copies it onto the caller's work instead of re-reading,
    /// re-verifying and re-decoding it; otherwise the record is read in full. Every
    /// reader receives and pays for its own copy exactly as if it had decoded one.
    pub(super) async fn admission(
        &self,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        if let Some(record) = self.admissions.get(operation) {
            return copy_admission(&record, work);
        }
        self.read_admission(operation, work).await
    }

    pub(super) async fn read_admission(
        &self,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut recheck = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .map(|work| {
                crate::executor::PreparationIo::from_native_work(&work.recheck_admitted())
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut read_work = match recheck.as_mut() {
            Some(io) => Some(io as &mut dyn crate::store::HistoryIo),
            None => work
                .as_mut()
                .map(|io| &mut **io as &mut dyn crate::store::HistoryIo),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut read_work = work
            .as_mut()
            .map(|io| &mut **io as &mut dyn crate::store::HistoryIo);
        let result = async {
            let work = &mut read_work;
            let io = operation_io(work)?;
            let mut quota = crate::wire_codec::CodecQuota::new(
                work,
                std::mem::size_of::<TaskAdmissionRecord>() as u64,
            )?;
            let spec = self
                .retained_declaration(operation, quota.work())
                .await?
                .ok_or_else(|| Error::NotFound("task admission".into()))?;
            quota.adopt(spec.logical_owned_bytes()?)?;
            let value = self.read_json(&spec.state, io.as_ref()).await?;
            let owned = crate::executor::json::json_value_logical_owned_bytes(&value)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let (Some(io), Some(observer)) = (&io, quota.work().as_deref_mut()) {
                observer.absorb_native_custody(io.take_native_custody(owned)?)?;
            }
            quota.adopt(owned)?;
            let (admission, held) = TaskAdmissionRecord::from_canonical_value(value, quota.work())?;
            quota.adopt(held)?;
            if admission.operation_id != operation
                || admission.task.name != spec.entrypoint.name
                || admission.task.version != spec.entrypoint.version
                || admission.task.digest != spec.entrypoint.digest
                || admission.output_schema != spec.entrypoint.result_schema
                || admission
                    .parent
                    .map(|task| OperationId::from_bytes(task.into_bytes()))
                    != spec.parent.as_ref().map(|parent| parent.operation_id)
                || spec
                    .parent
                    .as_ref()
                    .is_some_and(|parent| parent.slot != operation.to_string())
                || admission.machine.name != admission.task.name
                || admission.machine.version != admission.task.version
                || admission.policy != self.policy.as_ref().map(|policy| policy.identity())
                || admission.extensions.as_ref() != self.root_scope.extensions()
            {
                return Err(Error::Conflict(
                    "retained task admission disagrees with its declaration".into(),
                ));
            }
            if let Some(limits) = self.session_limits {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh(quota.work()).await?;
                if coordinator
                    .scheduler()
                    .session_limits(operation, quota.work())?
                    != limits
                {
                    return Err(Error::Conflict(
                        "retained session ceilings differ from host".into(),
                    ));
                }
            }
            if let Some(extensions) = &admission.extensions {
                extensions.validate(quota.work())?;
            }
            self.tasks.validate_durable_admission(
                &admission.task,
                &admission.machine,
                Some(&admission.input_schema),
                &admission.output_schema,
                // The record already validated input; the registry checks exact schema identity.
                None,
                quota.work(),
            )?;
            if self
                .machines
                .resolve(&admission.machine, quota.work())?
                .is_none()
            {
                return Err(Error::Unsupported(
                    "pinned task machine is unavailable".into(),
                ));
            }
            if admission.resource_proofs.is_empty() {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(native) = io.as_ref().and_then(|io| io.original_native_work()) {
                    self.root_scope.validate_narrowing_with_native_work(
                        &admission.grants,
                        &admission.limits,
                        native,
                    )?;
                } else {
                    self.root_scope
                        .narrow(admission.grants.clone(), admission.limits)?;
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                self.root_scope
                    .narrow(admission.grants.clone(), admission.limits)?;
                self.root_scope
                    .validate_run_limit_attenuation(&admission.run_limits, quota.work())?;
            }
            let retained = std::mem::size_of::<TaskAdmissionRecord>() as u64
                + admission.logical_owned_bytes()?;
            drop(spec);
            quota.replace(retained)?;
            quota.transfer();
            Ok(admission)
        }
        .await;
        drop(read_work);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if result.is_ok()
            && let Some(recheck) = recheck
        {
            work.as_deref_mut()
                .ok_or_else(|| Error::Invalid("admission observer is missing".into()))?
                .absorb_native_custody(recheck.into_custody()?)?;
        }
        result
    }
}

/// A copy of a validated admission, held exactly as a freshly decoded one would be.
fn copy_admission(
    record: &TaskAdmissionRecord,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<TaskAdmissionRecord> {
    let mut quota = crate::wire_codec::CodecQuota::new(
        work,
        std::mem::size_of::<TaskAdmissionRecord>() as u64,
    )?;
    let copied = crate::core::copy(record, &mut quota, |record, _| record.logical_owned_bytes())?;
    quota.replace(
        std::mem::size_of::<TaskAdmissionRecord>() as u64 + copied.logical_owned_bytes()?,
    )?;
    quota.transfer();
    Ok(copied)
}

/// The validated admissions that live journal owners hold, shared rather than copied.
/// The memo holds only weak handles: every record stays charged to the owner that
/// decoded it and is freed with that owner, so the memo retains no record of its own.
/// A handle whose owner is gone is dropped at the next access.
#[derive(Default)]
pub(super) struct AdmissionMemo(
    std::sync::Mutex<
        std::collections::VecDeque<(OperationId, std::sync::Weak<TaskAdmissionRecord>)>,
    >,
);

impl AdmissionMemo {
    const ENTRIES: usize = 16;
    fn live(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        std::collections::VecDeque<(OperationId, std::sync::Weak<TaskAdmissionRecord>)>,
    > {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|(_, record)| record.strong_count() != 0);
        entries
    }
    fn get(&self, operation: OperationId) -> Option<Arc<TaskAdmissionRecord>> {
        self.live()
            .iter()
            .find(|(held, _)| *held == operation)
            .and_then(|(_, record)| record.upgrade())
    }
    /// Shares a live owner's validated admission with later readers on this host.
    pub(super) fn remember(&self, record: &Arc<TaskAdmissionRecord>) {
        let mut entries = self.live();
        if entries.iter().any(|(held, _)| *held == record.operation_id) {
            return;
        }
        if entries.len() == Self::ENTRIES {
            entries.pop_front();
        }
        entries.push_back((record.operation_id, Arc::downgrade(record)));
    }
}
