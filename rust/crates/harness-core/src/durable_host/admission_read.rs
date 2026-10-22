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

    /// Reads, verifies and validates the immutable admission of `operation` once per
    /// host; later reads copy the validated record onto the caller's work. Every reader
    /// receives and pays for its own copy exactly as if it had decoded one.
    pub(super) async fn admission(
        &self,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        if let Some(record) = self.admissions.get(operation) {
            return copy_admission(&record, work);
        }
        let record = self.read_admission(operation, work).await?;
        self.admissions.insert(operation, &record);
        Ok(record)
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

/// Validated admissions, immutable once published, kept for the few tasks a host is
/// serving at once. The memo is bounded by entries and record size, so it is O(1) per
/// host however many tasks it has served.
#[derive(Default)]
pub(super) struct AdmissionMemo(
    std::sync::Mutex<std::collections::VecDeque<(OperationId, Arc<TaskAdmissionRecord>)>>,
);

impl AdmissionMemo {
    const ENTRIES: usize = 16;
    const RECORD_BYTES: u64 = 16 * 1024;
    fn entries(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        std::collections::VecDeque<(OperationId, Arc<TaskAdmissionRecord>)>,
    > {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn get(&self, operation: OperationId) -> Option<Arc<TaskAdmissionRecord>> {
        self.entries()
            .iter()
            .find(|(cached, _)| *cached == operation)
            .map(|(_, record)| Arc::clone(record))
    }
    fn insert(&self, operation: OperationId, record: &TaskAdmissionRecord) {
        if !record
            .logical_owned_bytes()
            .is_ok_and(|bytes| bytes <= Self::RECORD_BYTES)
        {
            return;
        }
        let mut entries = self.entries();
        if entries.iter().any(|(cached, _)| *cached == operation) {
            return;
        }
        if entries.len() == Self::ENTRIES {
            entries.pop_front();
        }
        entries.push_back((operation, Arc::new(record.clone())));
    }
}
