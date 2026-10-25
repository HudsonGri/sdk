//! A transactional adapter to the sole workspace graph and canonical codec.
use super::*;
use crate::kernel::{KernelAccounting, Owned};
use crate::lineage::{WorkspaceLineageError, WorkspaceLineageRecord, WorkspaceLineageStore};
use crate::facade::FsWork as _;

type Failure = WorkspaceLineageError<AuthorityStoreError>;

fn key(workspace: crate::WorkspaceId) -> [u8; 50] {
    let mut key = [0; 50];
    key[..18].copy_from_slice(b"workspace-lineage/");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (pair, byte) in key[18..].chunks_exact_mut(2).zip(workspace.into_bytes()) {
        pair[0] = HEX[usize::from(byte >> 4)];
        pair[1] = HEX[usize::from(byte & 15)];
    }
    key
}

impl IndexedDbAuthorityStore {
    async fn lineage_record(
        &self,
        tx: &Transaction<'_>,
        workspace: crate::WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<WorkspaceLineageRecord>>, Failure> {
        let key = key(workspace);
        let key = std::str::from_utf8(&key).map_err(|_| Failure::IncompatibleState)?;
        let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        let request = tx.object_store(OPERATION_WINDOWS).map_err(backend).map_err(Failure::Store)?
            .get::<JsValue, _, _>(key).primitive().map_err(backend).map_err(Failure::Store)?;
        let value = await_cancellable(request, accounting.cancellation()).await;
        accounting.record_completed(delta)?;
        let Some(value) = value.map_err(Failure::Store)? else { return Ok(None); };
        let array = value.dyn_into::<Uint8Array>().map_err(|_| Failure::IncompatibleState)?;
        let length = u64::from(array.length());
        if length > self.maximum_payload_bytes { return Err(Failure::IncompatibleState); }
        let mut allocation = accounting.claim_owned(length, u64::from(length != 0))?;
        let bytes = accounting.perform(WorkCounters { bytes_copied: length, ..WorkCounters::UNCHARGED }, || array.to_vec());
        let bytes = match bytes {
            Ok(bytes) => Owned::new(bytes, allocation, length),
            Err(error) => { accounting.release_owned(&mut allocation, length)?; return Err(error.into()); }
        };
        let decoded = crate::lineage::codec::decode(&bytes.value, self.maximum_payload_bytes, accounting);
        bytes.retire(accounting)?;
        let decoded = decoded?;
        if decoded.value.workspace_id != workspace {
            decoded.retire(accounting)?;
            return Err(Failure::IncompatibleState);
        }
        Ok(Some(decoded))
    }
}

impl WorkspaceLineageStore for IndexedDbAuthorityStore {
    type Error = AuthorityStoreError;

    async fn load(&self, workspace: crate::WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        self.load_accounted(workspace, &mut accounting).await
            .map(|record| record.map(|owned| owned.value)).map_err(backend)
    }

    async fn compare_and_swap(&self, workspace: crate::WorkspaceId, revision: u64, record: WorkspaceLineageRecord) -> Result<bool, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        self.compare_accounted(workspace, revision, &record, &mut accounting).await.map_err(backend)
    }

    async fn load_accounted(&self, workspace: crate::WorkspaceId, accounting: &mut KernelAccounting<'_>) -> Result<Option<Owned<WorkspaceLineageRecord>>, Failure> {
        accounting.check()?;
        let tx = transaction(&self.database, &[OPERATION_WINDOWS], TransactionMode::Readonly)
            .map_err(backend).map_err(Failure::Store)?;
        self.lineage_record(&tx, workspace, accounting).await
    }

    async fn compare_accounted(&self, workspace: crate::WorkspaceId, revision: u64, record: &WorkspaceLineageRecord, accounting: &mut KernelAccounting<'_>) -> Result<bool, Failure> {
        if record.workspace_id != workspace || revision.checked_add(1) != Some(record.revision) {
            return Err(Failure::IncompatibleState);
        }
        let bytes = crate::lineage::codec::encode(record, accounting)?;
        let result = async {
            if bytes.value.len() as u64 > self.maximum_payload_bytes { return Err(Failure::IncompatibleState); }
            let tx = transaction(&self.database, &[OPERATION_WINDOWS], TransactionMode::Readwrite)
                .map_err(backend).map_err(Failure::Store)?;
            let current = self.lineage_record(&tx, workspace, accounting).await?;
            let actual = current.as_ref().map_or(0, |owned| owned.value.revision);
            if let Some(current) = current { current.retire(accounting)?; }
            if actual != revision { return Ok(false); }
            let key = key(workspace);
            let key = std::str::from_utf8(&key).map_err(|_| Failure::IncompatibleState)?;
            let delta = WorkCounters { backend_write_operations: 1, bytes_copied: bytes.value.len() as u64,
                ..WorkCounters::UNCHARGED };
            // IndexedDB may commit after the last request. Admit the complete
            // write, including durability, before issuing that request.
            accounting.admit(WorkCounters { durability_operations: 1, ..delta })?;
            let request = tx.object_store(OPERATION_WINDOWS).map_err(backend).map_err(Failure::Store)?
                .put(JsValue::from(Uint8Array::from(bytes.value.as_slice()))).with_key(key)
                .primitive().map_err(backend).map_err(Failure::Store)?;
            let written = await_cancellable(request, accounting.cancellation()).await;
            accounting.record_completed(delta)?;
            written.map_err(Failure::Store)?;
            let delta = WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED };
            let committed = tx.commit().await;
            accounting.record_completed(delta)?;
            committed.map_err(backend).map_err(Failure::Store)?;
            Ok(true)
        }.await;
        bytes.retire(accounting)?;
        result
    }
}
