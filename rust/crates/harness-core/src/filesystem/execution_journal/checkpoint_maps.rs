//! Lazy summary entries and incremental immutable-page publication.
use super::*;
use acyclic_stream::OwnedBytes;
use persistent_index::{NodeRef, Reader, Writer};
use summary_map::{SummaryKey, SummaryMap};

mod model_cell;

trait Entry: serde::de::DeserializeOwned + Serialize {
    fn heap_bytes(&self) -> Result<u64> {
        Ok(0)
    }

    fn encode(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        match io {
            Some(io) => io.encode(&Cell { value: self }, io.remaining()?.bytes_encoded, false),
            None => serde_json::to_vec(&Cell { value: self })
                .map(Into::into)
                .map_err(|_| corrupt(None)),
        }
    }

    fn decode(bytes: &[u8], io: Option<&PreparationIo>) -> Result<Self> {
        let (cell, hold): (Cell<Self>, _) = match io {
            Some(io) => {
                let (cell, hold) = io.decode(
                    std::marker::PhantomData::<Cell<Self>>,
                    bytes,
                    crate::wire::ErrorCode::Storage,
                )?;
                (cell, Some(hold))
            }
            None => (
                crate::contract::json_from_slice(bytes).map_err(|_| corrupt(None))?,
                None,
            ),
        };
        if let Some(mut hold) = hold {
            hold.substitute(cell.value.heap_bytes()?)?;
            hold.into_retained()?;
        }
        Ok(cell.value)
    }
}
impl Entry for bool {}
impl Entry for (u64, [u8; 32]) {}
impl Entry for Option<ModelEventAdmission> {
    fn encode(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        model_cell::encode(self.as_ref(), io)
    }

    fn decode(bytes: &[u8], io: Option<&PreparationIo>) -> Result<Self> {
        model_cell::decode(bytes, io)
    }

    fn heap_bytes(&self) -> Result<u64> {
        self.as_ref().map_or(Ok(0), |state| {
            crate::core::owned_sum(
                std::iter::once((state.calls.capacity() * std::mem::size_of::<String>()) as u64)
                    .chain(state.calls.iter().map(|call| call.capacity() as u64)),
            )
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cell<V> {
    value: V,
}

impl<V: Entry> PreparationRecord for Cell<V> {
    fn maximum_slot_bytes() -> u64 {
        // Entry is closed over scalars, a fixed tuple, and a model admission
        // containing strings. No general model/workflow DTO occurs in a cell.
        std::mem::size_of::<Self>().max(std::mem::size_of::<String>()) as u64
    }

    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        crate::core::owned_sum([std::mem::size_of::<Self>() as u64, self.value.heap_bytes()?])
            .map(Some)
    }
}

async fn load<K: SummaryKey, V: Entry, P: StreamProvider>(
    map: &mut SummaryMap<K, V>,
    reader: &Reader<'_, P>,
    root: Option<NodeRef>,
    key: K,
) -> Result<()> {
    let digest = key.digest(reader.io)?;
    if map.get_digest(&digest, reader.io)?.is_some() {
        return Ok(());
    }
    let Some(read) = reader.get(root, &digest).await? else {
        return Ok(());
    };
    let value = reader.value(read).await?;
    let value = value.bytes(reader.io)?;
    let cell = V::decode(value, reader.io)?;
    map.insert_digest(digest, cell, reader.io)
}

async fn store<V: Entry, P: StreamProvider>(
    writer: &mut Writer<'_, P>,
    root: Option<NodeRef>,
    key: [u8; 32],
    value: &V,
    io: Option<&PreparationIo>,
) -> Result<NodeRef> {
    let bytes = value.encode(io)?;
    let hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
    let result = writer.set_value(root, key, &bytes, io).await;
    drop(bytes);
    drop(hold);
    result
}

async fn snapshot<K: SummaryKey, V: Entry, P: StreamProvider>(
    writer: &mut Writer<'_, P>,
    map: &SummaryMap<K, V>,
    io: Option<&PreparationIo>,
) -> Result<Option<NodeRef>> {
    let mut root = None;
    for entry in map.entries() {
        let (key, value) = entry?;
        crate::model::charge_items(io, 1)?;
        root = Some(store(writer, root, key, value, io).await?);
    }
    Ok(root)
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn index_authenticates(
        &self,
        state: checkpoint::IndexState,
        sequence: u64,
        retry: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        if sequence > state.tail {
            return Ok(false);
        }
        let path = self.index_path(state.operation, "pages", io)?;
        let reader = Reader {
            provider: &self.stream,
            path: &path,
            base: state.nodes_tail,
            staged: &[],
            io,
            cache: None,
        };
        let key = crate::model::hash_bytes(retry.as_bytes(), io)?;
        let read = reader
            .get(state.roots[0], &key)
            .await?
            .ok_or_else(|| changed(io))?;
        let persistent_index::node::Node::Leaf { value, .. } = read.node(io)? else {
            return Err(corrupt(io));
        };
        let hold;
        let cell: Cell<(u64, [u8; 32])> = if let Some(io) = io {
            let decoded = io.decode(
                std::marker::PhantomData::<Cell<(u64, [u8; 32])>>,
                value,
                crate::wire::ErrorCode::Storage,
            )?;
            hold = Some(decoded.1);
            decoded.0
        } else {
            hold = None;
            crate::contract::json_from_slice(value).map_err(|_| corrupt(None))?
        };
        let digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                event,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })?;
        let valid = cell.value == (sequence, digest);
        drop(hold);
        if valid { Ok(true) } else { Err(changed(io)) }
    }

    pub(super) async fn preload_summary(
        &self,
        summary: &mut ExecutionSummary,
        retry: [u8; 32],
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if !summary.partial {
            return Ok(());
        }
        let Some(recovery) = summary.recovery.filter(|recovery| recovery.indexed) else {
            return Ok(());
        };
        let path = self.index_path(recovery.state.operation, "pages", io)?;
        let reader = Reader {
            provider: &self.stream,
            path: &path,
            base: recovery.state.nodes_tail,
            staged: &[],
            io,
            cache: Some(&summary.pages),
        };
        let roots = recovery.state.roots;
        load(&mut summary.retries, &reader, roots[0], retry).await?;
        match event {
            ExecutionEvent::ContextPrepared { step, .. }
            | ExecutionEvent::ContextCompacted { step, .. } => {
                load(&mut summary.contexts, &reader, roots[1], *step).await?;
                for purpose in [
                    crate::executor::ModelPurpose::Response,
                    crate::executor::ModelPurpose::Summary,
                ] {
                    load(&mut summary.models, &reader, roots[2], (*step, purpose)).await?;
                }
            }
            ExecutionEvent::ModelStarted { step, purpose, .. }
            | ExecutionEvent::Model { step, purpose, .. }
            | ExecutionEvent::ModelStopped { step, purpose } => {
                load(&mut summary.models, &reader, roots[2], (*step, *purpose)).await?;
            }
            ExecutionEvent::ToolStarted { step, call_id, .. }
            | ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => {
                let call = crate::model::hash_bytes(call_id.as_bytes(), io)?;
                load(&mut summary.tools, &reader, roots[3], (*step, call)).await?;
            }
            ExecutionEvent::Started { .. } => {}
        }
        Ok(())
    }

    pub(super) async fn stage_summary_index(
        &self,
        writer: &mut Writer<'_, P>,
        summary: &ExecutionSummary,
        retry: [u8; 32],
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<[Option<NodeRef>; 5]> {
        let Some(recovery) = summary.recovery.filter(|recovery| recovery.indexed) else {
            return Ok([
                snapshot(writer, &summary.retries, io).await?,
                snapshot(writer, &summary.contexts, io).await?,
                snapshot(writer, &summary.models, io).await?,
                snapshot(writer, &summary.tools, io).await?,
                None,
            ]);
        };
        let mut roots = recovery.state.roots;
        let value = summary
            .retries
            .get(&retry, io)?
            .ok_or_else(|| corrupt(io))?;
        roots[0] = Some(store(writer, roots[0], retry, value, io).await?);
        match event {
            ExecutionEvent::ContextPrepared { step, .. }
            | ExecutionEvent::ContextCompacted { step, .. } => {
                let value = summary.contexts.get(step, io)?.ok_or_else(|| corrupt(io))?;
                roots[1] = Some(store(writer, roots[1], step.digest(io)?, value, io).await?);
            }
            ExecutionEvent::ModelStarted { step, purpose, .. }
            | ExecutionEvent::Model { step, purpose, .. }
            | ExecutionEvent::ModelStopped { step, purpose } => {
                let key = (*step, *purpose);
                let value = summary.models.get(&key, io)?.ok_or_else(|| corrupt(io))?;
                roots[2] = Some(store(writer, roots[2], key.digest(io)?, value, io).await?);
            }
            ExecutionEvent::ToolStarted { step, call_id, .. }
            | ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => {
                let key = (*step, crate::model::hash_bytes(call_id.as_bytes(), io)?);
                let value = summary.tools.get(&key, io)?.ok_or_else(|| corrupt(io))?;
                roots[3] = Some(store(writer, roots[3], key.digest(io)?, value, io).await?);
            }
            ExecutionEvent::Started { .. } => {}
        }
        Ok(roots)
    }
}

fn corrupt(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "execution index summary value is invalid",
    )
}

fn changed(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Conflict,
        "authenticated execution prefix changed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn model_admission_cells_exceed_one_record_without_restricting_calls() -> Result<()> {
        for native in [false, true] {
            check_admission(native).await?;
        }
        Ok(())
    }

    async fn check_admission(native: bool) -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let path = acyclic_stream::StreamPath::new("model-cell/pages")?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 16 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let native_baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let value = Some(ModelEventAdmission {
            count: 9_000,
            calls: (0..9_000)
                .map(|index| format!("{index:08}{}", "x".repeat(120)))
                .collect(),
            completed: false,
        });
        assert!(
            io.encode(
                &Cell { value: &value },
                acyclic_stream::MAX_RECORD_BYTES as u64,
                false
            )
            .is_err(),
            "single-record negative control accepted the aggregate model cell"
        );
        let json = serde_json::to_vec(&Cell { value: &value }).expect("fixture model JSON");
        assert!(
            io.decode(
                std::marker::PhantomData::<Cell<Option<ModelEventAdmission>>>,
                &json,
                crate::wire::ErrorCode::Storage,
            )
            .is_err(),
            "generic JSON negative control fit the bounded model-cell decoder"
        );
        let key = (0, crate::executor::ModelPurpose::Response);
        let mut writer = Writer::new(&provider, &path, 0, Some(&io), None)?;
        let root = store(&mut writer, None, key.digest(Some(&io))?, &value, Some(&io)).await?;
        let tail = writer.tail()?;
        assert!(
            writer
                .records()
                .iter()
                .all(|bytes| bytes.len() <= acyclic_stream::MAX_RECORD_BYTES)
        );
        let mut offset = 0;
        for records in writer.records().chunks(8) {
            let outcome = provider
                .append(acyclic_stream::AppendRequest {
                    path: path.clone(),
                    records: records.to_vec(),
                    if_tail: Some(offset),
                    idempotency_key: None,
                })
                .await?;
            assert!(matches!(
                outcome,
                acyclic_stream::AppendOutcome::Committed(_)
            ));
            offset += records.len() as u64;
        }
        drop(writer);
        let reader = Reader {
            provider: &provider,
            path: &path,
            base: tail,
            staged: &[],
            io: Some(&io),
            cache: None,
        };
        let before = io.owned_bytes()?;
        let mut map = SummaryMap::default();
        load::<_, Option<ModelEventAdmission>, _>(&mut map, &reader, Some(root), key).await?;
        let held = io.guard_owned(io.owned_since(before)?)?;
        let restored = map
            .get(&key, Some(&io))?
            .and_then(Option::as_ref)
            .expect("restored model cell");
        assert_eq!(restored.count, 9_000);
        assert!(restored.calls == value.as_ref().expect("fixture admission").calls);
        drop(map);
        drop(held);
        drop(provider);
        assert_eq!(
            io.owned_bytes()?,
            0,
            "large model cell retained original custody"
        );
        if native {
            assert_eq!(
                work.live_allocation_bytes(),
                native_baseline,
                "large model cell retained original native allocations"
            );
        }
        Ok(())
    }
}
