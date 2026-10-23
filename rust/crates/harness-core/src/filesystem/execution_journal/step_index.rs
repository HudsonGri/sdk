//! Authenticated per-step observation chains survive a fresh stock driver.
use super::*;
use crate::executor::ExecutionRecordQuery;
use persistent_index::{Funded, NodeRef, Reader, Writer, node::Node};

mod custody;

#[derive(Clone, Copy)]
struct Entry {
    step: Option<u32>,
    sequence: u64,
    retry: [u8; 32],
    digest: [u8; 32],
    tool: Option<([u8; 32], bool)>,
}

struct Entries {
    tail: u64,
    entries: Funded<Entry>,
}

#[derive(Default)]
pub(super) struct Pending(Vec<Entry>);

impl Pending {
    pub(super) fn push(
        &mut self,
        sequence: u64,
        retry: [u8; 32],
        digest: [u8; 32],
        step: Option<u32>,
        tool: Option<([u8; 32], bool)>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if self.0.len() == self.0.capacity() {
            let capacity = self
                .0
                .capacity()
                .checked_mul(2)
                .ok_or_else(|| invalid(io))?
                .max(1);
            crate::executor::stock_vec_reserve(&mut self.0, capacity, io)?;
        }
        self.0.push(Entry {
            step,
            sequence,
            retry,
            digest,
            tool,
        });
        Ok(())
    }

    pub(super) fn clear(&mut self) {
        self.0.clear();
    }

    pub(super) async fn stage<P: StreamProvider>(
        &self,
        writer: &mut Writer<'_, P>,
        mut root: Option<NodeRef>,
    ) -> Result<Option<NodeRef>> {
        for entry in &self.0 {
            let key = key(entry.step);
            let reader = writer.reader();
            let previous = reader.get(root, &key).await?.map(|read| read.reference());
            let value = encode(entry, previous);
            root = Some(writer.set(root, key, value.as_slice()).await?);
        }
        Ok(root)
    }
}

fn key(step: Option<u32>) -> [u8; 32] {
    let mut bytes = [0; 32];
    if let Some(step) = step {
        bytes[0] = 1;
        bytes[1..5].copy_from_slice(&step.to_le_bytes());
    }
    bytes
}

struct EncodedEntry {
    bytes: [u8; 145],
    has_tool: bool,
}

impl EncodedEntry {
    fn as_slice(&self) -> &[u8] {
        if self.has_tool {
            &self.bytes
        } else {
            &self.bytes[..113]
        }
    }
}

fn encode(entry: &Entry, previous: Option<NodeRef>) -> EncodedEntry {
    let mut bytes = [0; 145];
    bytes[..8].copy_from_slice(&entry.sequence.to_le_bytes());
    bytes[8..40].copy_from_slice(&entry.retry);
    bytes[40..72].copy_from_slice(&entry.digest);
    if let Some(previous) = previous {
        bytes[72] = 1;
        bytes[73..81].copy_from_slice(&previous.sequence.to_le_bytes());
        bytes[81..113].copy_from_slice(&previous.digest);
    }
    if let Some((call, settled)) = entry.tool {
        bytes[72] |= 2 | (u8::from(settled) << 2);
        bytes[113..].copy_from_slice(&call);
    }
    EncodedEntry {
        bytes,
        has_tool: entry.tool.is_some(),
    }
}

fn decode(
    value: &[u8],
    reference: NodeRef,
    step: Option<u32>,
    io: Option<&PreparationIo>,
) -> Result<(Entry, Option<NodeRef>)> {
    let bytes: &[u8; 113] = value
        .get(..113)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| invalid(io))?;
    let tool = match (bytes[72] & 6, value.len()) {
        (0, 113) => None,
        (flags @ (2 | 6), 145) => Some((
            value
                .get(113..)
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| invalid(io))?,
            flags == 6,
        )),
        _ => return Err(invalid(io)),
    };
    if bytes[72] & !7 != 0 {
        return Err(invalid(io));
    }
    let previous = match bytes[72] & 1 {
        0 if bytes[73..113].iter().all(|byte| *byte == 0) => None,
        1 => {
            let sequence = u64::from_le_bytes(bytes[73..81].try_into().map_err(|_| invalid(io))?);
            if sequence >= reference.sequence {
                return Err(invalid(io));
            }
            Some(NodeRef {
                sequence,
                digest: bytes[81..113].try_into().map_err(|_| invalid(io))?,
            })
        }
        _ => return Err(invalid(io)),
    };
    let entry = Entry {
        step,
        sequence: u64::from_le_bytes(bytes[..8].try_into().map_err(|_| invalid(io))?),
        retry: bytes[8..40].try_into().map_err(|_| invalid(io))?,
        digest: bytes[40..72].try_into().map_err(|_| invalid(io))?,
        tool,
    };
    if entry.sequence == 0 {
        return Err(invalid(io));
    }
    Ok((entry, previous))
}

/// Tool admission is part of the step chain, rather than a second durable map
/// of the same observations. Newest matching state wins; strictly decreasing
/// record positions prevent cycles and keep recovery within this one step.
pub(super) async fn tool_state<P: StreamProvider>(
    reader: &Reader<'_, P>,
    root: Option<NodeRef>,
    tail: u64,
    step: u32,
    call: [u8; 32],
) -> Result<Option<bool>> {
    let io = reader.io;
    let mut read = reader.get(root, &key(Some(step))).await?;
    let mut sequence = tail.checked_add(1).ok_or_else(|| invalid(io))?;
    while let Some(node) = read {
        let Node::Leaf { key: found, value } = node.node(io)? else {
            return Err(invalid(io));
        };
        if found != key(Some(step)) {
            return Err(invalid(io));
        }
        let (entry, previous) = decode(value, node.reference(), Some(step), io)?;
        if entry.sequence >= sequence {
            return Err(invalid(io));
        }
        sequence = entry.sequence;
        if let Some((found, settled)) = entry.tool
            && found == call
        {
            return Ok(Some(settled));
        }
        read = match previous {
            Some(previous) => Some(reader.read(previous).await?),
            None => None,
        };
    }
    Ok(None)
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn indexed_step(
        &self,
        query: ExecutionRecordQuery<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(u64, Vec<ExecutionRecord>)>> {
        let pool = custody::QueryIo::new(io)?;
        let result = self.indexed_step_inner(query, pool.io()).await?;
        pool.finish()?;
        Ok(result)
    }

    async fn indexed_step_inner(
        &self,
        query: ExecutionRecordQuery<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(u64, Vec<ExecutionRecord>)>> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let Some(Entries { tail, entries }) = self.step_entries(&query, io).await? else {
            return Ok(None);
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.retain_owned(std::mem::size_of::<Vec<ExecutionRecord>>() as u64)?;
        }
        let mut selected = Vec::new();
        for entry in &entries.values {
            self.select_step_record(&query, *entry, &mut selected, io)
                .await?;
        }
        drop(entries);
        crate::model::charge_items(io, selected.len() as u64 * u64::from(usize::BITS))?;
        selected.sort_unstable_by_key(|record| record.sequence);
        retain_output(&selected, selected.capacity(), before, io)?;
        Ok(Some((tail, selected)))
    }

    async fn step_entries(
        &self,
        query: &ExecutionRecordQuery<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Entries>> {
        self.require_operation(query.operation, io)?;
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
                self.maximum_payload_bytes,
                work,
            )
        })?;
        let mut cache = self.verified.lock().await;
        self.refresh_cached_summary(query.operation, &mut cache, io)
            .await?;
        let summary = cache.summary();
        let Some(recovery) = summary
            .recovery
            .filter(|recovery| recovery.indexed && recovery.state.tail == summary.tail)
        else {
            return Ok(None);
        };
        let state = recovery.state;
        let path = self.index_path(query.operation, "pages", io)?;
        let reader = Reader {
            provider: &self.stream,
            path: &path,
            base: state.nodes_tail,
            staged: &[],
            io,
            cache: Some(&summary.pages),
        };
        let mut entries = Funded::new(io)?;
        for step in [None, Some(query.step)] {
            if step.is_none() && !query.include_started {
                continue;
            }
            collect_step(
                &reader,
                state.roots[4],
                state.tail,
                step,
                if step.is_none() {
                    1
                } else {
                    query.maximum_step_records
                },
                &mut entries,
            )
            .await?;
        }
        Ok(Some(Entries {
            tail: state.tail,
            entries,
        }))
    }

    async fn select_step_record(
        &self,
        query: &ExecutionRecordQuery<'_>,
        entry: Entry,
        selected: &mut Vec<ExecutionRecord>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let records = self
            .records(query.operation, entry.sequence - 1, 1, io)
            .await?;
        let hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        let record = records.first().ok_or_else(|| invalid(io))?;
        let digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                &record.event,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })?;
        if record.sequence != entry.sequence
            || record.event.step() != entry.step
            || crate::model::hash_bytes(record.idempotency_key.as_bytes(), io)? != entry.retry
            || digest != entry.digest
        {
            return Err(invalid(io));
        }
        if (query.select)(&record.event) {
            if selected.len() >= query.maximum {
                return Err(invalid(io));
            }
            if selected.len() == selected.capacity() {
                let capacity = selected
                    .capacity()
                    .checked_mul(2)
                    .ok_or_else(|| invalid(io))?
                    .max(1);
                crate::executor::stock_vec_reserve(selected, capacity, io)?;
            }
            selected.push(ExecutionRecord {
                operation_id: record.operation_id,
                sequence: record.sequence,
                idempotency_key: crate::executor::stock_string_copy(&record.idempotency_key, io)?,
                event: record.event.copy_indexed(io)?,
            });
        }
        drop(records);
        drop(hold);
        Ok(())
    }
}

async fn collect_step<P: StreamProvider>(
    reader: &Reader<'_, P>,
    root: Option<NodeRef>,
    tail: u64,
    step: Option<u32>,
    maximum: usize,
    entries: &mut Funded<Entry>,
) -> Result<()> {
    let io = reader.io;
    let mut read = reader.get(root, &key(step)).await?;
    if read.is_none() && step.is_none() {
        return Err(invalid(io));
    }
    let mut previous_sequence = tail.checked_add(1).ok_or_else(|| invalid(io))?;
    let mut count = 0;
    while let Some(node) = read {
        count += 1;
        if count > maximum {
            return Err(invalid(io));
        }
        let Node::Leaf { key: found, value } = node.node(io)? else {
            return Err(invalid(io));
        };
        if found != key(step) {
            return Err(invalid(io));
        }
        let (entry, previous) = decode(value, node.reference(), step, io)?;
        if entry.sequence >= previous_sequence {
            return Err(invalid(io));
        }
        previous_sequence = entry.sequence;
        entries.push(entry)?;
        read = match previous {
            Some(previous) => Some(reader.read(previous).await?),
            None => None,
        };
    }
    Ok(())
}

fn invalid(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "execution step index is invalid",
    )
}

fn retain_output(
    records: &[ExecutionRecord],
    capacity: usize,
    before: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if let Some(io) = io {
        #[cfg(not(target_arch = "wasm32"))]
        let frame = if io.original_native_work().is_some() {
            std::mem::size_of::<Vec<ExecutionRecord>>() as u64
        } else {
            0
        };
        #[cfg(target_arch = "wasm32")]
        let frame = 0;
        let bytes = (capacity as u64)
            .checked_mul(std::mem::size_of::<ExecutionRecord>() as u64)
            .and_then(|bytes| bytes.checked_add(frame))
            .ok_or_else(|| invalid(Some(io)))?;
        let bytes = records.iter().try_fold(bytes, |bytes, record| {
            bytes
                .checked_add(record.payload_bytes(Some(io))?)
                .ok_or_else(|| invalid(Some(io)))
        })?;
        let mut hold = io.guard_owned(io.owned_since(before)?)?;
        hold.substitute(bytes)?;
        hold.into_retained()?;
    }
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
