//! Canonical durable lineage payload, shared by backend adapters.
use super::*;
use crate::kernel::codec::{Decoder, EncodeWorkStep, Encoder};
use crate::{FsError, WorkCounters, WorkError};

const DOMAIN: &[u8] = b"ACYFSLIN";

pub(crate) fn encode(
    record: &WorkspaceLineageRecord,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<u8>>, FsError> {
    if !lineage_shape_accounted(record, accounting).map_err(shape_error)? {
        return Err(FsError::InvalidDiff);
    }
    let mut length = DOMAIN.len() + 2 + 4 + 8 + 1 + 16 + 4 + 1 + 1 + 64;
    length = length
        .checked_add(record.workspace_name.len())
        .ok_or(WorkError::Overflow)?;
    if let Some(name) = &record.parent_workspace_name {
        length = length
            .checked_add(16 + 4)
            .and_then(|n| n.checked_add(name.len()))
            .ok_or(WorkError::Overflow)?;
    }
    if record.creation_operation.is_some() {
        length = length.checked_add(16).ok_or(WorkError::Overflow)?;
    }
    let (bytes, allocation) =
        crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
            let mut out = Encoder::with_exact_capacity(
                DOMAIN,
                1,
                length,
                |step| observe(EncodeWorkStep::Allocation(step)),
                allocation_error,
            )?;
            let body = WorkCounters {
                bytes_encoded: (length - DOMAIN.len() - 2) as u64,
                bytes_copied: (length - DOMAIN.len() - 2) as u64,
                ..WorkCounters::UNCHARGED
            };
            observe(EncodeWorkStep::Admit(body))?;
            out.u32(record.version);
            out.u64(record.revision);
            out.u8(u8::from(record.ready));
            out.fixed(&record.workspace_id.into_bytes());
            out.bounded_bytes(record.workspace_name.as_bytes())?;
            match (
                record.parent_workspace_id,
                record.parent_workspace_name.as_deref(),
            ) {
                (Some(id), Some(name)) => {
                    out.u8(1);
                    out.fixed(&id.into_bytes());
                    out.bounded_bytes(name.as_bytes())?;
                }
                (None, None) => out.u8(0),
                _ => return Err(FsError::InvalidDiff),
            }
            match record.creation_operation {
                Some(operation) => {
                    out.u8(1);
                    out.fixed(&operation.into_bytes());
                }
                None => out.u8(0),
            }
            out.fixed(record.fork_generation.digest().as_bytes());
            out.fixed(record.initial_generation.digest().as_bytes());
            observe(EncodeWorkStep::Completed(body))?;
            Ok(out.finish())
        })?;
    let retained = bytes.capacity() as u64;
    Ok(Owned::new(bytes, allocation, retained))
}

pub(crate) fn decode(
    bytes: &[u8],
    maximum: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceLineageRecord>, FsError> {
    let (record, allocation) = crate::kernel::decode_owned(accounting, |steps| {
        let mut input = Decoder::new(bytes, DOMAIN, 1, maximum, &mut |s| steps(s))?;
        let version = input.u32(&mut |s| steps(s))?;
        let revision = input.u64(&mut |s| steps(s))?;
        let ready = match input.u8(&mut |s| steps(s))? {
            0 => false,
            1 => true,
            _ => return Err(FsError::InvalidDiff),
        };
        let workspace_id = WorkspaceId::from_bytes(input.fixed(&mut |s| steps(s))?);
        let workspace_name = String::from_utf8(input.bounded_bytes(u32::MAX, &mut |s| steps(s))?)
            .map_err(|_| FsError::InvalidDiff)?;
        let (parent_workspace_id, parent_workspace_name) = match input.u8(&mut |s| steps(s))? {
            0 => (None, None),
            1 => (
                Some(WorkspaceId::from_bytes(input.fixed(&mut |s| steps(s))?)),
                Some(
                    String::from_utf8(input.bounded_bytes(u32::MAX, &mut |s| steps(s))?)
                        .map_err(|_| FsError::InvalidDiff)?,
                ),
            ),
            _ => return Err(FsError::InvalidDiff),
        };
        let creation_operation = match input.u8(&mut |s| steps(s))? {
            0 => None,
            1 => Some(crate::OperationId::from_bytes(
                input.fixed(&mut |s| steps(s))?,
            )),
            _ => return Err(FsError::InvalidDiff),
        };
        let fork_generation =
            GenerationId::new(crate::Digest::from_bytes(input.fixed(&mut |s| steps(s))?));
        let initial_generation =
            GenerationId::new(crate::Digest::from_bytes(input.fixed(&mut |s| steps(s))?));
        input.finish()?;
        Ok(WorkspaceLineageRecord {
            version,
            revision,
            ready,
            workspace_id,
            workspace_name,
            parent_workspace_id,
            parent_workspace_name,
            creation_operation,
            fork_generation,
            initial_generation,
        })
    })?;
    let retained = record
        .workspace_name
        .capacity()
        .checked_add(
            record
                .parent_workspace_name
                .as_ref()
                .map_or(0, String::capacity),
        )
        .ok_or(WorkError::Overflow)? as u64;
    let record = Owned::new(record, allocation, retained);
    let valid = lineage_shape_accounted(&record.value, accounting);
    match valid {
        Ok(true) => Ok(record),
        result => {
            record.retire(accounting)?;
            result.map_err(shape_error)?;
            Err(FsError::InvalidDiff)
        }
    }
}

fn shape_error(error: WorkspaceError) -> FsError {
    match error {
        WorkspaceError::Filesystem(error) => error,
        _ => FsError::InvalidDiff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facade::FsWork as _;
    use proptest::prelude::*;

    fn record(seed: [u8; 16], suffix: u32, child: bool) -> WorkspaceLineageRecord {
        let mut parent = seed;
        parent[0] ^= 0x80;
        let generation = GenerationId::new(crate::Digest::from_bytes([2; 32]));
        WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: WorkspaceId::from_bytes(seed),
            workspace_name: format!("workspace-{suffix}"),
            parent_workspace_id: child.then(|| WorkspaceId::from_bytes(parent)),
            parent_workspace_name: child.then(|| "parent".to_owned()),
            creation_operation: child.then(|| crate::OperationId::from_bytes(seed)),
            fork_generation: generation,
            initial_generation: generation,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn durable_lineage_codec_has_one_exact_round_trip_and_no_valid_prefix(
            seed in any::<[u8; 16]>(), suffix in any::<u32>(), child in any::<bool>(),
        ) {
            let cancellation = CancellationToken::new();
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
            let record = record(seed, suffix, child);
            let encoded = encode(&record, &mut accounting).unwrap();
            let decoded = decode(&encoded.value, u64::MAX, &mut accounting).unwrap();
            prop_assert_eq!(&decoded.value, &record);
            decoded.retire(&mut accounting).unwrap();
            for prefix in 0..encoded.value.len() {
                prop_assert!(decode(&encoded.value[..prefix], u64::MAX, &mut accounting).is_err());
            }
            encoded.retire(&mut accounting).unwrap();
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn durable_lineage_decode_precharges_every_original_allocation_cut() {
        let cancellation = CancellationToken::new();
        let mut setup = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let encoded = encode(&record([9; 16], 42, true), &mut setup).unwrap();
        let measure = crate::NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .unwrap();
        let mut accounting = KernelAccounting::native(&measure, &cancellation);
        let baseline = measure.live_allocation_bytes();
        let decoded = decode(&encoded.value, u64::MAX, &mut accounting).unwrap();
        assert_eq!(
            measure.live_allocation_bytes(),
            baseline
                + decoded.value.workspace_name.capacity() as u64
                + decoded
                    .value
                    .parent_workspace_name
                    .as_ref()
                    .map_or(0, String::capacity) as u64
        );
        decoded.retire(&mut accounting).unwrap();
        let operations = measure.receipt().allocation_operations;
        for allowed in 1..=operations {
            let mut budget = crate::performance::BOUNDARY_BUDGET;
            budget.allocation_operations = allowed;
            let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let result = decode(&encoded.value, u64::MAX, &mut accounting);
            assert_eq!(result.is_ok(), allowed == operations, "cut={allowed}");
            if let Ok(record) = result {
                record.retire(&mut accounting).unwrap();
            }
            assert_eq!(original.live_allocation_bytes(), baseline, "cut={allowed}");
        }
        encoded.retire(&mut setup).unwrap();
    }
}
