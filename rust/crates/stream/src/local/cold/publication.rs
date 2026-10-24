//! Build one immutable publication from a prepared coordinated decision. The
//! caller installs the returned root only after its storage transaction seals.
use super::*;
use crate::{CommittedEnvelope, CommittedMutation, IdempotencyKey, IdempotencyOutcome, StreamPath};
use envelope::Funded;
use root::Root;

pub(super) mod standalone;
#[cfg(test)]
mod tests;

fn paths(
    io: &mut impl PageIo,
    state: &mut Root,
    path: &StreamPath,
    commit: crate::CommitId,
) -> Result<(), LocalStreamError> {
    for end in path
        .as_str()
        .char_indices()
        .filter_map(|(position, value)| (value == '/').then_some(position))
        .chain(std::iter::once(path.as_str().len()))
    {
        let text = path.as_str().get(..end).ok_or(LocalStreamError::Corrupt)?;
        let ancestor = path.ancestor(text).ok_or(LocalStreamError::Corrupt)?;
        if catalog::get(io, *state, &ancestor)?.is_none() {
            let head = catalog::Head {
                tail: 0,
                created_decision: state.decision,
                records: None,
            };
            catalog::put(io, state, &ancestor, head)?;
            children::insert(io, state, &ancestor, commit)?;
        }
    }
    Ok(())
}

fn committed(
    io: &mut impl PageIo,
    state: &mut Root,
    envelope: &CommittedEnvelope,
    heads: &mut [catalog::Head],
) -> Result<PageRef, LocalStreamError> {
    if heads.len() != envelope.mutations.len() {
        return Err(LocalStreamError::Corrupt);
    }
    let count = envelope
        .mutations
        .iter()
        .try_fold(0_usize, |count, mutation| {
            let records = match mutation {
                CommittedMutation::Append(append) => &append.records,
                CommittedMutation::Fork(fork) => &fork.records,
            };
            count
                .checked_add(records.len())
                .ok_or(LocalStreamError::InvalidLimits)
        })?;
    let mut references = Funded::vector(count, io.work())?;
    for (position, (mutation, head)) in envelope.mutations.iter().zip(heads).enumerate() {
        let (path, records) = match mutation {
            CommittedMutation::Append(append) => (&append.path, &append.records),
            CommittedMutation::Fork(fork) => (&fork.destination, &fork.records),
        };
        paths(io, state, path, envelope.commit_id)?;
        for (offset, record) in records.iter().enumerate() {
            let reference = head.append_record(io, record)?;
            let copied = std::mem::size_of::<(usize, usize, PageRef)>() as u64;
            page::codec(io.work(), copied, 0, true)?;
            references.value.push((position, offset, reference));
            page::codec(io.work(), copied, 0, false)?;
        }
        catalog::put(io, state, path, *head)?;
    }
    let mut selected = references.value.iter();
    let reference = envelope::append(io, envelope, |position, offset| {
        let &(actual_position, actual_offset, reference) =
            selected.next().ok_or(LocalStreamError::Corrupt)?;
        if (position, offset) != (actual_position, actual_offset) {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(reference)
    })?;
    if selected.next().is_some() {
        return Err(LocalStreamError::Corrupt);
    }
    retained::put_envelope(io, state, envelope.commit_id, reference)?;
    Ok(reference)
}

pub(super) struct Applied {
    pub(super) state: Root,
    pub(super) outcome: Funded<IdempotencyOutcome>,
}

/// Every read during planning used the pre-publication root, including forks
/// from paths also appended in this decision. Only the private root advances.
pub(super) fn apply(
    io: &mut impl PageIo,
    mut prepared: planning::Prepared,
    key: &IdempotencyKey,
    digest: [u8; 32],
) -> Result<Applied, LocalStreamError> {
    let reference = match &prepared.outcome.value {
        crate::CommitOutcome::Committed(envelope) => Some(committed(
            io,
            &mut prepared.state,
            envelope,
            &mut prepared.heads.value,
        )?),
        crate::CommitOutcome::Conflict(_) => None,
    };
    let outcome = prepared.outcome.map(IdempotencyOutcome::Commit);
    let terminal = outcome::append(io, &outcome.value, reference)?;
    retained::put(
        io,
        &mut prepared.state,
        key,
        retained::Binding {
            family: retained::Family::Commit,
            request_digest: digest,
            outcome: terminal,
        },
    )?;
    Ok(Applied {
        state: prepared.state,
        outcome,
    })
}
