//! The append and fork families retain their own digests and receipts. They
//! share immutable publication machinery without changing their conditions.
use super::*;
use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, CommittedFork, ForkReceipt, ForkRequest,
    IdempotencyOutcome,
};

pub(in crate::local::cold) struct Standalone {
    pub(in crate::local::cold) state: Root,
    pub(in crate::local::cold) envelope: Option<Funded<CommittedEnvelope>>,
    pub(in crate::local::cold) heads: Funded<Vec<Head>>,
    pub(in crate::local::cold) outcome: Funded<IdempotencyOutcome>,
}

fn one<T>(
    value: T,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<Funded<Vec<T>>, LocalStreamError> {
    let mut values = Funded::vector(1, work)?;
    let copied = std::mem::size_of::<T>() as u64;
    page::codec(work, copied, 0, true)?;
    values.value.push(value);
    page::codec(work, copied, 0, false)?;
    Ok(values)
}

/// Canonical validation/digest and retry lookup precede this read-only plan.
pub(in crate::local::cold) fn append(
    io: &mut impl PageRead,
    state: Root,
    request: &AppendRequest,
    digest: [u8; 32],
    sampled_millis: u64,
) -> Result<Standalone, LocalStreamError> {
    let existing = catalog::get(io, state, &request.path)?;
    let start = existing.map_or(0, |head| head.tail);
    if request.if_tail.is_some_and(|expected| expected != start) {
        return Ok(Standalone {
            state,
            envelope: None,
            heads: Funded::vector(0, io.work())?,
            outcome: Funded::scalar(
                IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: start }),
                io.work(),
            ),
        });
    }
    let (mut state, commit) = advance(io, state, digest)?;
    state.committed_at_micros =
        crate::preparation::committed_time(state.committed_at_micros, sampled_millis);
    let records = records(
        start,
        &request.records,
        commit,
        state.committed_at_micros,
        io.work(),
    )
    .map_err(semantic)?;
    let mut facts = Funded::vector(1, io.work())?;
    let records = facts.absorb(records)?;
    let fact = crate::preparation::append_fact(path(&request.path, io.work())?, start, records)
        .map_err(semantic)?;
    let outcome = Funded::scalar(
        IdempotencyOutcome::Append(AppendOutcome::Committed(AppendReceipt {
            start,
            end: fact.end,
            tail: fact.tail,
            commit_id: commit,
        })),
        io.work(),
    );
    let copied = std::mem::size_of::<CommittedMutation>() as u64;
    page::codec(io.work(), copied, 0, true)?;
    facts.value.push(CommittedMutation::Append(fact));
    page::codec(io.work(), copied, 0, false)?;
    let heads = one(
        existing.unwrap_or(Head {
            tail: 0,
            created_decision: state.decision,
            records: None,
        }),
        io.work(),
    )?;
    Ok(Standalone {
        state,
        envelope: Some(facts.map(|mutations| CommittedEnvelope {
            commit_id: commit,
            mutations,
        })),
        heads,
        outcome,
    })
}

/// A standalone fork advances decision order, preserving the previous record
/// clock because it creates no new records, as in the reference backend.
pub(in crate::local::cold) fn fork(
    io: &mut impl PageRead,
    state: Root,
    request: &ForkRequest,
    digest: [u8; 32],
) -> Result<Standalone, LocalStreamError> {
    let destination = catalog::get(io, state, &request.destination)?;
    if destination.is_some() {
        return Err(semantic(StreamError::AlreadyExists));
    }
    let source = catalog::get(io, state, &request.source)?;
    let cut = crate::preparation::fork_cut(request, source.map(|head| head.tail), None)
        .map_err(semantic)?;
    let (state, commit) = advance(io, state, digest)?;
    let head = source
        .ok_or_else(|| semantic(StreamError::NotFound))?
        .fork(cut, state.decision)
        .map_err(semantic)?;
    let source = path(&request.source, io.work())?;
    let destination = path(&request.destination, io.work())?;
    let outcome = Funded::scalar(
        IdempotencyOutcome::Fork(ForkReceipt {
            source: source.clone(),
            destination: destination.clone(),
            forked_at: cut,
            tail: cut,
            commit_id: commit,
        }),
        io.work(),
    );
    let facts = one(
        CommittedMutation::Fork(CommittedFork {
            source,
            destination,
            forked_at: cut,
            tail: cut,
            records: Vec::new(),
        }),
        io.work(),
    )?;
    Ok(Standalone {
        state,
        envelope: Some(facts.map(|mutations| CommittedEnvelope {
            commit_id: commit,
            mutations,
        })),
        heads: one(head, io.work())?,
        outcome,
    })
}
