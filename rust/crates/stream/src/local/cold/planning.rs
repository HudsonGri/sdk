//! Prepare coordinated semantics against one captured disk root. The working
//! set contains only paths named by this request, never historical decisions.
use super::*;
use crate::{
    CommitCondition, CommitConflict, CommitId, CommitMutation, CommitRequest, CommittedEnvelope,
    CommittedMutation, Record, StreamError, StreamPath,
};
use catalog::Head;
use envelope::Funded;
use root::Root;

pub(super) mod standalone;
#[cfg(test)]
mod tests;

fn semantic(error: StreamError) -> LocalStreamError {
    LocalStreamError::Replay(error)
}

fn stream_error(error: LocalStreamError) -> StreamError {
    match error {
        LocalStreamError::Replay(error) => error,
        LocalStreamError::InvalidLimits => StreamError::Capacity,
        _ => StreamError::Unavailable,
    }
}

fn path(
    path: &StreamPath,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<StreamPath, LocalStreamError> {
    match work {
        Some(work) => page::observe(
            Some(work),
            StreamPath::new_native(path.as_str(), &work.meter),
        )?
        .map_err(semantic),
        None => Ok(path.clone()),
    }
}

struct Snapshot {
    entries: Funded<Vec<(StreamPath, Option<Head>)>>,
    work: Option<Arc<crate::memory::Work>>,
}

impl Snapshot {
    fn capture(
        io: &mut impl PageRead,
        state: Root,
        request: &CommitRequest,
    ) -> Result<Self, LocalStreamError> {
        let maximum = request
            .mutations
            .len()
            .checked_mul(2)
            .and_then(|mutations| request.conditions.len().checked_add(mutations))
            .ok_or(LocalStreamError::InvalidLimits)?;
        let mut snapshot = Self {
            entries: Funded::vector(maximum, io.work())?,
            work: io.work().cloned(),
        };
        for condition in &request.conditions {
            snapshot.remember(io, state, crate::request::condition_path(condition))?;
        }
        for mutation in &request.mutations {
            snapshot.remember(io, state, crate::request::mutation_path(mutation))?;
            if let CommitMutation::Fork { source, .. } = mutation {
                snapshot.remember(io, state, source)?;
            }
        }
        Ok(snapshot)
    }

    fn remember(
        &mut self,
        io: &mut impl PageRead,
        state: Root,
        identity: &StreamPath,
    ) -> Result<(), LocalStreamError> {
        if self.find(identity).map_err(semantic)?.is_some() {
            return Ok(());
        }
        let head = catalog::get(io, state, identity)?;
        let path = path(identity, io.work())?;
        let copied = std::mem::size_of::<(StreamPath, Option<Head>)>() as u64;
        page::codec(io.work(), copied, 0, true)?;
        self.entries.value.push((path, head));
        page::codec(io.work(), copied, 0, false)
    }

    fn head(&self, path: &StreamPath) -> Result<Option<Head>, StreamError> {
        self.find(path)?.ok_or(StreamError::Unavailable)
    }

    fn find(&self, path: &StreamPath) -> Result<Option<Option<Head>>, StreamError> {
        for (identity, head) in &self.entries.value {
            let examined = (path.as_str().len() + identity.as_str().len()) as u64;
            if let Some(work) = &self.work {
                work.record(work.meter.admit_examination(1, examined))?;
            }
            let matches = identity == path;
            if let Some(work) = &self.work {
                work.record(work.meter.record_examination(1, examined))?;
            }
            if matches {
                return Ok(Some(*head));
            }
        }
        Ok(None)
    }

    fn conflicts(
        &self,
        io: &mut impl PageRead,
        conditions: &[CommitCondition],
    ) -> Result<Funded<Vec<CommitConflict>>, LocalStreamError> {
        let mut conflicts = Funded::vector(conditions.len(), io.work())?;
        for condition in conditions {
            let head = self
                .head(crate::request::condition_path(condition))
                .map_err(semantic)?;
            let actual = head.map(|head| head.tail);
            let preceding = match condition {
                CommitCondition::Head { expected, .. } if actual == Some(*expected) => {
                    let sequence = expected
                        .checked_sub(1)
                        .ok_or_else(|| semantic(StreamError::InvalidArgument))?;
                    head.ok_or(LocalStreamError::Corrupt)?
                        .commit_id(io, sequence)?
                        .ok_or(LocalStreamError::Corrupt)?
                }
                _ => CommitId::default(),
            };
            if let Some(mut conflict) =
                crate::preparation::condition_conflict(condition, actual, || Ok(preceding))
                    .map_err(semantic)?
            {
                let stored = match &mut conflict {
                    CommitConflict::Head { path, .. }
                    | CommitConflict::Tail { path, .. }
                    | CommitConflict::Exists { path } => path,
                };
                *stored = path(stored, io.work())?;
                let copied = std::mem::size_of::<CommitConflict>() as u64;
                page::codec(io.work(), copied, 0, true)?;
                conflicts.value.push(conflict);
                page::codec(io.work(), copied, 0, false)?;
            }
        }
        Ok(conflicts)
    }
}

pub(super) struct Prepared {
    pub(super) state: Root,
    pub(super) outcome: Funded<crate::CommitOutcome>,
    pub(super) heads: Funded<Vec<Head>>,
}

fn records(
    start: u64,
    bodies: &[crate::AppendRecord],
    commit: CommitId,
    time: u64,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<Funded<Vec<Record>>, StreamError> {
    let mut records = Funded::vector(bodies.len(), work).map_err(stream_error)?;
    let copied = u64::try_from(bodies.len().saturating_mul(std::mem::size_of::<Record>()))
        .map_err(|_| StreamError::Capacity)?;
    page::codec(work, copied, 0, true).map_err(stream_error)?;
    records.value = crate::preparation::records(start, bodies, commit, time, records.value)?;
    page::codec(work, copied, 0, false).map_err(stream_error)?;
    if let Some(work) = work {
        for record in &mut records.value {
            record.headers = work.record(record.headers.bind_native(&work.meter))?;
        }
    }
    Ok(records)
}

fn facts(
    snapshot: &Snapshot,
    source: &[CommitMutation],
    commit: CommitId,
    time: u64,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<Funded<CommittedEnvelope>, LocalStreamError> {
    let mut facts = Funded::vector(source.len(), work)?;
    let output = std::mem::take(&mut facts.value);
    facts.value = crate::preparation::facts(
        source,
        |path| Ok(snapshot.head(path)?.map(|head| head.tail)),
        |start, bodies| {
            let records = records(start, bodies, commit, time, work)?;
            facts.absorb(records).map_err(stream_error)
        },
        output,
    )
    .map_err(semantic)?;
    // The shared semantic planner clones caller paths. Bind output metadata to
    // its original ledger before the caller's admitted input can be retired.
    for fact in &mut facts.value {
        match fact {
            CommittedMutation::Append(append) => append.path = path(&append.path, work)?,
            CommittedMutation::Fork(fork) => {
                fork.source = path(&fork.source, work)?;
                fork.destination = path(&fork.destination, work)?;
            }
        }
    }
    Ok(facts.map(|mutations| CommittedEnvelope {
        commit_id: commit,
        mutations,
    }))
}

fn heads(
    snapshot: &Snapshot,
    envelope: &CommittedEnvelope,
    state: Root,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<Funded<Vec<Head>>, LocalStreamError> {
    let mut heads = Funded::vector(envelope.mutations.len(), work)?;
    for mutation in &envelope.mutations {
        let head = match mutation {
            CommittedMutation::Append(append) => snapshot
                .head(&append.path)
                .map_err(semantic)?
                .unwrap_or(Head {
                    tail: 0,
                    records: None,
                    created_decision: state.decision,
                }),
            CommittedMutation::Fork(fork) => snapshot
                .head(&fork.source)
                .map_err(semantic)?
                .ok_or_else(|| semantic(StreamError::NotFound))?
                .fork(fork.forked_at, state.decision)
                .map_err(semantic)?,
        };
        let copied = std::mem::size_of::<Head>() as u64;
        page::codec(work, copied, 0, true)?;
        heads.value.push(head);
        page::codec(work, copied, 0, false)?;
    }
    Ok(heads)
}

/// The request must have passed the canonical commit digest/shape validation.
/// Retry inspection and the trusted deadline check precede this preparation.
pub(super) fn prepare(
    io: &mut impl PageRead,
    state: Root,
    request: &CommitRequest,
    digest: [u8; 32],
    sampled_millis: u64,
) -> Result<Prepared, LocalStreamError> {
    let snapshot = Snapshot::capture(io, state, request)?;
    let conflicts = snapshot.conflicts(io, &request.conditions)?;
    if !conflicts.value.is_empty() {
        return Ok(Prepared {
            state,
            outcome: conflicts.map(crate::CommitOutcome::Conflict),
            heads: Funded::vector(0, io.work())?,
        });
    }
    drop(conflicts);
    crate::preparation::authority(request, |path| {
        Ok(snapshot.head(path)?.map(|head| head.tail))
    })
    .map_err(semantic)?;
    let (mut state, commit) = advance(io, state, digest)?;
    state.committed_at_micros =
        crate::preparation::committed_time(state.committed_at_micros, sampled_millis);
    let envelope = facts(
        &snapshot,
        &request.mutations,
        commit,
        state.committed_at_micros,
        io.work(),
    )?;
    let heads = heads(&snapshot, &envelope.value, state, io.work())?;
    Ok(Prepared {
        state,
        outcome: envelope.map(crate::CommitOutcome::Committed),
        heads,
    })
}

fn advance(
    io: &impl PageRead,
    mut state: Root,
    digest: [u8; 32],
) -> Result<(Root, CommitId), LocalStreamError> {
    state.decision = state
        .decision
        .checked_add(1)
        .ok_or_else(|| semantic(StreamError::Capacity))?;
    let hash_bytes = (b"acyclic.stream.commit.v1\0".len() + 8 + 32) as u64;
    page::codec(io.work(), 0, hash_bytes, true)?;
    let commit = crate::preparation::commit_identity(state.decision, digest);
    page::codec(io.work(), 0, hash_bytes, false)?;
    Ok((state, commit))
}
