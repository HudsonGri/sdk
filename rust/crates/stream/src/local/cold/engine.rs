//! One mutation path for both ingress modes. A validated request first probes
//! its retained decision; only a new decision can invoke authority or time.
use super::*;
use crate::{AppendRequest, CommitRequest, ForkRequest, IdempotencyKey, IdempotencyOutcome};
use envelope::Funded;
use retained::Family;
use store::Store;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
enum Request<'a> {
    Append(&'a AppendRequest),
    Fork(&'a ForkRequest),
    Commit(&'a CommitRequest),
}

/// Only constructors that compute the canonical digest can form this value.
/// The caller keeps its original ingress allocation alive with the borrow.
pub(super) struct Validated<'a> {
    request: Request<'a>,
    digest: [u8; 32],
    work: Option<Arc<crate::memory::Work>>,
}

fn digest(
    work: Option<&Arc<crate::memory::Work>>,
    bytes: u64,
    calculate: impl FnOnce() -> Result<[u8; 32], crate::StreamError>,
) -> Result<[u8; 32], LocalStreamError> {
    page::codec(work, 0, bytes, true)?;
    let digest = calculate().map_err(LocalStreamError::Replay)?;
    page::codec(work, 0, bytes, false)?;
    Ok(digest)
}

impl<'a> Validated<'a> {
    pub(super) fn append(
        request: &'a AppendRequest,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let digest = digest(
            work.as_ref(),
            crate::request::append_hash_bytes(request),
            || crate::request::append_digest(request),
        )?;
        Ok(Self {
            request: Request::Append(request),
            digest,
            work,
        })
    }

    pub(super) fn fork(
        request: &'a ForkRequest,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let digest = digest(
            work.as_ref(),
            crate::request::fork_hash_bytes(request),
            || crate::request::fork_digest(request),
        )?;
        Ok(Self {
            request: Request::Fork(request),
            digest,
            work,
        })
    }

    pub(super) fn commit(
        request: &'a mut CommitRequest,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let bytes = crate::request::commit_hash_bytes(request);
        let digest = digest(work.as_ref(), bytes, || {
            crate::request::commit_digest(request)
        })?;
        Ok(Self {
            request: Request::Commit(request),
            digest,
            work,
        })
    }

    fn binding(&self) -> (Option<&IdempotencyKey>, Family) {
        match self.request {
            Request::Append(request) => (request.idempotency_key.as_ref(), Family::Append),
            Request::Fork(request) => (request.idempotency_key.as_ref(), Family::Fork),
            Request::Commit(request) => (Some(&request.idempotency_key), Family::Commit),
        }
    }

    pub(super) fn replay(
        &self,
        store: &mut Store,
    ) -> Result<Option<Funded<IdempotencyOutcome>>, LocalStreamError> {
        let state = store.state()?;
        let (key, family) = self.binding();
        retry::replay(
            &mut store.reader(self.work.clone()),
            state,
            key,
            family,
            self.digest,
        )
    }

    /// The caller serializes this operation with other mutations. The guard is
    /// held through acceptance, and can therefore protect physical publication.
    /// `now` is sampled only after retry and authority; a retry never calls it.
    pub(super) fn execute<G>(
        &self,
        store: &mut Store,
        maximum_bytes: u64,
        before: impl FnOnce() -> Result<G, LocalStreamError>,
        mut now: impl FnMut() -> u64,
        deadline: Option<u64>,
    ) -> Result<Accepted<G>, LocalStreamError> {
        if let Some(outcome) = self.replay(store)? {
            return Ok(Accepted {
                outcome,
                changed: false,
                cut: None,
                _guard: None,
            });
        }
        let guard = before()?;
        let sampled = now();
        if deadline.is_some_and(|deadline| sampled >= deadline) {
            return Err(LocalStreamError::Replay(
                crate::StreamError::DeadlineElapsed,
            ));
        }
        let mut transaction = store.begin(maximum_bytes, self.work.clone())?;
        let state = transaction.state;
        let applied = match self.request {
            Request::Append(request) => {
                let prepared = planning::standalone::append(
                    &mut transaction,
                    state,
                    request,
                    self.digest,
                    sampled,
                )?;
                publication::standalone::apply(
                    &mut transaction,
                    prepared,
                    request.idempotency_key.as_ref(),
                    self.digest,
                )?
            }
            Request::Fork(request) => {
                let prepared =
                    planning::standalone::fork(&mut transaction, state, request, self.digest)?;
                publication::standalone::apply(
                    &mut transaction,
                    prepared,
                    request.idempotency_key.as_ref(),
                    self.digest,
                )?
            }
            Request::Commit(request) => {
                let prepared =
                    planning::prepare(&mut transaction, state, request, self.digest, sampled)?;
                publication::apply(
                    &mut transaction,
                    prepared,
                    &request.idempotency_key,
                    self.digest,
                )?
            }
        };
        let changed = applied.state != state;
        transaction.state = applied.state;
        if deadline.is_some_and(|deadline| now() >= deadline) {
            return Err(LocalStreamError::Replay(
                crate::StreamError::DeadlineElapsed,
            ));
        }
        // This publishes only accepted visibility. Device synchronization is
        // separate, so followers can observe the new cut before it completes.
        let cut = changed.then(|| transaction.cut());
        if changed {
            transaction.accept()?;
        }
        Ok(Accepted {
            outcome: applied.outcome,
            changed,
            cut,
            _guard: Some(guard),
        })
    }
}

pub(super) struct Accepted<G> {
    pub(super) outcome: Funded<IdempotencyOutcome>,
    pub(super) changed: bool,
    pub(super) cut: Option<store::Cut>,
    _guard: Option<G>,
}
