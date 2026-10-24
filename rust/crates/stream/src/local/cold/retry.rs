//! Retry lookup is a read-only operation, before conditions, source authority
//! or trusted deadlines. It can never append a replacement terminal result.
use super::*;
use crate::{IdempotencyKey, IdempotencyOutcome};
use envelope::Funded;
use retained::Family;
use root::Root;

#[cfg(test)]
mod tests;

pub(super) fn replay(
    io: &mut impl PageRead,
    state: Root,
    key: Option<&IdempotencyKey>,
    family: Family,
    digest: [u8; 32],
) -> Result<Option<Funded<IdempotencyOutcome>>, LocalStreamError> {
    let Some(key) = key else {
        return Ok(None);
    };
    let Some(binding) = retained::get(io, state, key)? else {
        return Ok(None);
    };
    if binding.family != family || binding.request_digest != digest {
        return Err(LocalStreamError::Replay(
            crate::StreamError::IdempotencyMismatch,
        ));
    }
    let outcome = outcome::read(io, binding.outcome)?;
    let actual = match &outcome.value {
        IdempotencyOutcome::Append(_) => Family::Append,
        IdempotencyOutcome::Fork(_) => Family::Fork,
        IdempotencyOutcome::Commit(_) => Family::Commit,
    };
    if actual != family {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(Some(outcome))
}
