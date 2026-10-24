use super::*;
use crate::{AppendOutcome, IdempotencyOutcome};
use planning::standalone::Standalone;

#[cfg(test)]
mod tests;

pub(in crate::local::cold) fn apply(
    io: &mut impl PageIo,
    mut prepared: Standalone,
    key: Option<&IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Applied, LocalStreamError> {
    let family = match (&prepared.outcome.value, &prepared.envelope) {
        (IdempotencyOutcome::Append(AppendOutcome::Committed(_)), Some(_))
        | (IdempotencyOutcome::Append(AppendOutcome::TailConflict { .. }), None) => {
            retained::Family::Append
        }
        (IdempotencyOutcome::Fork(_), Some(_)) => retained::Family::Fork,
        _ => return Err(LocalStreamError::Corrupt),
    };
    if let Some(envelope) = &prepared.envelope {
        committed(
            io,
            &mut prepared.state,
            &envelope.value,
            &mut prepared.heads.value,
        )?;
    }
    if let Some(key) = key {
        let terminal = outcome::append(io, &prepared.outcome.value, None)?;
        retained::put(
            io,
            &mut prepared.state,
            key,
            retained::Binding {
                family,
                request_digest: digest,
                outcome: terminal,
            },
        )?;
    }
    Ok(Applied {
        state: prepared.state,
        outcome: prepared.outcome,
    })
}
