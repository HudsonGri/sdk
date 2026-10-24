use super::*;
use crate::local::cold::decode::Cursor;
use crate::{AppendReceipt, CommitId, ForkReceipt};
use envelope::Funded;

fn conflicts(
    cursor: &mut Cursor<'_>,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<Funded<IdempotencyOutcome>, LocalStreamError> {
    let count = cursor.count()?;
    // Every conflict contains a tag and at least one nonempty canonical path.
    if count == 0 || count > crate::MAX_ITEMS || count > cursor.0.len() / 6 {
        return Err(LocalStreamError::Corrupt);
    }
    let mut conflicts = Funded::vector(count, work)?;
    for _ in 0..count {
        let kind = cursor.byte()?;
        let path = cursor.path(work)?;
        let conflict = match kind {
            0 => CommitConflict::Head {
                path,
                expected: CommitId::from_bytes(*cursor.fixed()?),
                actual: CommitId::from_bytes(*cursor.fixed()?),
            },
            1 => {
                let expected = cursor.number()?;
                let actual = match cursor.byte()? {
                    0 => None,
                    1 => Some(cursor.number()?),
                    _ => return Err(LocalStreamError::Corrupt),
                };
                CommitConflict::Tail {
                    path,
                    expected,
                    actual,
                }
            }
            2 => CommitConflict::Exists { path },
            _ => return Err(LocalStreamError::Corrupt),
        };
        let copied = std::mem::size_of::<CommitConflict>() as u64;
        page::codec(work, copied, 0, true)?;
        conflicts.value.push(conflict);
        page::codec(work, copied, 0, false)?;
    }
    conflicts_len(&conflicts.value)?;
    Ok(conflicts.map(|values| IdempotencyOutcome::Commit(CommitOutcome::Conflict(values))))
}

pub(in crate::local::cold) fn read(
    io: &mut impl PageRead,
    reference: PageRef,
) -> Result<Funded<IdempotencyOutcome>, LocalStreamError> {
    let work = io.work().cloned();
    let page = io.read(reference)?;
    if page.kind != PageKind::Fact {
        return Err(LocalStreamError::Corrupt);
    }
    let mut cursor = Cursor(page.body()?);
    if cursor.byte()? != TAG {
        return Err(LocalStreamError::Corrupt);
    }
    let mut envelope_reference = None;
    let result = match cursor.byte()? {
        0 => {
            let start = cursor.number()?;
            let end = cursor.number()?;
            let commit_id = CommitId::from_bytes(*cursor.fixed()?);
            Funded::scalar(
                IdempotencyOutcome::Append(AppendOutcome::Committed(AppendReceipt {
                    start,
                    end,
                    tail: end,
                    commit_id,
                })),
                work.as_ref(),
            )
        }
        1 => Funded::scalar(
            IdempotencyOutcome::Append(AppendOutcome::TailConflict {
                actual_tail: cursor.number()?,
            }),
            work.as_ref(),
        ),
        2 => {
            let source = cursor.path(work.as_ref())?;
            let destination = cursor.path(work.as_ref())?;
            let forked_at = cursor.number()?;
            let commit_id = CommitId::from_bytes(*cursor.fixed()?);
            Funded::scalar(
                IdempotencyOutcome::Fork(ForkReceipt {
                    source,
                    destination,
                    forked_at,
                    tail: forked_at,
                    commit_id,
                }),
                work.as_ref(),
            )
        }
        3 => {
            let commit = CommitId::from_bytes(*cursor.fixed()?);
            let mut bytes = cursor.fixed::<52>()?.as_slice();
            let child = PageRef::decode(&mut bytes)?;
            if !child.precedes(reference) {
                return Err(LocalStreamError::Corrupt);
            }
            envelope_reference = Some(child);
            envelope::read(io, child, commit)?
                .map(|envelope| IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)))
        }
        4 => conflicts(&mut cursor, work.as_ref())?,
        _ => return Err(LocalStreamError::Corrupt),
    };
    if !cursor.0.is_empty() || encoded_len(&result.value, envelope_reference)? != page.body()?.len()
    {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(result)
}
