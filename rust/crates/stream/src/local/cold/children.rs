//! Two durable orderings of permanent direct children. Continuations seek an
//! immutable key instead of replaying the children preceding the anchor.
use super::*;
use crate::{CommitId, StreamPath};

#[cfg(test)]
mod tests;

const TAG: u8 = 3;

#[derive(Clone, Copy)]
pub(super) enum Order {
    Creation,
    Alphabetical,
}

pub(super) struct Child {
    pub(super) path: StreamPath,
    pub(super) decision: u64,
}

fn key(
    io: &impl PageRead,
    parent: Option<&StreamPath>,
    order: Order,
    child: Option<(&StreamPath, u64)>,
) -> Result<Buffer, LocalStreamError> {
    let parent = parent.map_or("", StreamPath::as_str);
    let name = child.map_or("", |(path, _)| {
        path.as_str().rsplit('/').next().unwrap_or("")
    });
    let length = 1
        + parent.len()
        + 1
        + name.len()
        + match order {
            Order::Creation => 8,
            Order::Alphabetical => 0,
        };
    let mut output = Buffer::new(length, io.work())?;
    page::codec(io.work(), length as u64, 0, true)?;
    output.bytes.push(match order {
        Order::Creation => 0,
        Order::Alphabetical => 1,
    });
    output.bytes.extend_from_slice(parent.as_bytes());
    output.bytes.push(0);
    if matches!(order, Order::Creation) {
        output
            .bytes
            .extend_from_slice(&child.map_or(0, |(_, decision)| decision).to_be_bytes());
    }
    output.bytes.extend_from_slice(name.as_bytes());
    page::codec(io.work(), length as u64, 0, false)?;
    Ok(output)
}

fn fact(
    io: &mut impl PageIo,
    path: &StreamPath,
    decision: u64,
) -> Result<PageRef, LocalStreamError> {
    if decision == 0 {
        return Err(LocalStreamError::Corrupt);
    }
    let length = 1 + 8 + path.as_str().len();
    let mut bytes = Buffer::new(length, io.work())?;
    page::codec(io.work(), length as u64, 0, true)?;
    bytes.bytes.push(TAG);
    bytes.bytes.extend_from_slice(&decision.to_le_bytes());
    bytes.bytes.extend_from_slice(path.as_str().as_bytes());
    page::codec(io.work(), length as u64, 0, false)?;
    io.append(PageKind::Fact, &bytes.bytes)
}

fn decode(io: &mut impl PageRead, reference: PageRef) -> Result<Child, LocalStreamError> {
    let page = io.read(reference)?;
    if page.kind != PageKind::Fact {
        return Err(LocalStreamError::Corrupt);
    }
    let (tag, rest) = page
        .body()?
        .split_first()
        .ok_or(LocalStreamError::Corrupt)?;
    if *tag != TAG {
        return Err(LocalStreamError::Corrupt);
    }
    let (decision, text) = rest
        .split_first_chunk::<8>()
        .ok_or(LocalStreamError::Corrupt)?;
    let decision = u64::from_le_bytes(*decision);
    if decision == 0 {
        return Err(LocalStreamError::Corrupt);
    }
    let text = std::str::from_utf8(text).map_err(|_| LocalStreamError::Corrupt)?;
    let path = match io.work() {
        Some(work) => page::observe(Some(work), StreamPath::new_native(text, &work.meter))?
            .map_err(|_| LocalStreamError::Corrupt)?,
        None => StreamPath::new(text).map_err(|_| LocalStreamError::Corrupt)?,
    };
    Ok(Child { path, decision })
}

/// Called only when the point catalog creates this path. Both indexes and the
/// hierarchy watermark are published in the same immutable root transaction.
pub(super) fn insert(
    io: &mut impl PageIo,
    state: &mut root::Root,
    path: &StreamPath,
    commit: CommitId,
) -> Result<(), LocalStreamError> {
    let reference = fact(io, path, state.decision)?;
    let parent = path.parent();
    for order in [Order::Creation, Order::Alphabetical] {
        let key = key(io, parent.as_ref(), order, Some((path, state.decision)))?;
        state.children = Some(index::put(io, state.children, &key.bytes, reference)?);
    }
    state.hierarchy_version = commit;
    Ok(())
}

/// Returns one funded child. The caller validates the public continuation's
/// hierarchy version (pages) or its permanent creation anchor (live follows).
pub(super) fn next(
    io: &mut impl PageRead,
    state: root::Root,
    parent: Option<&StreamPath>,
    order: Order,
    after: Option<(&StreamPath, u64)>,
) -> Result<Option<Child>, LocalStreamError> {
    if after.is_some_and(|(path, decision)| {
        path.parent().as_ref() != parent || decision == 0 || decision > state.decision
    }) {
        return Err(LocalStreamError::Corrupt);
    }
    let seek = key(io, parent, order, after)?;
    let Some(leaf) = index::next(io, state.children, Some(&seek.bytes))? else {
        return Ok(None);
    };
    let (found, reference) = index::entry(&leaf)?;
    let prefix = key(io, parent, Order::Alphabetical, None)?;
    // Ignore the ordering discriminator and creation suffix in the prefix.
    let parent_prefix = prefix.bytes.get(1..).ok_or(LocalStreamError::Corrupt)?;
    if found.first() != seek.bytes.first()
        || !found
            .get(1..)
            .is_some_and(|bytes| bytes.starts_with(parent_prefix))
    {
        return Ok(None);
    }
    let child = decode(io, reference)?;
    if child.path.parent().as_ref() != parent || child.decision > state.decision {
        return Err(LocalStreamError::Corrupt);
    }
    let expected = key(io, parent, order, Some((&child.path, child.decision)))?;
    if found != expected.bytes {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(Some(child))
}
