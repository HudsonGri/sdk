//! Ordered immutable Patricia index. Depth depends on the bounded key width,
//! never the number of historical facts. All traversal is iterative.
use super::*;

const MAX_KEY_BYTES: usize = crate::MAX_PATH_BYTES + crate::MAX_IDEMPOTENCY_KEY_BYTES + 16;
const MAX_BITS: usize = MAX_KEY_BYTES * 9 + 1;

#[derive(Clone, Copy)]
struct Branch {
    bit: usize,
    left: PageRef,
    right: PageRef,
}

#[derive(Clone, Copy)]
struct Edge {
    reference: PageRef,
    branch: Branch,
    right: bool,
}

enum Node<'a> {
    Leaf { key: &'a [u8], value: PageRef },
    Branch(Branch),
}

/// A leaf reached through the validated index traversal. Callers cannot turn
/// an arbitrary fact page into a decoded key/value pair.
pub(super) struct Leaf {
    page: Page,
    reference: PageRef,
}

fn key_bit(key: &[u8], bit: usize) -> bool {
    let Some(byte) = key.get(bit / 9) else {
        return false;
    };
    let within = bit % 9;
    within == 0 || byte & (1 << (8 - within)) != 0
}

fn difference(left: &[u8], right: &[u8]) -> Option<usize> {
    let common = left.iter().zip(right).take_while(|(a, b)| a == b).count();
    match (left.get(common), right.get(common)) {
        (Some(left), Some(right)) => Some(common * 9 + 1 + (left ^ right).leading_zeros() as usize),
        (None, None) => None,
        _ => Some(common * 9),
    }
}

fn decode(page: &Page, reference: PageRef) -> Result<Node<'_>, LocalStreamError> {
    let mut body = page.body()?;
    let (number, rest) = body
        .split_first_chunk::<4>()
        .ok_or(LocalStreamError::Corrupt)?;
    let number = u32::from_le_bytes(*number) as usize;
    body = rest;
    match page.kind {
        PageKind::Leaf => {
            if number > MAX_KEY_BYTES {
                return Err(LocalStreamError::Corrupt);
            }
            let (key, rest) = body
                .split_at_checked(number)
                .ok_or(LocalStreamError::Corrupt)?;
            body = rest;
            let value = PageRef::decode(&mut body)?;
            if !body.is_empty() || !value.precedes(reference) {
                return Err(LocalStreamError::Corrupt);
            }
            Ok(Node::Leaf { key, value })
        }
        PageKind::Branch => {
            let left = PageRef::decode(&mut body)?;
            let right = PageRef::decode(&mut body)?;
            if number >= MAX_BITS
                || !body.is_empty()
                || !left.precedes(reference)
                || !right.precedes(reference)
                || left == right
            {
                return Err(LocalStreamError::Corrupt);
            }
            Ok(Node::Branch(Branch {
                bit: number,
                left,
                right,
            }))
        }
        _ => Err(LocalStreamError::Corrupt),
    }
}

fn leaf(io: &mut impl PageIo, key: &[u8], value: PageRef) -> Result<PageRef, LocalStreamError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let mut body = Buffer::new(4 + key.len() + 52, io.work())?;
    page::codec(io.work(), (4 + key.len() + 52) as u64, 0, true)?;
    body.bytes.extend_from_slice(
        &u32::try_from(key.len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    )?;
    body.bytes.extend_from_slice(key)?;
    body.bytes.extend_from_slice(&value.encoded())?;
    page::codec(io.work(), body.bytes.len() as u64, 0, false)?;
    io.append(PageKind::Leaf, &body.bytes)
}

fn branch(io: &mut impl PageIo, branch: Branch) -> Result<PageRef, LocalStreamError> {
    let mut body = [0; 108];
    let (bit, children) = body.split_at_mut(4);
    bit.copy_from_slice(
        &u32::try_from(branch.bit)
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    );
    let (left, right) = children.split_at_mut(52);
    left.copy_from_slice(&branch.left.encoded());
    right.copy_from_slice(&branch.right.encoded());
    io.append(PageKind::Branch, &body)
}

fn edge(
    reference: PageRef,
    branch: Branch,
    key: &[u8],
    previous: Option<usize>,
) -> Result<(Edge, PageRef), LocalStreamError> {
    if previous.is_some_and(|previous| previous >= branch.bit) {
        return Err(LocalStreamError::Corrupt);
    }
    let right = key_bit(key, branch.bit);
    Ok((
        Edge {
            reference,
            branch,
            right,
        },
        if right { branch.right } else { branch.left },
    ))
}

pub(super) fn get(
    io: &mut impl PageRead,
    root: Option<PageRef>,
    key: &[u8],
) -> Result<Option<PageRef>, LocalStreamError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let Some(mut reference) = root else {
        return Ok(None);
    };
    let mut previous = None;
    loop {
        let page = io.read(reference)?;
        match decode(&page, reference)? {
            Node::Leaf { key: found, value } => return Ok((found == key).then_some(value)),
            Node::Branch(branch) => {
                reference = edge(reference, branch, key, previous)?.1;
                previous = Some(branch.bit);
            }
        }
    }
}

pub(super) fn put(
    io: &mut impl PageIo,
    root: Option<PageRef>,
    key: &[u8],
    value: PageRef,
) -> Result<PageRef, LocalStreamError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let Some(mut reference) = root else {
        return leaf(io, key, value);
    };
    let mut trail = Slots::<Edge>::default();
    let split = loop {
        let page = io.read(reference)?;
        match decode(&page, reference)? {
            Node::Leaf {
                key: found,
                value: old,
            } => {
                let split = difference(found, key);
                if split.is_none() && old == value {
                    return root.ok_or(LocalStreamError::Corrupt);
                }
                break split;
            }
            Node::Branch(branch) => {
                let previous = trail.values.last().map(|edge| edge.branch.bit);
                let (step, next) = edge(reference, branch, key, previous)?;
                trail.push(step, io.work())?;
                reference = next;
            }
        }
    };
    let mut replacement = leaf(io, key, value)?;
    if let Some(bit) = split {
        while trail
            .values
            .last()
            .is_some_and(|edge| edge.branch.bit >= bit)
        {
            reference = trail
                .values
                .pop()
                .ok_or(LocalStreamError::Corrupt)?
                .reference;
        }
        let (left, right) = if key_bit(key, bit) {
            (reference, replacement)
        } else {
            (replacement, reference)
        };
        replacement = branch(io, Branch { bit, left, right })?;
    }
    while let Some(step) = trail.values.pop() {
        let mut next = step.branch;
        if step.right {
            next.right = replacement;
        } else {
            next.left = replacement;
        }
        replacement = branch(io, next)?;
    }
    Ok(replacement)
}

/// Remove one exact key by replacing its parent with the surviving sibling.
/// Older roots remain immutable, including the deleted key and its fact.
pub(super) fn remove(
    io: &mut impl PageIo,
    root: Option<PageRef>,
    key: &[u8],
) -> Result<Option<PageRef>, LocalStreamError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let Some(mut reference) = root else {
        return Ok(None);
    };
    let mut trail = Slots::<Edge>::default();
    loop {
        let page = io.read(reference)?;
        match decode(&page, reference)? {
            Node::Leaf { key: found, .. } => {
                if found != key {
                    return Ok(root);
                }
                break;
            }
            Node::Branch(branch) => {
                let previous = trail.values.last().map(|edge| edge.branch.bit);
                let (step, next) = edge(reference, branch, key, previous)?;
                trail.push(step, io.work())?;
                reference = next;
            }
        }
    }
    let Some(parent) = trail.values.pop() else {
        return Ok(None);
    };
    let mut replacement = if parent.right {
        parent.branch.left
    } else {
        parent.branch.right
    };
    while let Some(step) = trail.values.pop() {
        let mut next = step.branch;
        if step.right {
            next.right = replacement;
        } else {
            next.left = replacement;
        }
        replacement = branch(io, next)?;
    }
    Ok(Some(replacement))
}

fn first(
    io: &mut impl PageRead,
    mut reference: PageRef,
    mut previous: Option<usize>,
) -> Result<Leaf, LocalStreamError> {
    loop {
        let page = io.read(reference)?;
        match decode(&page, reference)? {
            Node::Leaf { .. } => return Ok(Leaf { page, reference }),
            Node::Branch(branch) => {
                if previous.is_some_and(|previous| previous >= branch.bit) {
                    return Err(LocalStreamError::Corrupt);
                }
                previous = Some(branch.bit);
                reference = branch.left;
            }
        }
    }
}

/// The first key strictly after `after`, or the first key when it is absent.
pub(super) fn next(
    io: &mut impl PageRead,
    root: Option<PageRef>,
    after: Option<&[u8]>,
) -> Result<Option<Leaf>, LocalStreamError> {
    let Some(mut reference) = root else {
        return Ok(None);
    };
    let Some(after) = after else {
        return first(io, reference, None).map(Some);
    };
    if after.len() > MAX_KEY_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let mut trail = Slots::<Edge>::default();
    let (split, greater) = loop {
        let page = io.read(reference)?;
        match decode(&page, reference)? {
            Node::Leaf { key, .. } => break (difference(key, after), key > after),
            Node::Branch(branch) => {
                let previous = trail.values.last().map(|edge| edge.branch.bit);
                let (step, next) = edge(reference, branch, after, previous)?;
                trail.push(step, io.work())?;
                reference = next;
            }
        }
    };
    if let Some(bit) = split {
        while trail
            .values
            .last()
            .is_some_and(|edge| edge.branch.bit >= bit)
        {
            reference = trail
                .values
                .pop()
                .ok_or(LocalStreamError::Corrupt)?
                .reference;
        }
        if greater {
            return first(
                io,
                reference,
                trail.values.last().map(|edge| edge.branch.bit),
            )
            .map(Some);
        }
    }
    while let Some(step) = trail.values.pop() {
        if !step.right {
            return first(io, step.branch.right, Some(step.branch.bit)).map(Some);
        }
    }
    Ok(None)
}

pub(super) fn entry(leaf: &Leaf) -> Result<(&[u8], PageRef), LocalStreamError> {
    // The leaf was already address-checked while traversing. Decode its bounded
    // fields again without following a reference or allocating another key.
    match decode(&leaf.page, leaf.reference)? {
        Node::Leaf { key, value } => Ok((key, value)),
        Node::Branch(_) => Err(LocalStreamError::Corrupt),
    }
}
