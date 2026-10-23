use super::*;

// Full namespace scans arrive in file-identity order. Append first so a tiny
// directory rename never shifts the unrelated namespace once per path.
pub(super) fn append(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
    let (path, mut allocation) = path.clone_with_accounting(accounting, cancellation)?;
    let copied = copy_work(std::mem::size_of::<ChangedPath>() as u64);
    accounting.admit(copied)?;
    paths.1.absorb(&mut allocation)?;
    paths.0.push(ChangedPath {
        path,
        before: if before { record } else { None },
        after: if before { None } else { record },
    });
    accounting.record_completed(copied)?;
    Ok(())
}

pub(super) fn sort_and_coalesce(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let count = paths.0.len();
    // In-place heapsort bounds both work and live allocation on adversarial
    // namespaces. Every comparison and swap uses the original caller's ledger.
    for start in (0..count / 2).rev() {
        sift_down(&mut paths.0, start, count, accounting)?;
    }
    for end in (1..count).rev() {
        swap(&mut paths.0, 0, end, accounting)?;
        sift_down(&mut paths.0, 0, end, accounting)?;
    }
    let mut retained = 0_usize;
    let mut retired_bytes = 0_u64;
    for index in 0..count {
        accounting.charge_items(1)?;
        let entry = paths.0.get(index).ok_or(FsError::InvalidDiff)?;
        let previous = retained.checked_sub(1).and_then(|i| paths.0.get(i));
        if previous.is_some_and(|previous| previous.path == entry.path) {
            let (before, after) = (entry.before, entry.after);
            retired_bytes = retired_bytes
                .checked_add(entry.path.retained_bytes())
                .ok_or(crate::WorkError::Overflow)?;
            let copied =
                copy_work((2 * std::mem::size_of::<Option<crate::kernel::FileRecord>>()) as u64);
            accounting.admit(copied)?;
            let previous = paths.0.get_mut(retained - 1).ok_or(FsError::InvalidDiff)?;
            previous.before = previous.before.or(before);
            previous.after = previous.after.or(after);
            accounting.record_completed(copied)?;
        } else {
            if retained != index {
                swap(&mut paths.0, retained, index, accounting)?;
            }
            retained += 1;
        }
    }
    paths.0.truncate(retained);
    accounting.release_owned(&mut paths.1, retired_bytes)?;
    accounting.check()?;
    Ok(())
}

fn compare(
    values: &[ChangedPath],
    left: usize,
    right: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<std::cmp::Ordering, WorkspaceError> {
    accounting.charge_items(1)?;
    let left = values.get(left).ok_or(FsError::InvalidDiff)?;
    let right = values.get(right).ok_or(FsError::InvalidDiff)?;
    Ok(left.path.components().cmp(right.path.components()))
}

fn swap(
    values: &mut [ChangedPath],
    left: usize,
    right: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = copy_work((2 * std::mem::size_of::<ChangedPath>()) as u64);
    accounting.admit(copied)?;
    values.swap(left, right);
    accounting.record_completed(copied)?;
    Ok(())
}

fn sift_down(
    values: &mut [ChangedPath],
    mut root: usize,
    end: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    loop {
        let Some(left) = root
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .filter(|left| *left < end)
        else {
            return Ok(());
        };
        let right = left + 1;
        let greater = if right < end && compare(values, left, right, accounting)?.is_lt() {
            right
        } else {
            left
        };
        if !compare(values, root, greater, accounting)?.is_lt() {
            return Ok(());
        }
        swap(values, root, greater, accounting)?;
        root = greater;
    }
}
