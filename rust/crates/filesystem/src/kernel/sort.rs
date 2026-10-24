//! One allocation-free, before-effect sort for all filesystem frontiers.
use super::KernelAccounting;
use crate::{FsError, WorkCounters, WorkError};
use std::cmp::Ordering;

pub(crate) fn sort_by_accounted<T>(
    values: &mut [T],
    accounting: &mut KernelAccounting<'_>,
    mut compare: impl FnMut(&T, &T) -> Ordering,
) -> Result<(), FsError> {
    let count = values.len();
    if count < 2 {
        return accounting.check();
    }
    accounting.admit(examination(count - 1)?)?;
    let mut ordered = true;
    for index in 1..count {
        if compare_at(values, index - 1, index, accounting, &mut compare)?.is_gt() {
            ordered = false;
            break;
        }
    }
    if ordered {
        return Ok(());
    }
    let maximum = u64::try_from(count)
        .map_err(|_| WorkError::Overflow)?
        .checked_mul(u64::from(usize::BITS - count.leading_zeros()))
        .and_then(|value| value.checked_mul(3))
        .ok_or(WorkError::Overflow)?;
    accounting.admit(WorkCounters {
        items_examined: maximum,
        ..WorkCounters::UNCHARGED
    })?;
    for start in (0..count / 2).rev() {
        sift_down(values, start, count, accounting, &mut compare)?;
    }
    for end in (1..count).rev() {
        swap(values, 0, end, accounting)?;
        sift_down(values, 0, end, accounting, &mut compare)?;
    }
    Ok(())
}

fn examination(count: usize) -> Result<WorkCounters, FsError> {
    Ok(WorkCounters {
        items_examined: u64::try_from(count).map_err(|_| WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    })
}

fn compare_at<T>(
    values: &[T],
    left: usize,
    right: usize,
    accounting: &mut KernelAccounting<'_>,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> Result<Ordering, FsError> {
    let left = values.get(left).ok_or(FsError::InvalidDiff)?;
    let right = values.get(right).ok_or(FsError::InvalidDiff)?;
    let work = examination(1)?;
    accounting.admit(work)?;
    let result = compare(left, right);
    accounting.record_completed(work)?;
    Ok(result)
}

fn swap<T>(
    values: &mut [T],
    left: usize,
    right: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if left >= values.len() || right >= values.len() {
        return Err(FsError::InvalidDiff);
    }
    let bytes = std::mem::size_of::<T>()
        .checked_mul(2)
        .ok_or(WorkError::Overflow)?;
    let work = WorkCounters {
        bytes_copied: u64::try_from(bytes).map_err(|_| WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(work)?;
    values.swap(left, right);
    accounting.record_completed(work)
}

fn sift_down<T>(
    values: &mut [T],
    mut root: usize,
    end: usize,
    accounting: &mut KernelAccounting<'_>,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> Result<(), FsError> {
    loop {
        let Some(left) = root
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .filter(|left| *left < end)
        else {
            return Ok(());
        };
        let right = left + 1;
        let greater =
            if right < end && compare_at(values, left, right, accounting, compare)?.is_lt() {
                right
            } else {
                left
            };
        if !compare_at(values, root, greater, accounting, compare)?.is_lt() {
            return Ok(());
        }
        swap(values, root, greater, accounting)?;
        root = greater;
    }
}

#[cfg(test)]
mod tests;
