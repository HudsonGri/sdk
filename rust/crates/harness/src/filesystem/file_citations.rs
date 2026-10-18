//! Snapshot-pinned citation reads through the existing volume provider and policy.
use super::PreparationIo;
use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, VolumeClass, VolumeRef},
    durable_host::TaskVolumeSnapshot,
    file_citations::{FileCitation, FileCitations, MAX_BLOCKS, visit_blocks},
    runtime::TaskContext,
};
use acyclic_fs::WorkCounters;
use acyclic_markdown::directive::{Excerpt, Kind, Resolution, ResolutionError, TargetRef};
use std::ops::Range;

#[cfg(test)]
thread_local! { pub(crate) static READ_WHOLE_FILE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { pub(crate) static PIN_PROJECT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

fn message_error(error: ResolutionError, source: &str) -> FileCitations {
    FileCitations {
        source_bytes: source.len() as u64,
        error: Some(Resolution::Error(error)),
        ..FileCitations::default()
    }
}

fn allocation(io: &PreparationIo, bytes: usize) -> Result<()> {
    if bytes != 0 {
        io.retain_owned(bytes as u64)?;
        io.allocation_attempt(bytes as u64)?;
    }
    Ok(())
}

fn copy(io: &PreparationIo, value: &str) -> Result<String> {
    allocation(io, value.len())?;
    io.charge(WorkCounters {
        bytes_copied: value.len() as u64,
        ..Default::default()
    })?;
    Ok(value.to_owned())
}

/// Resolve at one immutable cut. The caller retains the original owner fence.
pub(crate) async fn resolve(
    context: &TaskContext,
    source: &str,
    io: &PreparationIo,
) -> Result<FileCitations> {
    let maximum = context.scope().limits().file_bytes;
    if source.len() as u64 > maximum {
        return Ok(message_error(ResolutionError::TooLarge, source));
    }
    io.charge(WorkCounters {
        items_examined: (source.len() as u64).saturating_mul(2),
        ..Default::default()
    })?;
    let mut count = 0;
    let counted = visit_blocks(source, |_, _| {
        count += 1;
        if count > MAX_BLOCKS { Err(()) } else { Ok(()) }
    });
    if counted.is_err() {
        return Ok(message_error(ResolutionError::TooMany, source));
    }
    if count == 0 {
        return Ok(FileCitations::default());
    }
    let Some(volume) = context
        .content_binding(None)
        .and_then(|binding| binding.writer.as_ref())
        .map(|writer| writer.volume())
        .filter(|volume| volume.class() == VolumeClass::Project)
    else {
        return Ok(message_error(ResolutionError::Denied, source));
    };
    allocation(io, count * std::mem::size_of::<FileCitation>())?;
    let mut snapshot = FileCitations {
        source_bytes: source.len() as u64,
        view: None,
        entries: Vec::with_capacity(count),
        ..FileCitations::default()
    };
    // Only ranges are retained while reads yield; syntax and path fields borrow the message.
    allocation(io, count * std::mem::size_of::<Range<usize>>())?;
    let ranges_hold = io.guard_owned((count * std::mem::size_of::<Range<usize>>()) as u64)?;
    let mut ranges = Vec::with_capacity(count);
    visit_blocks(source, |range, _| {
        ranges.push(range);
        Ok::<_, Error>(())
    })?;
    let mut remaining = maximum;
    for range in ranges {
        let raw = source
            .get(range.clone())
            .ok_or_else(|| io.invalid("citation range is invalid"))?;
        let target = TargetRef::parse(raw, Kind::Block)
            .ok_or_else(|| io.invalid("citation syntax changed"))?;
        let path_valid = match crate::executor::with_history_io(Some(io), |work| {
            crate::conversation::validate_content_path(target.path, work)
        }) {
            Ok(()) => true,
            Err(error) if error.code() == crate::wire::ErrorCode::Invalid => false,
            Err(error) => return Err(error),
        };
        let (resolution, read) = if target.symbol.is_some() {
            (Resolution::Error(ResolutionError::UnsupportedSymbol), None)
        } else if !path_valid
            || crate::conversation::is_internal_path(target.path)
            || target.path.as_bytes().get(1) == Some(&b':')
                && target
                    .path
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphabetic)
            || target.path.len() > context.scope().limits().path_bytes
        {
            (Resolution::Error(ResolutionError::OutsideBoundary), None)
        } else {
            match prefix(context, volume, target.path, io) {
                Ok(prefix) => {
                    let reader = &context
                        .content_binding(Some(volume))
                        .ok_or_else(|| io.invalid("citation reader disappeared"))?
                        .reader;
                    excerpt(
                        reader.as_ref(),
                        volume,
                        prefix,
                        &target,
                        remaining,
                        &mut snapshot.view,
                        io,
                    )
                    .await?
                }
                Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => {
                    (Resolution::Error(ResolutionError::Denied), None)
                }
                Err(error) => return Err(error),
            }
        };
        if let Some(read) = &read {
            remaining = remaining.saturating_sub(read.end);
        }
        snapshot.entries.push(FileCitation {
            range,
            raw: copy(io, raw)?,
            resolution,
            read,
        });
    }
    drop(ranges_hold);
    Ok(snapshot)
}

fn prefix<'a>(
    context: &TaskContext,
    volume: &VolumeRef,
    path: &'a str,
    io: &PreparationIo,
) -> Result<&'a str> {
    // Only an existing ancestor grant can authorize the model's path. The
    // bound reader separately verifies its signed scope and creation lease.
    for end in std::iter::once(path.len())
        .chain(path.match_indices('/').map(|(at, _)| at).rev())
        .chain([0])
    {
        let prefix = path
            .get(..end)
            .ok_or_else(|| io.invalid("citation prefix is invalid"))?;
        let (exact, full) = crate::executor::with_history_io(Some(io), |work| {
            Ok::<_, Error>((
                volume.directory_read_capability(prefix, work)?,
                volume.capability(crate::conversation::VolumeOperation::Read, work)?,
            ))
        })?;
        let _hold = io.guard_owned((exact.capacity() + full.capacity()) as u64)?;
        if context.scope().grants().contains(&exact) || context.scope().grants().contains(&full) {
            return Ok(prefix);
        }
    }
    Err(crate::executor::stock_boundary(
        Some(io),
        crate::wire::ErrorCode::Unauthorized,
        "task scope cannot read the citation path",
    ))
}

fn file_error(error: &Error) -> Option<ResolutionError> {
    #[cfg(not(target_arch = "wasm32"))]
    match error {
        Error::NativeWorkspace { source, .. } => {
            return match source.as_ref() {
                acyclic_fs::WorkspaceError::NotFound => Some(ResolutionError::Read),
                acyclic_fs::WorkspaceError::NotDirectory
                | acyclic_fs::WorkspaceError::NotRegularFile => {
                    Some(ResolutionError::OutsideBoundary)
                }
                acyclic_fs::WorkspaceError::ReadLimitExceeded => Some(ResolutionError::TooLarge),
                acyclic_fs::WorkspaceError::Filesystem(source) => filesystem_error(source),
                _ => None,
            };
        }
        Error::NativeFilesystem { source, .. } => return filesystem_error(source),
        _ => {}
    }
    match error {
        Error::NotFound(_) | Error::Unsupported(_) => Some(ResolutionError::Read),
        Error::Unauthorized(_) => Some(ResolutionError::Denied),
        #[cfg(not(target_arch = "wasm32"))]
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        } => Some(ResolutionError::Read),
        #[cfg(not(target_arch = "wasm32"))]
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        } => Some(ResolutionError::Denied),
        Error::Invalid(_) => Some(ResolutionError::OutsideBoundary),
        _ => None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn filesystem_error(error: &acyclic_fs::FsError) -> Option<ResolutionError> {
    match error {
        acyclic_fs::FsError::NotFound => Some(ResolutionError::Read),
        acyclic_fs::FsError::NotDirectory
        | acyclic_fs::FsError::Path(acyclic_fs::kernel::PathLookupError::NotDirectory) => {
            Some(ResolutionError::OutsideBoundary)
        }
        _ => None,
    }
}

async fn excerpt(
    reader: &dyn ContentResidencyVerifier,
    volume: &VolumeRef,
    prefix: &str,
    target: &TargetRef<'_>,
    maximum: u64,
    view: &mut Option<TaskVolumeSnapshot>,
    io: &PreparationIo,
) -> Result<(Resolution, Option<Range<u64>>)> {
    let mut end = maximum.min(4096);
    #[cfg(test)]
    if READ_WHOLE_FILE.get() {
        end = maximum;
    }
    loop {
        let expected = view.as_ref().map(|view| &view.generation);
        let read = match reader.inspect_private_prefix(
            volume,
            prefix,
            target.path,
            expected,
            end,
            Some(io),
        ) {
            Ok(read) => read.await,
            Err(error) => Err(error),
        };
        let (generation, length, bytes, _hold) = match read {
            Ok(read) => read,
            Err(error) => match file_error(&error) {
                Some(error) => return Ok((Resolution::Error(error), None)),
                None => return Err(error),
            },
        };
        end = end.min(length);
        if bytes.len() as u64 != end
            || generation.as_resource().provider() != volume.provider()
            || expected.is_some_and(|expected| expected != &generation)
        {
            return Err(io.invalid("citation reader returned another prefix or generation"));
        }
        if view.is_none() {
            *view = Some(TaskVolumeSnapshot {
                volume: crate::executor::with_history_io(Some(io), |work| volume.copy(work))?,
                generation: generation.clone_with_io(io)?,
            });
        }
        let (text, invalid) = match std::str::from_utf8(&bytes) {
            Ok(text) => (text, false),
            Err(error) => (
                std::str::from_utf8(bytes.get(..error.valid_up_to()).unwrap_or_default())
                    .map_err(|_| io.invalid("UTF-8 prefix changed"))?,
                error.error_len().is_some() || end == length,
            ),
        };
        let complete = end == length
            || if let Some(lines) = &target.lines {
                text.bytes().filter(|&byte| byte == b'\n').count() >= lines.end
            } else if let Some(chars) = &target.chars {
                text.chars().count() >= chars.end
            } else {
                false
            };
        if invalid && end == length {
            return Ok((Resolution::Error(ResolutionError::Read), Some(0..end)));
        }
        if complete {
            let resolution = match target.selected(text) {
                Ok(selected) => Resolution::Content(Excerpt {
                    content: copy(io, selected)?,
                    lines: target.lines.clone(),
                }),
                Err(error) => Resolution::Error(error),
            };
            return Ok((resolution, Some(0..end)));
        }
        if invalid {
            return Ok((Resolution::Error(ResolutionError::Read), Some(0..end)));
        }
        if end == maximum {
            return Ok((Resolution::Error(ResolutionError::TooLarge), Some(0..end)));
        }
        end = end.saturating_mul(2).max(1).min(maximum).min(length);
    }
}
