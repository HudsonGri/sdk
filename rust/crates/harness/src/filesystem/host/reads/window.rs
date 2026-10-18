//! Streaming resolution of a line-window read. One pass over bounded chunks
//! hashes and classifies the whole file, so the returned reference names the
//! exact version, while only the requested lines are retained.
use super::*;
use crate::conversation::LineWindow;
use crate::store::history::HistoryIo as _;
use sha2::{Digest as _, Sha256};

/// Bytes read per range request; the only transient buffer besides the window.
const CHUNK_BYTES: u64 = 64 * 1024;

/// Identity of the whole streamed file.
pub(super) struct Scanned {
    pub(super) sha256: [u8; 32],
    pub(super) length: u64,
    pub(super) media_type: &'static str,
}

/// Reads `path` from `generation` in chunks, retaining only `window`'s lines.
pub(super) async fn scan<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    maximum_bytes: u64,
    window: LineWindow,
    io: &PreparationIo,
) -> Result<(acyclic_stream::OwnedBytes, Scanned)> {
    io.remaining()?;
    let stat = io.sdk(generation.stat(path, io, io.cancellation()).await)?;
    if stat.kind != FileKind::Regular {
        return io.sdk((
            Err::<(acyclic_stream::OwnedBytes, Scanned), _>(
                acyclic_fs::WorkspaceError::NotRegularFile,
            ),
            Ok(()),
        ));
    }
    let length = stat.logical_bytes.ok_or_else(|| {
        stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Storage,
            "file length is missing",
        )
    })?;
    if length > maximum_bytes {
        return io.sdk((
            Err::<(acyclic_stream::OwnedBytes, Scanned), _>(
                acyclic_fs::WorkspaceError::ReadLimitExceeded,
            ),
            Ok(()),
        ));
    }
    let mut lines = LineScan::new(window);
    let mut sha = Sha256::new();
    let mut offset = 0;
    while offset < length {
        let size = CHUNK_BYTES.min(length - offset);
        io.remaining()?;
        let chunk = io.sdk(
            generation
                .read_range(path, offset, size, io, io.cancellation())
                .await,
        )?;
        let bytes = chunk.bytes.as_slice();
        if bytes.len() as u64 != size {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "file range is shorter than its length",
            ));
        }
        let mut observer = io;
        observer.check_cancellation()?;
        observer.hashed_bytes(size)?;
        sha.update(bytes);
        lines.update(bytes);
        offset += size;
    }
    let invalid = |reason| stock_boundary(Some(io), crate::wire::ErrorCode::Invalid, reason);
    let (selected, media_type) = lines.finish();
    let Some(media_type) = media_type else {
        return Err(invalid("file read requires UTF-8 content"));
    };
    let selected = selected.ok_or_else(|| invalid("read offset is past the end of the file"))?;
    io.retain_owned(selected.capacity() as u64)?;
    Ok((
        selected.into(),
        Scanned {
            sha256: sha.finalize().into(),
            length,
            media_type,
        },
    ))
}

/// Pure streaming line selection and classification, chunk by chunk, equal
/// to selecting the terminator-inclusive line window of the whole text.
pub(super) struct LineScan {
    offset: u64,
    end: Option<u64>,
    /// Lines started so far; a line starts at byte 0 and after each `\n`
    /// that is followed by another byte.
    started: u64,
    at_line_start: bool,
    retaining: bool,
    done: bool,
    retained: Vec<u8>,
    length: u64,
    zero: bool,
    utf8: bool,
    carry: [u8; 4],
    carried: usize,
}

impl LineScan {
    pub(super) fn new(window: LineWindow) -> Self {
        let valid = window.offset != 0 && window.limit != Some(0);
        Self {
            offset: window.offset,
            end: window
                .limit
                .and_then(|limit| window.offset.checked_add(limit)),
            started: 0,
            at_line_start: true,
            retaining: false,
            done: !valid,
            retained: Vec::new(),
            length: 0,
            zero: false,
            utf8: true,
            carry: [0; 4],
            carried: 0,
        }
    }

    pub(super) fn update(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        self.length += chunk.len() as u64;
        self.zero |= chunk.contains(&0);
        self.validate(chunk);
        if self.done {
            return;
        }
        let mut segment = self.retaining.then_some(0);
        let starts = self.at_line_start.then_some(0).into_iter().chain(
            chunk
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == b'\n')
                .map(|(index, _)| index + 1)
                .filter(|start| *start < chunk.len()),
        );
        for start in starts {
            self.started += 1;
            if self.started == self.offset {
                self.retaining = true;
                segment = Some(start);
            }
            if Some(self.started) == self.end {
                if let Some(from) = segment.take() {
                    self.retained.extend_from_slice(&chunk[from..start]);
                }
                self.retaining = false;
                self.done = true;
                break;
            }
        }
        if let Some(from) = segment {
            self.retained.extend_from_slice(&chunk[from..]);
        }
        self.at_line_start = chunk.last() == Some(&b'\n');
    }

    /// Incremental UTF-8 validation that tolerates a character split
    /// across chunk boundaries.
    fn validate(&mut self, mut chunk: &[u8]) {
        if !self.utf8 {
            return;
        }
        while self.carried > 0 {
            let Some((&byte, rest)) = chunk.split_first() else {
                return;
            };
            self.carry[self.carried] = byte;
            self.carried += 1;
            chunk = rest;
            match std::str::from_utf8(&self.carry[..self.carried]) {
                Ok(_) => self.carried = 0,
                Err(error) if error.error_len().is_some() || self.carried == 4 => {
                    self.utf8 = false;
                    return;
                }
                Err(_) => {}
            }
        }
        if let Err(error) = std::str::from_utf8(chunk) {
            let rest = &chunk[error.valid_up_to()..];
            if error.error_len().is_some() {
                self.utf8 = false;
            } else {
                self.carry[..rest.len()].copy_from_slice(rest);
                self.carried = rest.len();
            }
        }
    }

    /// The selected lines (`None` when the offset names no line) and the
    /// whole file's media type (`None` when it is not UTF-8).
    pub(super) fn finish(self) -> (Option<Vec<u8>>, Option<&'static str>) {
        let utf8 = self.utf8 && self.carried == 0;
        let media = utf8.then_some(if self.zero {
            "application/octet-stream"
        } else {
            "text/plain"
        });
        let selected = if self.offset == 0 || self.end == Some(self.offset) {
            None
        } else if self.length == 0 {
            (self.offset == 1).then_some(Vec::new())
        } else {
            (self.started >= self.offset).then_some(self.retained)
        };
        (selected, media)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Specification: the requested window of terminator-inclusive lines;
    /// an empty file has only line 1, and offsets past the end select nothing.
    fn reference(source: &str, offset: u64, limit: Option<u64>) -> Option<String> {
        let lines: Vec<&str> = source.split_inclusive('\n').collect();
        if offset == 0 || limit == Some(0) {
            None
        } else if lines.is_empty() {
            (offset == 1).then(String::new)
        } else if offset as usize > lines.len() {
            None
        } else {
            let start = offset as usize - 1;
            let end = limit.map_or(lines.len(), |limit| {
                (start + limit as usize).min(lines.len())
            });
            Some(lines[start..end].concat())
        }
    }

    fn scan_chunks(bytes: &[u8], cuts: &[usize], window: LineWindow) -> LineScan {
        let mut scan = LineScan::new(window);
        let mut from = 0;
        for cut in cuts.iter().map(|cut| cut % (bytes.len() + 1)) {
            if cut > from {
                scan.update(&bytes[from..cut]);
                from = cut;
            }
        }
        scan.update(&bytes[from..]);
        scan
    }

    proptest! {
        #[test]
        fn chunked_selection_matches_whole_text_selection(
            text in "[ab\n\u{e9}\u{1F600}\0]{0,40}",
            offset in 0u64..12,
            limit in proptest::option::of(0u64..6),
            cuts in proptest::collection::vec(0usize..200, 0..6),
        ) {
            let mut cuts = cuts;
            cuts.sort_unstable();
            let window = LineWindow { offset, limit };
            let (selected, media) = scan_chunks(text.as_bytes(), &cuts, window).finish();
            let expected = reference(&text, offset, limit);
            prop_assert_eq!(selected.as_deref(), expected.as_deref().map(str::as_bytes));
            let expected = if text.contains('\0') {
                "application/octet-stream"
            } else {
                "text/plain"
            };
            prop_assert_eq!(media, Some(expected));
        }

        #[test]
        fn chunked_validation_matches_whole_validation(
            bytes in proptest::collection::vec(any::<u8>(), 0..24),
            cuts in proptest::collection::vec(0usize..40, 0..6),
        ) {
            let mut cuts = cuts;
            cuts.sort_unstable();
            let window = LineWindow { offset: 1, limit: None };
            let (_, media) = scan_chunks(&bytes, &cuts, window).finish();
            prop_assert_eq!(media.is_some(), std::str::from_utf8(&bytes).is_ok());
        }
    }

    #[test]
    fn a_window_retains_only_its_lines() {
        let text = "one\ntwo\nthree\n".repeat(1000);
        let window = LineWindow {
            offset: 2,
            limit: Some(1),
        };
        let scan = scan_chunks(text.as_bytes(), &[5, 4096, 9000], window);
        assert_eq!(scan.finish().0.as_deref(), Some(&b"two\n"[..]));
        // Control: an offset past the last line selects nothing.
        let window = LineWindow {
            offset: 3001,
            limit: None,
        };
        assert_eq!(scan_chunks(text.as_bytes(), &[], window).finish().0, None);
    }
}
