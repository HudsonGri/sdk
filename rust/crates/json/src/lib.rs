//! One byte, structural-item and nesting boundary for SDK JSON emission.
//!
//! Ownership and allocation ledgers remain with their caller. `admit_chunk`
//! lets those callers fund each chunk before allocating or copying it; the
//! ordinary writer/count adapters use exactly the same boundary.
use std::io::{self, Write};

/// Finite limits for a JSON document.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    /// Maximum encoded UTF-8 bytes.
    pub maximum_bytes: u64,
    /// Maximum values and map keys, including duplicate entries.
    pub maximum_items: u64,
    /// Maximum zero-based value nesting depth.
    pub maximum_depth: u32,
}

/// Fixed admission failures, independent of a serializer's own errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Admission {
    /// Encoded size exceeds the caller's limit.
    #[error("JSON exceeds admitted output")]
    Bytes,
    /// Values and keys exceed the caller's limit.
    #[error("JSON exceeds declared structural bound")]
    Items,
    /// A value exceeds the nesting limit.
    #[error("JSON nesting limit exceeded")]
    Depth,
    /// A closing container has no matching opener.
    #[error("invalid JSON nesting")]
    Nesting,
    /// Serialization left a string or container open.
    #[error("incomplete JSON document")]
    Incomplete,
    /// A structural counter cannot be represented.
    #[error("JSON count overflow")]
    Overflow,
}

/// A policy denial or a failure from the original observer.
#[derive(Debug)]
pub enum Failure<E> {
    /// The shared document boundary refused progress.
    Admission(Admission),
    /// The original work, cancellation or sink callback refused progress.
    Observer(E),
}

/// Incremental structural counts over serializer-emitted JSON bytes.
/// This is not a JSON validator; syntax belongs to the serializer/parser.
#[derive(Default)]
pub struct Lexical {
    /// Values and map keys observed so far.
    pub items: u64,
    /// Container openings observed so far.
    pub containers: u64,
    /// Array openings observed so far.
    pub arrays: usize,
    /// Current nesting depth.
    pub depth: u32,
    /// Whether the next byte belongs to a string.
    pub string: bool,
    escape: bool,
    atom: bool,
}

impl Lexical {
    /// Admit one emitted byte, returning whether it begins a value or key.
    pub fn byte(&mut self, byte: u8, bounds: Bounds) -> Result<bool, Admission> {
        if self.string {
            if self.escape {
                self.escape = false;
            } else if byte == b'\\' {
                self.escape = true;
            } else if byte == b'"' {
                self.string = false;
            }
            return Ok(false);
        }
        let depth = self.depth;
        let item = match byte {
            b'"' => {
                self.string = true;
                self.atom = false;
                true
            }
            b'[' | b'{' => {
                self.containers = self.containers.checked_add(1).ok_or(Admission::Overflow)?;
                self.arrays = self
                    .arrays
                    .checked_add(usize::from(byte == b'['))
                    .ok_or(Admission::Overflow)?;
                self.depth = self.depth.checked_add(1).ok_or(Admission::Overflow)?;
                self.atom = false;
                true
            }
            b']' | b'}' => {
                self.depth = self.depth.checked_sub(1).ok_or(Admission::Nesting)?;
                self.atom = false;
                false
            }
            b':' | b',' | b' ' | b'\n' | b'\r' | b'\t' => {
                self.atom = false;
                false
            }
            _ => {
                let first = !self.atom;
                self.atom = true;
                first
            }
        };
        if item {
            if self.items >= bounds.maximum_items {
                return Err(Admission::Items);
            }
            if depth > bounds.maximum_depth {
                return Err(Admission::Depth);
            }
            self.items += 1;
        }
        Ok(item)
    }

    /// Check the terminal structural boundary after successful serialization.
    pub fn finish(&self) -> Result<(), Admission> {
        if self.string || self.depth != 0 {
            Err(Admission::Incomplete)
        } else {
            Ok(())
        }
    }
}

/// Admit a complete serializer chunk before the caller copies or emits it.
/// `inspect` sees the lexical state immediately before each byte and whether
/// that byte begins an item, for callers maintaining their admitted index.
pub fn admit_chunk<E>(
    lexical: &mut Lexical,
    written: &mut u64,
    bytes: &[u8],
    bounds: Bounds,
    mut charge: impl FnMut(u64, u64) -> Result<(), E>,
    mut inspect: impl FnMut(u8, u32, bool, bool) -> Result<(), E>,
) -> Result<(), Failure<E>> {
    let next = written
        .checked_add(bytes.len() as u64)
        .ok_or(Failure::Admission(Admission::Overflow))?;
    if next > bounds.maximum_bytes {
        return Err(Failure::Admission(Admission::Bytes));
    }
    charge(bytes.len() as u64, 0).map_err(Failure::Observer)?;
    for byte in bytes {
        let (depth, string) = (lexical.depth, lexical.string);
        let item = lexical.byte(*byte, bounds).map_err(Failure::Admission)?;
        if item {
            charge(0, 1).map_err(Failure::Observer)?;
        }
        inspect(*byte, depth, string, item).map_err(Failure::Observer)?;
    }
    *written = next;
    Ok(())
}

/// Serializer errors remain distinct from output and admission failures.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    /// A fixed shared document boundary rejected the value.
    #[error(transparent)]
    Admission(Admission),
    /// The caller's sink failed.
    #[error(transparent)]
    Output(io::Error),
    /// The value's serializer failed independently of the output bound.
    #[error(transparent)]
    Serialization(serde_json::Error),
}

struct Writer<'a, W: ?Sized> {
    output: &'a mut W,
    lexical: Lexical,
    written: u64,
    bounds: Bounds,
    failed: Option<EncodeError>,
}
impl<W: Write + ?Sized> Write for Writer<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Returning an IO error stops recursive serializers immediately.
        // Callers with allocation custody use admit_chunk directly instead.
        let admitted = admit_chunk(
            &mut self.lexical,
            &mut self.written,
            bytes,
            self.bounds,
            |_, _| Ok::<_, std::convert::Infallible>(()),
            |_, _, _, _| Ok(()),
        );
        let error = match admitted {
            Err(Failure::Admission(error)) => Some(EncodeError::Admission(error)),
            Err(Failure::Observer(never)) => match never {},
            Ok(()) => self.output.write_all(bytes).err().map(EncodeError::Output),
        };
        if let Some(error) = error {
            self.failed.get_or_insert(error);
            return Err(io::ErrorKind::Other.into());
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

/// Stream JSON through the sole shared document boundary, without an
/// intermediate buffer. Pretty formatting uses the same admission policy.
pub fn to_writer<T: serde::Serialize + ?Sized, W: Write + ?Sized>(
    value: &T,
    output: &mut W,
    bounds: Bounds,
    pretty: bool,
) -> Result<u64, EncodeError> {
    let mut writer = Writer {
        output,
        lexical: Lexical::default(),
        written: 0,
        bounds,
        failed: None,
    };
    let result = if pretty {
        serde_json::to_writer_pretty(&mut writer, value)
    } else {
        serde_json::to_writer(&mut writer, value)
    };
    if let Some(error) = writer.failed {
        return Err(error);
    }
    result.map_err(EncodeError::Serialization)?;
    writer.lexical.finish().map_err(EncodeError::Admission)?;
    Ok(writer.written)
}

/// Count compact JSON through the same boundary as sink and owned encodings.
pub fn count<T: serde::Serialize + ?Sized>(value: &T, bounds: Bounds) -> Result<u64, EncodeError> {
    to_writer(value, &mut io::sink(), bounds, false)
}

#[cfg(test)]
mod tests;
