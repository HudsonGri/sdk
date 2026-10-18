//! Allocation-free incremental positions shared by owned and budgeted consumers.
use super::{Kind, Pending, Prefix};
use std::ops::Range;

/// Recognizes completed citation positions without retaining or copying source bytes.
#[derive(Clone, Debug, Default)]
pub struct Scanner {
    read: usize,
    pending: Option<Pending>,
    prefix: Option<Prefix>,
    pub(super) escaped: bool,
    #[cfg(test)]
    pub(super) swallow_invalid: bool,
}

impl Scanner {
    /// Visit appended bytes once. A failed sink stops recognition immediately.
    pub fn push<E>(
        &mut self,
        chunk: &str,
        emit: &mut impl FnMut(Range<usize>, Kind) -> Result<(), E>,
    ) -> Result<(), E> {
        let offset = self.read;
        self.read += chunk.len();
        for (relative, byte) in chunk.bytes().enumerate() {
            let position = offset + relative;
            let escaped = self.escaped;
            self.escaped = byte == b'\\' && !escaped;
            let opened = self.recognize(position, byte, escaped);
            let pending = self.pending.take();
            match pending {
                Some(Pending::Body { start, kind }) => {
                    let invalid = matches!(byte, b'[' | b'\n' | b'\r');
                    #[cfg(test)]
                    let invalid = invalid && !self.swallow_invalid;
                    if invalid {
                        // The source retains every abandoned byte. A nested
                        // prefix is already recognized when its '[' invalidates
                        // this body, so it becomes the next candidate directly.
                        self.pending = opened;
                    } else if byte == b']' {
                        if kind == Kind::Block {
                            self.pending = Some(Pending::Closed { start });
                        } else {
                            emit(start..position + 1, kind)?;
                        }
                    } else {
                        self.pending = Some(Pending::Body { start, kind });
                    }
                }
                Some(Pending::Closed { start }) => {
                    if byte == b'{' {
                        self.pending = Some(Pending::Attributes { start });
                    } else {
                        emit(start..position, Kind::Block)?;
                        self.pending = opened;
                    }
                }
                Some(Pending::Attributes { start }) => {
                    if byte == b'}' {
                        emit(start..position + 1, Kind::Block)?;
                    } else {
                        self.pending = Some(Pending::Attributes { start });
                    }
                }
                None => self.pending = opened,
            }
        }
        Ok(())
    }

    // Prefix recognition runs beside an active body. A second `:file[` makes
    // that body invalid, but its opening colon must not be lost on recovery.
    fn recognize(&mut self, position: usize, byte: u8, escaped: bool) -> Option<Pending> {
        if let Some(Prefix {
            start,
            matched,
            block,
        }) = self.prefix.take()
        {
            if matched == 1 && byte == b':' {
                self.prefix = Some(Prefix {
                    start: if block { position - 1 } else { start },
                    matched,
                    block: true,
                });
                return None;
            }
            if b"file[".get(matched - 1) == Some(&byte) {
                if matched == 5 {
                    return Some(Pending::Body {
                        start,
                        kind: if block { Kind::Block } else { Kind::Inline },
                    });
                }
                self.prefix = Some(Prefix {
                    start,
                    matched: matched + 1,
                    block,
                });
                return None;
            }
        }
        if byte == b':' && !escaped {
            self.prefix = Some(Prefix {
                start: position,
                matched: 1,
                block: false,
            });
        }
        None
    }

    /// Complete an attribute-free block at EOF; unfinished syntax stays literal.
    pub fn finish<E>(
        &mut self,
        emit: &mut impl FnMut(Range<usize>, Kind) -> Result<(), E>,
    ) -> Result<(), E> {
        self.prefix = None;
        if let Some(Pending::Closed { start }) = self.pending.take() {
            emit(start..self.read, Kind::Block)?;
        }
        Ok(())
    }
}
