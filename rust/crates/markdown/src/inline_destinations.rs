use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default)]
enum Phase {
    #[default]
    Open,
    Leading(bool),
    Bare(usize),
    Angle,
    AfterDestination(bool),
    Title(u8),
    AfterTitle(bool),
    Complete(usize),
    Invalid,
}

#[derive(Default)]
pub(super) struct Suffix {
    phase: Phase,
    offset: usize,
    escaped: bool,
    carriage_return: bool,
    #[cfg(test)]
    replay: bool,
    #[cfg(test)]
    forget_escape: bool,
}

impl Suffix {
    pub(super) fn push(&mut self, source: &str) -> Option<usize> {
        #[cfg(test)]
        if self.replay {
            self.phase = Phase::default();
            self.offset = 0;
            self.escaped = false;
            self.carriage_return = false;
        }
        for &byte in source.as_bytes().get(self.offset..)? {
            if matches!(self.phase, Phase::Complete(_) | Phase::Invalid) {
                break;
            }
            #[cfg(test)]
            crate::work::scan(1);
            self.offset += 1;
            if self.carriage_return {
                self.carriage_return = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if self.escaped {
                self.escaped = false;
                if byte.is_ascii_punctuation() {
                    continue;
                }
            }
            let escapes = matches!(self.phase, Phase::Bare(_) | Phase::Angle | Phase::Title(_));
            #[cfg(test)]
            let escapes = escapes && !self.forget_escape;
            if byte == b'\\' && escapes {
                self.escaped = true;
                continue;
            }
            self.phase = match self.phase {
                Phase::Open if byte == b'(' => Phase::Leading(false),
                Phase::Leading(newline) => {
                    if let Some(next) = self.separator(byte, newline, true) {
                        next
                    } else {
                        match byte {
                            b'<' => Phase::Angle,
                            b')' => Phase::Complete(self.offset),
                            b'(' => Phase::Bare(1),
                            b'\\' => {
                                #[cfg(test)]
                                let escape = !self.forget_escape;
                                #[cfg(not(test))]
                                let escape = true;
                                self.escaped = escape;
                                Phase::Bare(0)
                            }
                            0..=0x20 => Phase::Invalid,
                            _ => Phase::Bare(0),
                        }
                    }
                }
                Phase::Bare(depth) => match byte {
                    b'(' if depth <= 32 => Phase::Bare(depth + 1),
                    b'(' => Phase::Invalid,
                    b')' if depth == 0 => Phase::Complete(self.offset),
                    b')' => Phase::Bare(depth - 1),
                    0..=0x20 if depth == 0 => {
                        self.separator(byte, false, false).unwrap_or(Phase::Invalid)
                    }
                    0..=0x20 => Phase::Invalid,
                    _ => Phase::Bare(depth),
                },
                Phase::Angle => match byte {
                    b'>' => Phase::AfterDestination(false),
                    b'<' | b'\n' | b'\r' => Phase::Invalid,
                    _ => Phase::Angle,
                },
                Phase::AfterDestination(newline) => {
                    if let Some(next) = self.separator(byte, newline, false) {
                        next
                    } else {
                        match byte {
                            b')' => Phase::Complete(self.offset),
                            b'\'' | b'"' | b'(' => Phase::Title(byte),
                            _ => Phase::Invalid,
                        }
                    }
                }
                Phase::Title(open) => {
                    let close = if open == b'(' { b')' } else { open };
                    if byte == close {
                        Phase::AfterTitle(false)
                    } else if byte == open {
                        Phase::Invalid
                    } else {
                        Phase::Title(open)
                    }
                }
                Phase::AfterTitle(newline) => {
                    if let Some(next) = self.separator(byte, newline, false) {
                        match next {
                            Phase::AfterDestination(newline) => Phase::AfterTitle(newline),
                            _ => next,
                        }
                    } else if byte == b')' {
                        Phase::Complete(self.offset)
                    } else {
                        Phase::Invalid
                    }
                }
                _ => Phase::Invalid,
            };
        }
        match self.phase {
            Phase::Complete(end) => Some(end),
            _ => None,
        }
    }

    fn separator(&mut self, byte: u8, newline: bool, leading: bool) -> Option<Phase> {
        let next = |newline| {
            if leading {
                Phase::Leading(newline)
            } else {
                Phase::AfterDestination(newline)
            }
        };
        match byte {
            b' ' | b'\t' | 0x0b | 0x0c => Some(next(newline)),
            b'\n' | b'\r' if !newline => {
                self.carriage_return = byte == b'\r';
                Some(next(true))
            }
            b'\n' | b'\r' => Some(Phase::Invalid),
            _ => None,
        }
    }

    pub(super) fn target(&self, source: &str) -> Option<Arc<str>> {
        let Phase::Complete(end) = self.phase else {
            return None;
        };
        let source = source.get(..end)?;
        #[cfg(test)]
        crate::work::copy(3 + source.len());
        let probe = format!("[x]{source}");
        let (events, _) =
            crate::events::scoped(&probe, &crate::References::default(), crate::options(), 0);
        events.into_iter().find_map(|(event, range)| {
            if range == (0..probe.len())
                && let crate::Event::Start(crate::Tag::Link { dest_url, .. }) = event
            {
                #[cfg(test)]
                crate::work::copy(dest_url.len());
                Some(Arc::from(dest_url.as_ref()))
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
#[path = "../tests/inline_destinations.rs"]
mod tests;
