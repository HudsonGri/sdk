use std::{collections::HashMap, sync::Arc};
#[path = "inline_bare.rs"]
mod bare;

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

    #[cfg(test)]
    fn target(&self, source: &str) -> Option<Arc<str>> {
        let Phase::Complete(end) = self.phase else {
            return None;
        };
        resolve(source, end)
    }
}

fn resolve(source: &str, end: usize) -> Option<Arc<str>> {
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

#[derive(Default)]
pub(in crate::inline_scan) struct Index {
    offset: Option<usize>,
    closing: bool,
    pending: Vec<(usize, Suffix)>,
    bare: bare::Bare,
    completed: HashMap<usize, (usize, Option<Option<Arc<str>>>)>,
    #[cfg(test)]
    replay: bool,
    #[cfg(test)]
    forget_escape: bool,
}

impl Index {
    pub(in crate::inline_scan) fn push(&mut self, source: &str, origin: usize) {
        #[cfg(test)]
        if self.replay {
            self.offset = None;
            self.closing = false;
            self.pending.clear();
            self.bare = bare::Bare::default();
            self.completed.clear();
        }
        #[cfg(test)]
        {
            self.bare.forget_escape = self.forget_escape;
        }
        let offset = *self.offset.get_or_insert(origin);
        for (relative, character) in source
            .get(offset..)
            .expect("append-only destination input")
            .char_indices()
        {
            #[cfg(test)]
            crate::work::scan(character.len_utf8());
            let end = offset + relative + character.len_utf8();
            if self.closing && character == '(' {
                #[cfg(test)]
                crate::work::copy(
                    std::mem::size_of::<(usize, Suffix)>()
                        * (1 + if self.pending.len() == self.pending.capacity() {
                            self.pending.len()
                        } else {
                            0
                        }),
                );
                let suffix = Suffix::default();
                #[cfg(test)]
                let suffix = Suffix {
                    forget_escape: self.forget_escape,
                    ..suffix
                };
                self.pending.push((end - 1, suffix));
            }
            self.closing = character == ']';
            self.advance(source, end, character);
        }
        self.offset = Some(source.len());
    }

    fn complete(&mut self, start: usize, end: usize) {
        #[cfg(test)]
        crate::work::copy(
            std::mem::size_of::<(usize, (usize, Option<Option<Arc<str>>>))>()
                * (1 + if self.completed.len() == self.completed.capacity() {
                    self.completed.len()
                } else {
                    0
                }),
        );
        self.completed.insert(start, (end, None));
    }

    fn advance(&mut self, source: &str, end: usize, character: char) {
        let (finished, control) = self.bare.advance(character);
        for start in finished {
            #[cfg(test)]
            crate::work::scan(1);
            if control {
                #[cfg(test)]
                crate::work::copy(
                    std::mem::size_of::<(usize, Suffix)>()
                        * (1 + if self.pending.len() == self.pending.capacity() {
                            self.pending.len()
                        } else {
                            0
                        }),
                );
                self.pending.push((
                    start,
                    Suffix {
                        phase: Phase::Bare(0),
                        offset: end - character.len_utf8() - start,
                        ..Suffix::default()
                    },
                ));
            } else {
                self.complete(start, end);
            }
        }
        let mut next = 0;
        while next < self.pending.len() {
            #[cfg(test)]
            crate::work::scan(1);
            let (start, suffix) = &mut self.pending[next];
            suffix.push(source.get(*start..end).expect("destination suffix"));
            match suffix.phase {
                Phase::Complete(length) => {
                    let (start, end) = (*start, *start + length);
                    self.complete(start, end);
                }
                Phase::Bare(depth) => {
                    self.bare.insert(*start, depth, suffix.escaped);
                }
                Phase::Invalid => {}
                _ => {
                    next += 1;
                    continue;
                }
            }
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<(usize, Suffix)>());
            self.pending.swap_remove(next);
        }
    }

    pub(in crate::inline_scan) fn target(
        &mut self,
        source: &str,
        start: usize,
    ) -> Option<(usize, Arc<str>)> {
        let (end, target) = self.completed.get_mut(&start)?;
        let target = target.get_or_insert_with(|| resolve(source.get(start..*end)?, *end - start));
        Some((*end, target.as_ref()?.clone()))
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn retained(
        &self,
        links: &mut std::collections::HashSet<*const u8>,
    ) -> (usize, usize) {
        let payload = self
            .completed
            .values()
            .filter_map(|(_, target)| target.as_ref()?.as_ref())
            .filter(|target| links.insert(Arc::as_ptr(target).cast::<u8>()))
            .map(|target| target.len())
            .sum();
        (
            payload,
            self.bare.retained()
                + self.pending.capacity() * std::mem::size_of::<(usize, Suffix)>()
                + self.completed.capacity()
                    * std::mem::size_of::<(usize, (usize, Option<Option<Arc<str>>>))>(),
        )
    }
}

#[cfg(test)]
#[path = "../tests/inline_destinations.rs"]
mod tests;
