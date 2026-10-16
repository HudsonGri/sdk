use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Text,
    Escape(char),
    Run { byte: u8, before: char, after: char },
    Open { image: bool },
    Close,
    Entity,
    AngleOpen,
    AngleClose,
    ParenOpen,
    ParenClose,
    Quote(char),
    Break,
}

impl Kind {
    // Preserve the parser's first-pass expansion state across source checkpoints.
    // A preceding expansion also processes a terminal escaped hard break.
    pub(super) fn expands(self, range: Range<usize>) -> bool {
        match self {
            Self::Open { .. }
            | Self::Close
            | Self::AngleOpen
            | Self::Escape('`')
            | Self::Run { byte: b'`', .. } => true,
            Self::Run {
                byte,
                before,
                after,
            } => {
                let run = super::delimiters::Run::new(range, byte, before, after);
                (byte != b'~' || run.range.len() <= 2) && (run.open || run.close)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Token {
    pub(super) range: Range<usize>,
    pub(super) kind: Kind,
}

#[derive(Default)]
enum Pending {
    #[default]
    None,
    Escape(usize),
    Bang(usize),
    Entity(usize),
    Run {
        start: usize,
        byte: u8,
        before: char,
        escaped: bool,
    },
}

#[derive(Default)]
pub(super) struct Tokens {
    pub(super) ready: VecDeque<Token>,
    pub(super) ends: HashMap<usize, usize>,
    emitted: usize,
    pub(super) code_ends: VecDeque<usize>,
    offset: usize,
    previous: Option<char>,
    pending: Pending,
}

impl Tokens {
    pub(super) fn at(offset: usize, previous: Option<char>) -> Self {
        Self {
            offset,
            previous,
            ..Self::default()
        }
    }

    pub(super) fn discard_before(&mut self, offset: usize) {
        while self
            .ready
            .front()
            .is_some_and(|token| token.range.end <= offset)
        {
            self.ready.pop_front();
        }
        while self.code_ends.front().is_some_and(|end| *end <= offset) {
            self.code_ends.pop_front();
        }
        if let Some(token) = self.ready.front_mut() {
            token.range.start = token.range.start.max(offset);
        }
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.ready.capacity() * std::mem::size_of::<Token>()
            + self.ends.capacity() * std::mem::size_of::<(usize, usize)>()
            + self.code_ends.capacity() * std::mem::size_of::<usize>()
    }

    pub(super) fn push(&mut self, source: &str) {
        let base = self.offset;
        for (relative, character) in source
            .get(base..)
            .expect("append-only UTF-8 source")
            .char_indices()
        {
            let position = base + relative;
            #[cfg(test)]
            crate::work::scan(character.len_utf8());
            self.character(position, character);
            self.offset = position + character.len_utf8();
            self.previous = Some(character);
        }
    }

    // EOF is a preview, never a state transition: a following chunk can still
    // extend this run or supply the escaped character/entity terminator.
    pub(super) fn preview(&self) -> Option<Token> {
        let (start, kind) = match self.pending {
            Pending::None => return None,
            Pending::Run {
                start,
                byte,
                before,
                escaped,
            } => (
                start,
                if escaped {
                    Kind::Escape(char::from(byte))
                } else {
                    Kind::Run {
                        byte,
                        before,
                        after: ' ',
                    }
                },
            ),
            Pending::Escape(start) | Pending::Bang(start) | Pending::Entity(start) => {
                (start, Kind::Text)
            }
        };
        Some(Token {
            range: start..self.offset,
            kind,
        })
    }

    pub(super) fn collapsed_end(&self, end: usize, discarded: usize) -> Option<usize> {
        #[cfg(test)]
        crate::work::scan(2);
        let after = self.ends.get(&end)?.checked_sub(discarded)?;
        let opening = self.ready.get(after)?;
        let closing = self.ready.get(after + 1)?;
        (opening.range.start == end
            && matches!(
                opening.kind,
                Kind::Open { image: false } | Kind::Escape('[')
            )
            && closing.kind == Kind::Close
            && opening.range.end == closing.range.start)
            .then_some(closing.range.end)
    }

    pub(super) fn neutral(&self) -> bool {
        matches!(self.pending, Pending::None)
    }

    fn character(&mut self, position: usize, character: char) {
        let end = position + character.len_utf8();
        match std::mem::take(&mut self.pending) {
            Pending::Escape(start) => {
                // Inside code the backslash is literal, so the physical run
                // can still be a closing delimiter extended by another chunk.
                if matches!(character, '*' | '_' | '~' | '`') {
                    self.pending = Pending::Run {
                        start,
                        byte: character as u8,
                        before: '\\',
                        escaped: true,
                    };
                    return;
                }
                if character.is_ascii_punctuation() || character == '\n' {
                    self.emit(start..end, Kind::Escape(character));
                    return;
                }
                self.emit(start..position, Kind::Text);
            }
            Pending::Bang(start) => {
                if character == '[' {
                    self.emit(start..end, Kind::Open { image: true });
                    return;
                }
                self.emit(start..position, Kind::Text);
            }
            Pending::Run {
                start,
                byte,
                before,
                escaped,
            } => {
                if character == char::from(byte) {
                    self.pending = Pending::Run {
                        start,
                        byte,
                        before,
                        escaped,
                    };
                    return;
                }
                let end = if escaped && byte == b'`' {
                    // Code treats the whole physical run as a possible closer,
                    // including its escaped first tick. Keep that unit intact.
                    self.emit(start..position, Kind::Escape('`'));
                    position
                } else if escaped {
                    self.emit(start..start + 2, Kind::Escape(char::from(byte)));
                    start + 2
                } else {
                    start
                };
                if end < position {
                    self.emit(
                        end..position,
                        Kind::Run {
                            byte,
                            before: if escaped { char::from(byte) } else { before },
                            after: character,
                        },
                    );
                }
            }
            Pending::Entity(start) => {
                if character == ';' {
                    self.emit(start..end, Kind::Entity);
                    return;
                }
                if character.is_ascii_alphanumeric() || character == '#' {
                    self.pending = Pending::Entity(start);
                    return;
                }
                self.emit(start..position, Kind::Text);
            }
            Pending::None => {}
        }
        self.begin(position, character);
    }

    fn begin(&mut self, position: usize, character: char) {
        let kind = match character {
            '\\' => {
                self.pending = Pending::Escape(position);
                return;
            }
            '!' => {
                self.pending = Pending::Bang(position);
                return;
            }
            '&' => {
                self.pending = Pending::Entity(position);
                return;
            }
            '*' | '_' | '~' | '`' => {
                self.pending = Pending::Run {
                    start: position,
                    byte: character as u8,
                    before: self.previous.unwrap_or(' '),
                    escaped: false,
                };
                return;
            }
            '[' => Kind::Open { image: false },
            ']' => Kind::Close,
            '<' => Kind::AngleOpen,
            '>' => Kind::AngleClose,
            '(' => Kind::ParenOpen,
            ')' => Kind::ParenClose,
            '\'' | '"' => Kind::Quote(character),
            '\n' => Kind::Break,
            _ => Kind::Text,
        };
        self.emit(position..position + character.len_utf8(), kind);
    }

    fn record_end(&mut self, end: usize, ordinal: usize) {
        #[cfg(test)]
        work_end(self.ends.len(), self.ends.capacity());
        self.ends.insert(end, ordinal);
    }

    fn emit(&mut self, range: Range<usize>, kind: Kind) {
        if matches!(kind, Kind::Run { byte: b'`', .. })
            || kind == Kind::Escape('`') && range.len() > 2
        {
            #[cfg(test)]
            crate::work::copy(
                std::mem::size_of::<usize>()
                    * (1 + if self.code_ends.len() == self.code_ends.capacity() {
                        self.code_ends.len()
                    } else {
                        0
                    }),
            );
            self.code_ends.push_back(range.end);
        }
        if kind == Kind::Text
            && let Some(last) = self.ready.back_mut()
            && last.kind == Kind::Text
            && last.range.end == range.start
        {
            last.range.end = range.end;
            return;
        }
        #[cfg(test)]
        crate::work::copy(
            std::mem::size_of::<Token>()
                * (1 + if self.ready.len() == self.ready.capacity() {
                    self.ready.len()
                } else {
                    0
                }),
        );
        self.emitted += 1;
        if matches!(kind, Kind::Close | Kind::AngleClose | Kind::ParenClose) {
            self.record_end(range.end, self.emitted);
        }
        self.ready.push_back(Token { range, kind });
    }
}

#[cfg(test)]
fn work_end(length: usize, capacity: usize) {
    crate::work::copy(
        std::mem::size_of::<(usize, usize)>() * (1 + if length == capacity { length } else { 0 }),
    );
}

#[cfg(test)]
#[path = "../tests/inline_tokens.rs"]
mod tests;
