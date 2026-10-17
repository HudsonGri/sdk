// Incremental recognizer for constructs opened by `<`: raw HTML (tags,
// closing tags, comments, processing instructions, declarations, CDATA),
// autolinks and pointy link destinations.
//
// An unclosed `<` is literal text, so it can change earlier output only when
// a later byte completes a construct that starts at it. Each construct ends at
// the first byte that can close it, so every opener closes at most once. The
// recognizer is a nondeterministic automaton over a fixed set of states.
// Openers sharing a state have the same future, so each state keeps only its
// earliest opener: per-byte work is bounded by the state count, and a closing
// byte reports the earliest opener whose construct it may complete.
//
// The automaton over-approximates the grammar. Reporting a close that the
// parser rejects only reclassifies a suffix whose opener it then consumes;
// missing a close would leave stale output, so a state is dropped only when
// no construct from its opener can still close.

const LT: usize = 0;
const TAG_NAME: usize = 1;
const TAG_SPACE: usize = 2;
const ATTRIBUTE: usize = 3;
const AFTER_ATTRIBUTE: usize = 4;
const BEFORE_VALUE: usize = 5;
const UNQUOTED: usize = 6;
const DOUBLE: usize = 7;
const SINGLE: usize = 8;
const AFTER_VALUE: usize = 9;
const SELF_CLOSE: usize = 10;
const CLOSE_START: usize = 11;
const CLOSE_NAME: usize = 12;
const CLOSE_SPACE: usize = 13;
const BANG: usize = 14;
const BANG_DASH: usize = 15;
const COMMENT_START: usize = 16;
const COMMENT: usize = 17;
const COMMENT_DASH: usize = 18;
const COMMENT_DASHES: usize = 19;
// `<![` followed by the remaining bytes of `CDATA[`.
const CDATA_PREFIX: usize = 20;
const CDATA: usize = 26;
const CDATA_BRACKET: usize = 27;
const CDATA_BRACKETS: usize = 28;
const DECLARATION: usize = 29;
const INSTRUCTION: usize = 30;
const INSTRUCTION_MARK: usize = 31;
const SCHEME: usize = 32;
const URI: usize = 33;
const EMAIL_LOCAL: usize = 34;
const EMAIL_DOMAIN: usize = 35;
const POINTY: usize = 36;
const POINTY_ESCAPE: usize = 37;
const STATES: usize = 38;

#[cfg(test)]
thread_local! {
    // Negative control: treat `>` inside a double-quoted value as closing.
    static QUOTES_CLOSE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) struct Angles {
    // Earliest opener per live state; the two sides alternate per byte.
    earliest: [[usize; STATES]; 2],
    side: usize,
    live: u64,
    fed: usize,
    // The previous byte is an unescaped backslash, so a `<` here is literal.
    escaped: bool,
}

impl Default for Angles {
    fn default() -> Self {
        Self {
            earliest: [[0; STATES]; 2],
            side: 0,
            live: 0,
            fed: 0,
            escaped: false,
        }
    }
}

fn space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn attribute_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':')
}

fn email_local(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b".!#$%&'*+/=?^_`{|}~-".contains(&byte)
}

// Successor states and whether the byte completes a construct.
fn step(state: usize, byte: u8) -> (u64, bool) {
    let to = |state: usize| 1u64 << state;
    let tag_end = |byte: u8, stay: u64| match byte {
        b'>' => (0, true),
        b'/' => (to(SELF_CLOSE), false),
        _ if space(byte) => (to(TAG_SPACE), false),
        _ => (stay, false),
    };
    match state {
        LT => {
            let (pointy, accept) = step(POINTY, byte);
            let opened = match byte {
                b'/' => to(CLOSE_START) | to(EMAIL_LOCAL),
                b'!' => to(BANG) | to(EMAIL_LOCAL),
                b'?' => to(INSTRUCTION) | to(EMAIL_LOCAL),
                _ if byte.is_ascii_alphabetic() => to(TAG_NAME) | to(SCHEME) | to(EMAIL_LOCAL),
                _ if email_local(byte) => to(EMAIL_LOCAL),
                _ => 0,
            };
            (pointy | opened, accept)
        }
        TAG_NAME if byte.is_ascii_alphanumeric() || byte == b'-' => (to(TAG_NAME), false),
        TAG_NAME => tag_end(byte, 0),
        TAG_SPACE | AFTER_ATTRIBUTE if attribute_start(byte) => (to(ATTRIBUTE), false),
        TAG_SPACE if space(byte) => (to(TAG_SPACE), false),
        TAG_SPACE => tag_end(byte, 0),
        AFTER_ATTRIBUTE if space(byte) => (to(AFTER_ATTRIBUTE), false),
        ATTRIBUTE | AFTER_ATTRIBUTE if byte == b'=' => (to(BEFORE_VALUE), false),
        AFTER_ATTRIBUTE => tag_end(byte, 0),
        ATTRIBUTE if byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte) => {
            (to(ATTRIBUTE), false)
        }
        ATTRIBUTE if space(byte) => (to(AFTER_ATTRIBUTE), false),
        ATTRIBUTE => tag_end(byte, 0),
        BEFORE_VALUE => match byte {
            b'"' => (to(DOUBLE), false),
            b'\'' => (to(SINGLE), false),
            b'>' | b'=' | b'<' | b'`' => (0, false),
            _ if space(byte) => (to(BEFORE_VALUE), false),
            _ => (to(UNQUOTED), false),
        },
        UNQUOTED => match byte {
            b'>' => (0, true),
            b'"' | b'\'' | b'=' | b'<' | b'`' => (0, false),
            _ if space(byte) => (to(TAG_SPACE), false),
            _ => (to(UNQUOTED), false),
        },
        #[cfg(test)]
        DOUBLE if byte == b'>' && QUOTES_CLOSE.get() => (0, true),
        DOUBLE => (to(if byte == b'"' { AFTER_VALUE } else { DOUBLE }), false),
        SINGLE => (to(if byte == b'\'' { AFTER_VALUE } else { SINGLE }), false),
        AFTER_VALUE => tag_end(byte, 0),
        SELF_CLOSE => (0, byte == b'>'),
        CLOSE_START if byte.is_ascii_alphabetic() => (to(CLOSE_NAME), false),
        CLOSE_NAME if byte.is_ascii_alphanumeric() || byte == b'-' => (to(CLOSE_NAME), false),
        CLOSE_NAME | CLOSE_SPACE if space(byte) => (to(CLOSE_SPACE), false),
        CLOSE_NAME | CLOSE_SPACE => (0, byte == b'>'),
        BANG => match byte {
            b'-' => (to(BANG_DASH), false),
            b'[' => (to(CDATA_PREFIX), false),
            _ if byte.is_ascii_alphabetic() => (to(DECLARATION), false),
            _ => (0, false),
        },
        BANG_DASH if byte == b'-' => (to(COMMENT_START), false),
        COMMENT_START => match byte {
            b'>' => (0, true),
            b'-' => (to(COMMENT_DASHES), false),
            _ => (to(COMMENT), false),
        },
        COMMENT => (to(if byte == b'-' { COMMENT_DASH } else { COMMENT }), false),
        COMMENT_DASH => (to(if byte == b'-' { COMMENT_DASHES } else { COMMENT }), false),
        COMMENT_DASHES => match byte {
            b'>' => (0, true),
            b'-' => (to(COMMENT_DASHES), false),
            _ => (to(COMMENT), false),
        },
        CDATA_PREFIX..CDATA if byte == b"CDATA["[state - CDATA_PREFIX] => {
            (to(state + 1), false)
        }
        CDATA => (to(if byte == b']' { CDATA_BRACKET } else { CDATA }), false),
        CDATA_BRACKET => (to(if byte == b']' { CDATA_BRACKETS } else { CDATA }), false),
        CDATA_BRACKETS => match byte {
            b'>' => (0, true),
            b']' => (to(CDATA_BRACKETS), false),
            _ => (to(CDATA), false),
        },
        DECLARATION => (if byte == b'>' { 0 } else { to(DECLARATION) }, byte == b'>'),
        INSTRUCTION => (to(if byte == b'?' { INSTRUCTION_MARK } else { INSTRUCTION }), false),
        INSTRUCTION_MARK => match byte {
            b'>' => (0, true),
            b'?' => (to(INSTRUCTION_MARK), false),
            _ => (to(INSTRUCTION), false),
        },
        SCHEME if byte.is_ascii_alphanumeric() || b"+.-".contains(&byte) => (to(SCHEME), false),
        SCHEME if byte == b':' => (to(URI), false),
        URI => match byte {
            b'>' => (0, true),
            b'<' => (0, false),
            _ if byte <= b' ' || byte == 0x7f => (0, false),
            _ => (to(URI), false),
        },
        EMAIL_LOCAL if email_local(byte) => (to(EMAIL_LOCAL), false),
        EMAIL_LOCAL if byte == b'@' => (to(EMAIL_DOMAIN), false),
        EMAIL_DOMAIN if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-') => {
            (to(EMAIL_DOMAIN), false)
        }
        EMAIL_DOMAIN => (0, byte == b'>'),
        POINTY => match byte {
            b'>' => (0, true),
            b'\n' | b'\r' | b'<' => (0, false),
            b'\\' => (to(POINTY_ESCAPE), false),
            _ => (to(POINTY), false),
        },
        POINTY_ESCAPE => (
            if matches!(byte, b'\n' | b'\r') {
                0
            } else {
                to(POINTY)
            },
            false,
        ),
        _ => (0, false),
    }
}

impl Angles {
    /// Recognize openers from `start`; the first feed reports constructs
    /// that the bytes already after it complete.
    pub(super) fn at(start: usize) -> Self {
        Self {
            fed: start,
            ..Self::default()
        }
    }

    /// The earliest opener that a later byte can still close.
    pub(super) fn floor(&self) -> Option<usize> {
        let current = &self.earliest[self.side];
        let mut states = self.live;
        let mut floor = None::<usize>;
        while states != 0 {
            let state = states.trailing_zeros() as usize;
            states &= states - 1;
            floor = Some(floor.map_or(current[state], |floor| floor.min(current[state])));
        }
        floor
    }

    /// Consume the appended bytes. Returns the earliest opener whose
    /// construct one of them may complete.
    pub(super) fn feed(&mut self, source: &str) -> Option<usize> {
        let mut closed = None::<usize>;
        let appended = source.as_bytes().get(self.fed..).unwrap_or_default();
        for (offset, &byte) in appended.iter().enumerate() {
            #[cfg(test)]
            crate::work::scan(1 + self.live.count_ones() as usize);
            let [first, second] = &mut self.earliest;
            let (current, next_side) = if self.side == 0 {
                (&*first, second)
            } else {
                (&*second, first)
            };
            let mut live = 0u64;
            let mut states = self.live;
            while states != 0 {
                let state = states.trailing_zeros() as usize;
                states &= states - 1;
                let opener = current[state];
                let (mut next, accept) = step(state, byte);
                if accept {
                    closed = Some(closed.map_or(opener, |closed| closed.min(opener)));
                }
                while next != 0 {
                    let target = next.trailing_zeros() as usize;
                    next &= next - 1;
                    let slot = &mut next_side[target];
                    *slot = if live & (1 << target) == 0 {
                        opener
                    } else {
                        (*slot).min(opener)
                    };
                    live |= 1 << target;
                }
            }
            if byte == b'<' && !self.escaped && live & (1 << LT) == 0 {
                next_side[LT] = self.fed + offset;
                live |= 1 << LT;
            }
            self.escaped = byte == b'\\' && !self.escaped;
            self.side ^= 1;
            self.live = live;
        }
        self.fed = source.len();
        closed
    }
}

#[cfg(test)]
#[path = "../tests/inline_angles.rs"]
mod tests;
