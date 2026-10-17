//! File citations parsed independently of filesystem access.

use std::ops::Range;

/// Citation presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// A path chip: `:file[path]`.
    Inline,
    /// A code excerpt: `::file[path]{attributes}`.
    Block,
}

/// Inclusive, one-based source lines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lines {
    /// First included line.
    pub start: usize,
    /// Last included line.
    pub end: usize,
}

/// Valid syntax. Paths are logical task-view paths, never OS paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Target {
    /// Path as authored, with surrounding whitespace removed.
    pub path: String,
    /// Optional inclusive line selection.
    pub lines: Option<Lines>,
    /// Optional zero-based, end-exclusive character selection.
    pub chars: Option<Range<usize>>,
    /// Requested symbol; resolution may report unsupported.
    pub symbol: Option<String>,
}

/// Invalid syntax remains ordinary literal text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Directive {
    raw: String,
    kind: Kind,
    target: Option<Target>,
}

impl Directive {
    /// Parse one complete citation, preserving its exact serialization.
    pub fn parse(raw: &str, kind: Kind) -> Self {
        Self {
            raw: raw.to_owned(),
            kind,
            target: parse(raw, kind),
        }
    }

    /// Exact authored bytes, including invalid syntax and unknown attributes.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Inline chip or block excerpt.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Validated syntax, or `None` for literal invalid text.
    pub fn target(&self) -> Option<&Target> {
        self.target.as_ref()
    }
}

fn integer(raw: &str) -> Option<usize> {
    (!raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| raw.parse().ok())
        .flatten()
}

fn lines(raw: &str, shorthand: bool) -> Option<Lines> {
    let raw = if shorthand {
        raw.strip_prefix('L')?
    } else {
        raw
    };
    let (start, end) = raw.split_once('-').map_or((raw, raw), |(a, b)| {
        (
            a,
            if shorthand {
                b.strip_prefix('L').unwrap_or(b)
            } else {
                b
            },
        )
    });
    let (start, end) = (integer(start)?, integer(end)?);
    (start > 0 && end >= start).then_some(Lines { start, end })
}

fn parse(raw: &str, kind: Kind) -> Option<Target> {
    let prefix = match kind {
        Kind::Inline => ":file[",
        Kind::Block => "::file[",
    };
    let (body, suffix) = raw.strip_prefix(prefix)?.split_once(']')?;
    if body.contains(['[', '\n', '\r']) {
        return None;
    }
    let (path, selection) = body
        .split_once('#')
        .map_or((body, None), |(a, b)| (a, Some(b)));
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    let mut target = Target {
        path: path.to_owned(),
        lines: match selection {
            Some(raw) => Some(lines(raw, true)?),
            None => None,
        },
        chars: None,
        symbol: None,
    };
    if suffix.is_empty() {
        return Some(target);
    }
    if kind != Kind::Block {
        return None;
    }
    let attrs = suffix.strip_prefix('{')?.strip_suffix('}')?;
    if attrs.contains(['{', '}']) {
        return None;
    }
    for attr in attrs
        .split(|c: char| c == ',' || c.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
    {
        let Some((key, value)) = attr.split_once('=') else {
            continue;
        };
        match key {
            "lines" => target.lines = Some(lines(value, false)?),
            "chars" => {
                let (start, end) = value.split_once(':')?;
                let (start, end) = (integer(start)?, integer(end)?);
                if end < start {
                    return None;
                }
                target.chars = Some(start..end);
            }
            "symbol" => target.symbol = (!value.is_empty()).then(|| value.to_owned()),
            _ => {}
        }
    }
    Some(target)
}

/// A completed citation's position in the original text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Located {
    /// Original UTF-8 byte range.
    pub range: Range<usize>,
    /// Parsed citation or literal invalid syntax.
    pub directive: Directive,
}

#[derive(Clone, Debug)]
enum Pending {
    Prefix {
        start: usize,
        matched: usize,
        block: bool,
    },
    Body {
        start: usize,
        kind: Kind,
    },
    Closed {
        start: usize,
    },
    Attributes {
        start: usize,
    },
}

/// Append-only syntax recognition. Pending bytes are visited once as they arrive;
/// completed citations are parsed once, and ordinary text is left untouched.
#[derive(Clone, Debug, Default)]
pub struct Stream {
    source: String,
    nodes: Vec<Located>,
    pending: Option<Pending>,
    escaped: bool,
    #[cfg(test)]
    work: usize,
}

impl Stream {
    /// Append arbitrary UTF-8 chunks. A block's optional attributes remain
    /// provisional after `]` until the next byte or the end of the message.
    pub fn push(&mut self, chunk: &str) {
        let offset = self.source.len();
        self.source.push_str(chunk);
        for (relative, byte) in chunk.bytes().enumerate() {
            let position = offset + relative;
            #[cfg(test)]
            {
                self.work += 1;
            }
            let pending = self.pending.take();
            match pending {
                Some(Pending::Prefix {
                    start,
                    matched,
                    block,
                }) => {
                    if matched == 1 && !block && byte == b':' {
                        self.pending = Some(Pending::Prefix {
                            start,
                            matched,
                            block: true,
                        });
                    } else if b"file[".get(matched - 1) == Some(&byte) {
                        self.pending = Some(if matched == 5 {
                            Pending::Body {
                                start,
                                kind: if block { Kind::Block } else { Kind::Inline },
                            }
                        } else {
                            Pending::Prefix {
                                start,
                                matched: matched + 1,
                                block,
                            }
                        });
                    } else {
                        self.start(position, byte);
                    }
                }
                Some(Pending::Body { start, kind }) => {
                    if byte == b']' {
                        if kind == Kind::Block {
                            self.pending = Some(Pending::Closed { start });
                        } else {
                            self.complete(start, position + 1, kind);
                        }
                    } else {
                        self.pending = Some(Pending::Body { start, kind });
                    }
                }
                Some(Pending::Closed { start }) => {
                    if byte == b'{' {
                        self.pending = Some(Pending::Attributes { start });
                    } else {
                        self.complete(start, position, Kind::Block);
                        self.start(position, byte);
                    }
                }
                Some(Pending::Attributes { start }) => {
                    if byte == b'}' {
                        self.complete(start, position + 1, Kind::Block);
                    } else {
                        self.pending = Some(Pending::Attributes { start });
                    }
                }
                None => self.start(position, byte),
            }
        }
    }

    fn start(&mut self, position: usize, byte: u8) {
        if self.escaped {
            self.escaped = false;
        } else if byte == b'\\' {
            self.escaped = true;
        } else if byte == b':' {
            self.pending = Some(Pending::Prefix {
                start: position,
                matched: 1,
                block: false,
            });
        }
    }

    fn complete(&mut self, start: usize, end: usize, kind: Kind) {
        let raw = &self.source[start..end];
        #[cfg(test)]
        {
            self.work += raw.len();
        }
        self.nodes.push(Located {
            range: start..end,
            directive: Directive::parse(raw, kind),
        });
    }

    /// Completed nodes; any unfinished citation remains literal provisional text.
    pub fn nodes(&self) -> &[Located] {
        &self.nodes
    }

    /// Original bytes, including provisional and invalid syntax.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Finalize the message. An optional attribute list can no longer follow `]`.
    /// Incomplete prefixes, brackets and attributes remain ordinary raw text.
    pub fn finish(&mut self) {
        if let Some(Pending::Closed { start }) = self.pending.take() {
            self.complete(start, self.source.len(), Kind::Block);
        }
    }
}

#[cfg(test)]
#[path = "../tests/directives.rs"]
mod tests;
