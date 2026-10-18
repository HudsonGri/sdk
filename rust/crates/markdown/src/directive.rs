//! File citations parsed independently of filesystem access.

use serde::{Deserialize, Serialize};
use std::ops::Range;

mod presentation;
pub(crate) mod render;
mod scan;
pub use presentation::{Excerpt, Resolution, ResolutionError};
pub use scan::Scanner;

/// Citation presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Kind {
    /// A path chip: `:file[path]`.
    Inline,
    /// A code excerpt: `::file[path]{attributes}`.
    Block,
}

/// Inclusive, one-based source lines.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

impl Serialize for Directive {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.kind, self.raw.as_str()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Directive {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let (kind, raw) = <(Kind, String)>::deserialize(decoder)?;
        // Parsed fields are always derived from the authored bytes, including
        // invalid syntax; serialized input cannot forge a different target.
        let target = parse(&raw, kind);
        Ok(Self { raw, kind, target })
    }
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

/// Allocation-free view of validated directive fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetRef<'a> {
    /// Logical path in the task view.
    pub path: &'a str,
    /// Inclusive source lines.
    pub lines: Option<Lines>,
    /// Zero-based, end-exclusive scalar offsets.
    pub chars: Option<Range<usize>>,
    /// Symbol selection, even when unsupported by the task.
    pub symbol: Option<&'a str>,
}

impl<'a> TargetRef<'a> {
    /// Validate borrowed syntax before allocating output or attempting reads.
    pub fn parse(raw: &'a str, kind: Kind) -> Option<Self> {
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
        let mut target = Self {
            path,
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
                "symbol" => target.symbol = (!value.is_empty()).then_some(value),
                _ => {}
            }
        }
        Some(target)
    }
}

fn parse(raw: &str, kind: Kind) -> Option<Target> {
    TargetRef::parse(raw, kind).map(|target| Target {
        path: target.path.to_owned(),
        lines: target.lines,
        chars: target.chars,
        symbol: target.symbol.map(str::to_owned),
    })
}

/// A completed citation's position in the original text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Located {
    /// Original UTF-8 byte range.
    pub range: Range<usize>,
    /// Parsed citation or literal invalid syntax.
    pub directive: Directive,
}

#[derive(Clone, Debug)]
struct Prefix {
    start: usize,
    matched: usize,
    block: bool,
}

#[derive(Clone, Debug)]
enum Pending {
    Body { start: usize, kind: Kind },
    Closed { start: usize },
    Attributes { start: usize },
}

/// Append-only owned AST. The same allocation-free scanner serves budgeted readers.
#[derive(Clone, Debug, Default)]
pub struct Stream {
    source: String,
    nodes: Vec<Located>,
    scanner: Scanner,
    #[cfg(test)]
    swallow_invalid: bool,
    #[cfg(test)]
    work: usize,
}

fn record(source: &str, nodes: &mut Vec<Located>, range: Range<usize>, kind: Kind) -> usize {
    let Some(raw) = source.get(range.clone()) else {
        return 0;
    };
    let length = raw.len();
    nodes.push(Located {
        range,
        directive: Directive::parse(raw, kind),
    });
    length
}

impl Stream {
    /// Append arbitrary UTF-8 chunks, keeping optional block attributes provisional.
    pub fn push(&mut self, chunk: &str) {
        self.source.push_str(chunk);
        #[cfg(test)]
        {
            self.scanner.swallow_invalid = self.swallow_invalid;
            self.work += chunk.len();
        }
        let result = self.scanner.push(chunk, &mut |range, kind| {
            let length = record(&self.source, &mut self.nodes, range, kind);
            #[cfg(test)]
            {
                self.work += length;
            }
            #[cfg(not(test))]
            let _ = length;
            Ok::<_, std::convert::Infallible>(())
        });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }

    /// Completed nodes; unfinished citations remain literal provisional text.
    pub fn nodes(&self) -> &[Located] {
        &self.nodes
    }
    /// Exact original bytes, including invalid and provisional syntax.
    pub fn source(&self) -> &str {
        &self.source
    }
    /// Finalize the message without accepting a later attribute list.
    pub fn finish(&mut self) {
        let result = self.scanner.finish(&mut |range, kind| {
            let length = record(&self.source, &mut self.nodes, range, kind);
            #[cfg(test)]
            {
                self.work += length;
            }
            #[cfg(not(test))]
            let _ = length;
            Ok::<_, std::convert::Infallible>(())
        });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }
}

#[cfg(test)]
#[path = "../tests/directives.rs"]
mod tests;
