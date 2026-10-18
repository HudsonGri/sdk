//! Message-local, immutable file excerpts published through ordinary task files.
use crate::{Error, Result, durable_host::TaskVolumeSnapshot};
use acyclic_markdown::directive::{Directive, Kind, Located, Resolution, Scanner, TargetRef};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Maximum block citations resolved for one assistant message.
pub const MAX_BLOCKS: usize = 256;

pub(crate) type Views =
    std::sync::Arc<std::collections::BTreeMap<u64, std::sync::Arc<FileCitations>>>;

/// One result bound to the exact authored bytes in its message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCitation {
    /// Original UTF-8 source offsets.
    pub range: Range<usize>,
    /// Exact block directive, including its selection attributes.
    pub raw: String,
    /// Immutable excerpt or typed failure.
    pub resolution: Resolution,
    /// Authorized source prefix observed at the snapshot's generation.
    pub read: Option<Range<u64>>,
}

/// `turns/<n>/citations.json`; renderers never re-read source files.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCitations {
    /// Byte offset of this response step in the completed turn's concatenated text.
    pub output_offset: u64,
    /// Complete message length; a retained suffix cannot reuse full-message offsets.
    pub source_bytes: u64,
    /// Last assistant chunk covered by this file.
    pub through_sequence: Option<u64>,
    /// One project cut shared by every read fact and excerpt.
    pub view: Option<TaskVolumeSnapshot>,
    /// Message-wide rejection, before any excerpt reads.
    pub error: Option<Resolution>,
    /// Source-ordered results, at most 256.
    #[serde(deserialize_with = "entries")]
    pub entries: Vec<FileCitation>,
}

impl FileCitations {
    /// Decode a bounded snapshot; source offsets are checked again before use.
    pub fn decode(bytes: &[u8], maximum_bytes: u64) -> Result<Self> {
        if bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "file citation snapshot exceeds its bound".into(),
            ));
        }
        let snapshot: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Invalid("invalid file citation snapshot".into()))?;
        if snapshot
            .output_offset
            .checked_add(snapshot.source_bytes)
            .is_none()
            || snapshot.entries.len() > MAX_BLOCKS
            || snapshot
                .error
                .as_ref()
                .is_some_and(|error| !matches!(error, Resolution::Error(_)))
            || snapshot.error.is_some() && !snapshot.entries.is_empty()
            || snapshot.view.as_ref().is_some_and(|view| {
                view.volume.provider() != view.generation.as_resource().provider()
                    || view.volume.class() != crate::conversation::VolumeClass::Project
            })
            || snapshot
                .entries
                .windows(2)
                .any(|pair| matches!(pair, [left, right] if left.range.end > right.range.start))
            || snapshot.entries.iter().any(|entry| {
                entry.range.end as u64 > snapshot.source_bytes
                    || entry.range.end.checked_sub(entry.range.start) != Some(entry.raw.len())
                    || Directive::parse(&entry.raw, Kind::Block).target().is_none()
                    || matches!(entry.resolution, Resolution::Content(_)) && entry.read.is_none()
                    || entry
                        .read
                        .as_ref()
                        .is_some_and(|read| read.start != 0 || snapshot.view.is_none())
            })
        {
            return Err(Error::Invalid("inconsistent file citation snapshot".into()));
        }
        Ok(snapshot)
    }

    /// Results bound to complete literal directives in one exact message.
    pub(crate) fn results(
        &self,
        source: &str,
        offset: usize,
    ) -> Vec<(Range<usize>, String, Resolution)> {
        if source.len() as u64 != self.source_bytes {
            return Vec::new();
        }
        blocks(source)
            .into_iter()
            .filter_map(|node| {
                let resolution = self.resolution(source, &node)?.clone();
                Some((
                    node.range.start + offset..node.range.end + offset,
                    node.directive.raw().to_owned(),
                    resolution,
                ))
            })
            .collect()
    }

    /// A result is usable only for the exact directive bytes at the recorded offset.
    pub fn resolution(&self, source: &str, directive: &Located) -> Option<&Resolution> {
        if source.len() as u64 != self.source_bytes
            || source.get(directive.range.clone()) != Some(directive.directive.raw())
        {
            return None;
        }
        if self.error.is_some() {
            return self.error.as_ref();
        }
        let index = self
            .entries
            .binary_search_by_key(&directive.range.start, |entry| entry.range.start)
            .ok()?;
        let entry = self.entries.get(index)?;
        (entry.range == directive.range && entry.raw == directive.directive.raw())
            .then_some(&entry.resolution)
    }
}

/// Parse each streamed slice once, retaining only complete, valid block citations.
pub(crate) fn blocks(source: &str) -> Vec<Located> {
    let mut excluded = pulldown_cmark::Parser::new(source)
        .into_offset_iter()
        .filter_map(|(event, range)| {
            matches!(
                event,
                pulldown_cmark::Event::Code(_)
                    | pulldown_cmark::Event::Html(_)
                    | pulldown_cmark::Event::InlineHtml(_)
                    | pulldown_cmark::Event::Start(pulldown_cmark::Tag::CodeBlock(_))
            )
            .then_some(range)
        })
        .peekable();
    let mut nodes = Vec::new();
    let mut scanner = Scanner::default();
    let mut emit = |range: Range<usize>, kind| {
        while excluded
            .peek()
            .is_some_and(|excluded| excluded.end <= range.start)
        {
            excluded.next();
        }
        if kind == Kind::Block
            && source
                .get(range.clone())
                .is_some_and(|raw| TargetRef::parse(raw, kind).is_some())
            && !excluded
                .peek()
                .is_some_and(|excluded| excluded.start < range.end && range.start < excluded.end)
        {
            let raw = source.get(range.clone()).ok_or(())?;
            nodes.push(Located {
                range,
                directive: Directive::parse(raw, kind),
            });
            if nodes.len() == MAX_BLOCKS {
                return Err(());
            }
        }
        Ok(())
    };
    if scanner.push(source, &mut emit).is_ok() {
        let _ = scanner.finish(&mut emit);
    }
    nodes
}

fn entries<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Vec<FileCitation>, D::Error> {
    struct Entries;
    impl<'de> serde::de::Visitor<'de> for Entries {
        type Value = Vec<FileCitation>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 256 file citations")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut entries = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_BLOCKS));
            while entries.len() < MAX_BLOCKS {
                let Some(entry) = sequence.next_element()? else {
                    return Ok(entries);
                };
                entries.push(entry);
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("too many file citations"));
            }
            Ok(entries)
        }
    }
    decoder.deserialize_seq(Entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_markdown::directive::ResolutionError;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn file_citations_ast_round_trip(raw in ".{0,128}", block in any::<bool>()) {
            let directive = Directive::parse(&raw, if block { Kind::Block } else { Kind::Inline });
            let bytes = serde_json::to_vec(&directive).unwrap();
            prop_assert_eq!(serde_json::from_slice::<Directive>(&bytes).unwrap(), directive);
        }
        #[test]
        fn file_citations_json_round_trip_and_source_binding(paths in prop::collection::vec("[a-z]{1,12}", 0..32)) {
            let mut source = String::new();
            let mut snapshot = FileCitations::default();
            for path in paths {
                let raw = format!("::file[{path}]");
                let start = source.len();
                source.push_str(&raw);
                source.push(' ');
                snapshot.entries.push(FileCitation { range: start..start + raw.len(), raw, resolution: Resolution::Error(ResolutionError::Denied), read: None });
            }
            snapshot.source_bytes = source.len() as u64;
            let bytes = serde_json::to_vec(&snapshot).unwrap();
            prop_assert_eq!(FileCitations::decode(&bytes, bytes.len() as u64).unwrap(), snapshot.clone());
            prop_assert_eq!(snapshot.results(&source, 0).len(), snapshot.entries.len());
            if !source.is_empty() {
                prop_assert!(snapshot.results(&source[1..], 0).is_empty());
            }
            if let Some(first) = snapshot.entries.first_mut() {
                first.range.end += 1;
                let broken = serde_json::to_vec(&snapshot).unwrap();
                prop_assert!(FileCitations::decode(&broken, broken.len() as u64).is_err());
            }
        }
    }
    #[test]
    fn file_citations_decoder_rejects_entry_overflow_and_unresolved_authoring() {
        let entry = FileCitation {
            range: 0..9,
            raw: "::file[x]".into(),
            resolution: Resolution::Error(ResolutionError::Denied),
            read: None,
        };
        let snapshot = FileCitations {
            source_bytes: ((MAX_BLOCKS + 1) * entry.raw.len()) as u64,
            entries: (0..=MAX_BLOCKS)
                .map(|index| FileCitation {
                    range: index * entry.raw.len()..(index + 1) * entry.raw.len(),
                    ..entry.clone()
                })
                .collect(),
            ..FileCitations::default()
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        assert!(FileCitations::decode(&bytes, bytes.len() as u64).is_err());
        for value in ["::file[x]", "prefix ::file[unfinished", "\\::file[x]"] {
            assert!(matches!(
                crate::task_volume::ChildRequest::parse(value.as_bytes(), 4096),
                Err(Error::UnresolvedFileDirective)
            ));
        }
        assert!(crate::task_volume::ChildRequest::parse(b"literal text :file[x]", 4096).is_ok());
    }
}
