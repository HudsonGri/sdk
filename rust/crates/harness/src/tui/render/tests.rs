use super::*;
use proptest::prelude::*;

fn partition(text: &str, cuts: &[bool]) {
    let mut stream = StreamText::default();
    let mut rows = Vec::new();
    for end in 1..=text.len() {
        if end != text.len() && !cuts.get(end - 1).copied().unwrap_or(false) {
            continue;
        }
        let bytes = &text.as_bytes()[..end];
        let pending = std::str::from_utf8(bytes).is_err();
        let visible = String::from_utf8_lossy(bytes);
        let changed = stream.push(&visible, usize::from(pending) * 3);
        rows.truncate(changed);
        rows.extend(stream.rows(changed));
    }
    let mut unsplit = StreamText::default();
    let changed = unsplit.push(text, 0);
    assert_eq!(rows, unsplit.rows(changed));
    assert_eq!(stream.work, text.len(), "stable bytes are parsed once");
}

#[test]
fn provisional_archive_row_is_invalidated() {
    let seed = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../harness-fuzz/corpus/ui_prompt/utf8-archive-row-repair"
    ));
    let text = format!("retained archive\n\n{seed}");
    let mut cuts = vec![false; text.len()];
    cuts[text.len() - 3] = true;
    partition(&text, &cuts);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn byte_partitions_preserve_rows_and_linear_work(
        text in "[a-z日é€🙂\\n*# ]{1,40}",
        cuts in prop::collection::vec(any::<bool>(), 0..200)
    ) { partition(&format!("retained archive\n\n{text}"), &cuts); }
}

#[test]
fn file_citations_offsets_survive_streamed_crlf_normalization() {
    use acyclic_markdown::directive::{Resolution, ResolutionError};
    let source = "retained\r\n\r\n::file[x]{lines=1\r\n chars=0:1}\r\n\r\n";
    let raw = "::file[x]{lines=1\r\n chars=0:1}";
    let at = source.find(raw).unwrap();
    let values = || {
        [(
            at..at + raw.len(),
            raw.to_owned(),
            Resolution::Error(ResolutionError::Denied),
        )]
    };
    let mut whole = StreamText::default();
    whole.push(source, 0);
    whole.resolve(values());
    let mut stream = StreamText::default();
    for end in 1..=source.len() {
        stream.push(&source[..end], 0);
    }
    stream.resolve(values());
    assert_eq!(stream.markdown.lines(), whole.markdown.lines());
    let text: String = stream
        .markdown
        .lines()
        .iter()
        .flat_map(|line| line.spans.iter().map(|span| span.text.as_str()))
        .collect();
    assert!(text.contains("read denied"), "{text}");
    assert!(!text.contains("::file["));
}
#[test]
fn file_citations_stream_reset_also_resets_snapshot_identity() {
    use crate::file_citations::{FileCitation, FileCitations};
    use acyclic_markdown::directive::{Resolution, ResolutionError};
    let raw = "::file[x]";
    let mut transcript = crate::client::TranscriptView::default();
    transcript.streaming_text = raw.into();
    transcript.text_turns.insert(0, 0..raw.len());
    std::sync::Arc::make_mut(&mut transcript.file_citations).insert(
        0,
        std::sync::Arc::new(FileCitations {
            source_bytes: raw.len() as u64,
            entries: vec![FileCitation {
                range: 0..raw.len(),
                raw: raw.into(),
                resolution: Resolution::Error(ResolutionError::Denied),
                read: None,
            }],
            ..Default::default()
        }),
    );
    let render = |stream: &mut StreamText| {
        stream.push(raw, 0);
        stream.resolve_transcript(&transcript);
        stream
            .markdown
            .lines()
            .iter()
            .flat_map(|line| line.spans.iter().map(|span| span.text.as_str()))
            .collect::<String>()
    };
    let mut stream = StreamText::default();
    let expected = render(&mut stream);
    assert!(expected.contains("read denied"));
    stream = StreamText::default();
    assert_eq!(render(&mut stream), expected);
    // Keeping the old identity while resetting its parser loses the excerpt.
    let mut broken = StreamText {
        citations: Some(transcript.file_citations.clone()),
        ..Default::default()
    };
    assert_ne!(render(&mut broken), expected);
}
