use super::*;

fn replay(source: &str, cuts: &[usize]) {
    let mut markdown = Markdown::default();
    let mut consumer = Vec::new();
    let mut start = 0;
    for end in cuts.iter().copied().chain([source.len()]) {
        if end <= start || end > source.len() || !source.is_char_boundary(end) {
            continue;
        }
        let delta = source.get(start..end).expect("UTF-8 chunk");
        let changed = markdown.push(delta);
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend_from_slice(markdown.lines().get(changed..).expect("change boundary"));
        assert_eq!(
            consumer,
            full_parse(source.get(..end).expect("UTF-8 prefix")),
            "source {source:?}, cuts {cuts:?}, prefix {end}"
        );
        start = end;
    }
}

fn units(chunks: &[String], count: usize) -> (usize, usize) {
    let before = work::current();
    let mut markdown = Markdown::default();
    for chunk in chunks.iter().cycle().take(count) {
        markdown.push(chunk);
    }
    let used = work::current() - before
        + markdown.visible_work.get()
        + markdown.reference_cache_visits
        + markdown.reference_moved_lines
        + markdown.boundary_work;
    (used, markdown.source.len())
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inline_chunks.txt")
        ))),
        ..ProptestConfig::with_cases(24)
    })]
    #[test]
    fn arbitrary_chunk_splits_equal_full_render(
        input in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(|character| character.to_string()),
            prop::sample::select(vec!["\\", "*", "!", "[", "]", "&", ";", "\n", "`", "_", "~",
                "[r]: /r\n[s]: /s\n\nprefix ", "[r]", "[s]", "(u)", "[]",
                "**a**", "`a`", "~~a~~", "[a](u)", "[`x](u)", "![`x](u)", "<https://x>", "\\\n", "\x0b\n",
                "| `a` | b |\n| - | - |\n| x | y |\n",
            ])
                .prop_map(str::to_owned),
        ], 0..64),
        splits in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let source = input.concat();
        let mut cuts: Vec<_> = splits.iter().map(|cut| usize::from(*cut) % (source.len() + 1)).collect();
        cuts.sort_unstable();
        replay(&source, &cuts);
        // Every byte boundary is also a possible split, including escape pairs.
        replay(&source, &(1..source.len()).collect::<Vec<_>>());
    }

    #[test]
    fn adversarial_inline_work_is_linear(
        chunks in prop::collection::vec(prop::sample::select(vec![
            "\\*", "!", "**bold**", "*em*", "_em_", "~~gone~~", "`code`",
            "[x](u)", "[x][r]", "[]", "&amp;", "&#42;", "word", "\n",
            "\\", "*", "[", "]", "&", ";", "x",
        ]).prop_map(str::to_owned), 1..5),
    ) {
        for count in [64, 128, 256, 1024] {
            let (work, bytes) = units(&chunks, count);
            prop_assert!(work <= 128 * bytes + 4096,
                "{chunks:?}: {count} chunks, {work} work for {bytes} bytes");
        }
    }
}

#[test]
fn checked_in_chunk_corpus_matches_full_render() {
    for data in [
        include_bytes!("../fuzz/corpus/chunk_splits/control-flanking").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/code-literal-nul").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/visible-source-end").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/restored-scope-frontier").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escape-pair").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-image").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/code-escaped-run").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escape-newline").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/opaque-precedence").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/partial-opener").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/reference-label-dependency").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/fence-row-context").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-stars").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/bangs").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/mixed").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/list-indent").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/deferred-row-indent").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/shortcut-inline-continuation").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/reference-inline-suffix").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/collapsed-inline-suffix").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-break-whitespace").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/inline-pass-checkpoint").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/inline-pass-table-shape").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/paragraph-source-start").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-reference-suffix").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/code-steals-link").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/definition-paragraph-start").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/scope-ancestor-frontier").as_slice(),
    ] {
        let encoded = data
            .get(..8)
            .expect("corpus header")
            .try_into()
            .expect("eight bytes");
        let flags = usize::try_from(u64::from_le_bytes(encoded)).expect("corpus length");
        let source =
            std::str::from_utf8(data.get(8 + flags..).expect("corpus body")).expect("UTF-8 corpus");
        replay(source, &(1..source.len()).collect::<Vec<_>>());
    }
}

#[test]
fn every_punctuation_escape_preserves_terminal_source_coverage() {
    for byte in 0u8..=127 {
        if !byte.is_ascii_punctuation() {
            continue;
        }
        for ending in ["", "\n", "\r\n", "\nnext"] {
            let source = format!("x \\{}{ending}", char::from(byte));
            replay(&source, &(1..source.len()).collect::<Vec<_>>());
        }
    }
}

#[test]
fn shortcut_scope_extension_replaces_the_entire_previous_scope() {
    let source = "[r]: /r\n[s]: /s\n\nprefix [r][s]";
    let start = source.find("prefix").expect("paragraph");
    for first in start..=source.len() {
        for second in first..=source.len() {
            replay(source, &[first, second]);
        }
    }
}
