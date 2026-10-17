use super::*;
use std::{collections::HashSet, sync::Arc};

fn retained(markdown: &Markdown) -> (usize, usize) {
    let mut targets = HashSet::new();
    let mut texts = HashSet::new();
    let mut nodes = HashSet::new();
    let mut text_metadata = 0;
    let mut bytes = markdown.source.capacity() + markdown.visibility.retained();
    let (state_bytes, state_metadata) =
        markdown
            .state
            .text_retained(&mut nodes, &mut texts, &mut targets);
    bytes += state_bytes;
    text_metadata += state_metadata;
    bytes += markdown
        .context
        .iter()
        .map(|container| container.opening.capacity() + container.continuation.capacity())
        .sum::<usize>();
    for line in &markdown.lines {
        let (payload, metadata) = line.spans.retained(&mut nodes, &mut texts, &mut targets);
        bytes += payload;
        text_metadata += metadata;
    }
    for cache in &markdown.cached {
        let (payload, metadata) = cache
            .state
            .text_retained(&mut nodes, &mut texts, &mut targets);
        bytes += payload;
        text_metadata += metadata;
        bytes += cache.template.source.capacity();
        bytes += cache
            .template
            .keys
            .iter()
            .map(|key| key.as_ref().len())
            .sum::<usize>();
    }
    for (key, (destination, title, _)) in markdown
        .references
        .committed
        .iter()
        .chain(&markdown.references.pending)
    {
        bytes += key.as_ref().len();
        for target in [destination, title] {
            if targets.insert(Arc::as_ptr(target).cast::<u8>()) {
                bytes += target.len();
            }
        }
    }
    let (table_payload, table_metadata) = markdown.table.as_ref().map_or((0, 0), |table| {
        table.retained(&mut targets, &mut texts, &mut nodes)
    });
    bytes += table_payload;
    let (inline_payload, inline_metadata) = markdown.inline.as_ref().map_or((0, 0), |inline| {
        inline.retained(&mut nodes, &mut texts, &mut targets)
    });
    bytes += inline_payload;
    let metadata = text_metadata
        + table_metadata
        + inline_metadata
        + markdown.context.capacity() * std::mem::size_of::<Container>()
        + markdown.lines.capacity() * std::mem::size_of::<Line>()
        + markdown.cached.capacity() * std::mem::size_of::<Cached>()
        + markdown.dependencies.capacity() * std::mem::size_of::<(Key, Vec<usize>)>()
        + markdown
            .dependencies
            .values()
            .map(|indices| indices.capacity() * std::mem::size_of::<usize>())
            .sum::<usize>()
        + (markdown.references.committed.capacity() + markdown.references.pending.capacity())
            * std::mem::size_of::<(Key, (Arc<str>, Arc<str>, usize))>();
    (bytes, bytes + metadata)
}

#[test]
fn backslash_checkpoint_context_has_linear_work() {
    let measure = |count| {
        let before = work::current();
        let mut markdown = Markdown::default();
        for _ in 0..count {
            markdown.push("\\\\");
        }
        markdown.push("*‐*");
        let used = work::current() - before;
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{}*‐*", "\\\\".repeat(count)))
        );
        used
    };
    let small = measure(512);
    let large = measure(1024);
    assert!(large <= 2 * small + 1024, "work {small} -> {large}");
}

fn measure(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
    copies: usize,
) -> (usize, usize) {
    measure_with(
        actions,
        count,
        destination,
        padding,
        separators,
        copies,
        &mut |_, source| full_parse(source),
    )
}

fn measure_with(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
    copies: usize,
    oracle: &mut impl FnMut(&str, &str) -> Vec<Line>,
) -> (usize, usize) {
    let mut markdown = Markdown::default();
    let mut total = 0;
    let mut source = String::new();
    let mut consumer = Vec::new();
    let mut expected = Vec::new();
    let mut peak = 0;
    let mut growth_total = 0;
    let mut push = |delta: &str, finish: bool| {
        let growth_before = work::source_growth();
        let before = work::current()
            + markdown.visible_work.get()
            + markdown.reference_cache_visits
            + markdown.reference_moved_lines
            + markdown.boundary_work;
        let open = markdown.source.len() - markdown.offset;
        let visits_before = markdown.reference_cache_visits;
        // Definition updates reopen dependency blocks; ordinary pushes do not.
        let cached = markdown.cached.len();
        let changed = markdown.push(delta);
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend_from_slice(markdown.lines().get(changed..).unwrap_or_default());
        let used = work::current()
            + markdown.visible_work.get()
            + markdown.reference_cache_visits
            + markdown.reference_moved_lines
            + markdown.boundary_work
            - before;
        let dependent = if markdown.reference_cache_visits > visits_before {
            markdown
                .cached
                .get(..cached)
                .expect("retained dependency blocks")
                .iter()
                .map(|cache| cache.template.source.len())
                .sum::<usize>()
        } else {
            0
        };
        let growth = work::source_growth() - growth_before;
        growth_total += growth;
        // Geometric source-buffer growth is prepaid by two copies per input byte.
        assert!(growth_total <= 2 * markdown.source.len() + 64);
        let amortized = used - growth + 2 * delta.len();
        assert!(
            amortized <= 128 * (delta.len() + open + dependent + 1) + 256,
            "push work {used} for {} chunk bytes and {open} open bytes",
            delta.len()
        );
        total += used;
        let (payload, bytes) = retained(&markdown);
        assert!(payload <= markdown.source.len() * 8 + 1024);
        peak = peak.max(bytes);
        source.push_str(delta);
        // Empty chunks have the same one-shot oracle as the preceding push.
        if !delta.is_empty() {
            expected = oracle(delta, &source);
        }
        assert!(
            consumer == expected,
            "consumer differs after {delta:?}, source {:?}",
            source.chars().take(256).collect::<String>()
        );
        finish
            .then(|| {
                markdown.fence.as_ref().map(|fence| {
                    if fence.indented {
                        "\nend\n\n".to_owned()
                    } else {
                        format!(
                            "\n{}{}\n\n",
                            fence.opening,
                            char::from(fence.marker).to_string().repeat(fence.count)
                        )
                    }
                })
            })
            .flatten()
    };
    for index in 0..count * copies {
        for (position, action) in actions.iter().enumerate() {
            production(*action, index, padding, &mut |delta| {
                push(delta, false);
            });
            push(
                ["", " ", "\n", "!", "x"][separators[position % separators.len()] as usize],
                false,
            );
        }
    }
    let definition = actions
        .iter()
        .any(|action| matches!(action, 3 | 15))
        .then(|| format!("\n\n[r]: /{destination}\n\n"));
    let closing = push(definition.as_deref().unwrap_or("\n\n"), true);
    // Preserve the original prefix checks, then actually exercise late
    // resolution even when repetition left the first definition inside code.
    if let Some(closing) = closing {
        push(&closing, false);
        if let Some(definition) = definition {
            push(&definition, false);
        }
    }
    let (payload, bytes) = retained(&markdown);
    assert!(
        payload <= source.len() * 8 + 1024,
        "{bytes} retained for {} source bytes",
        source.len()
    );
    (total, peak.max(bytes))
}

// Both repetitions share the same original prefix. Parse its oracle fixtures
// once; work/storage measurement still uses independent production renderers.
fn measure_pair(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
) -> ((usize, usize), (usize, usize)) {
    let mut fixtures = Vec::new();
    let small = measure_with(
        actions,
        count,
        destination,
        padding,
        separators,
        1,
        &mut |delta, source| {
            let expected = full_parse(source);
            fixtures.push((delta.to_owned(), expected.clone()));
            expected
        },
    );
    let mut index = 0;
    let mut same = true;
    let large = measure_with(
        actions,
        count,
        destination,
        padding,
        separators,
        2,
        &mut |delta, source| {
            let fixture = fixtures.get(index);
            index += 1;
            same &= fixture.is_some_and(|(raw, _)| raw == delta);
            if same {
                fixture.expect("shared prefix fixture").1.clone()
            } else {
                full_parse(source)
            }
        },
    );
    (small, large)
}

fn production(action: u8, index: usize, padding: usize, push: &mut impl FnMut(&str)) {
    match action {
        0 => push("word"),
        1 => push("*em* **strong** ~~gone~~"),
        2 => push("[label](u)"),
        3 => push("[x][r]"),
        4 => push("![](u)\n\n"),
        5 => {
            push(&format!("![][i{index:08x}]\n\n"));
            push(&format!("[i{index:08x}]: /u\n\n"));
        }
        6 => {
            push(&format!("-{}parent\n\n", " ".repeat(padding)));
            push(&format!("{}child\n", " ".repeat(padding + 3)));
        }
        7 => {
            push("```rust\n");
            push("let x = 1;\n");
            push("```\n");
        }
        8 => push("\n\n"),
        9 => push("![alt](u)"),
        10 => push("[outer [inner](u)](v)"),
        11 => push("a &amp; b"),
        12 => push("[label](u)"),
        13 => push("word\n"),
        14 => {
            if index == 0 {
                push("| a | b |\n|---|---|\n");
            }
            push("| x | y |\n");
        }
        15 => {
            push("word ![](u)\n\n");
            push("![][r]\n\n");
        }
        16 => push("`code`"),
        17 => push("*em*"),
        18 => push("**bold**"),
        19 => push("~~strike~~"),
        20 => push("<https://example.org>"),
        21 => push("<i>"),
        22 => push("\\*"),
        23 => push("&amp;"),
        24 => push("![](u)"),
        25 => push("[a][]"),
        26 => push("_em_"),
        27 => push("__bold__"),
        28 => push("</i>"),
        29 => push("<a@example.org>"),
        30 => push("&#42;"),
        31 => push("\\\\"),
        32 => push("&#10;"),
        34 => {
            push("***");
            push("\n");
        }
        _ => push("  \n"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sequence_work.txt")
        ))),
        ..ProptestConfig::with_cases(24)
    })]
    #[test]
    fn push_grammar_has_linear_work_and_storage(
        actions in prop_oneof![
            prop::collection::vec(0u8..4, 1..5),
            prop::collection::vec(prop::sample::select(vec![4u8, 5, 8]), 1..5),
            prop::collection::vec(0u8..16, 1..5),
            prop::collection::vec(12u8..16, 1..5),
            prop::sample::select(vec![vec![12u8], vec![13], vec![14]]),
            prop::collection::vec(0u8..35, 1..5),
            prop::collection::vec(16u8..35, 1..5),
        ],
        count in 1usize..65,
        destination in "[a-z]{64,1024}",
        padding in 1usize..5,
        separators in prop::collection::vec(0u8..5, 1..5),
    ) {
        let (small, large) = measure_pair(&actions, count, &destination, padding, &separators);
        prop_assert!(large.0 <= 2 * small.0 + 1024, "work {} -> {}", small.0, large.0);
        prop_assert!(large.1 <= 2 * small.1 + 1024, "storage {} -> {}", small.1, large.1);
    }
}

#[test]
fn grammar_property_detects_copying_the_reference_suffix() {
    let measure_pair = |copy: bool| {
        crate::reference_render::COPY_SUFFIX.set(copy);
        let small = measure(&[15, 16, 7], 7, &"x".repeat(540), 2, &[0], 1).0;
        let large = measure(&[15, 16, 7], 7, &"x".repeat(540), 2, &[0], 2).0;
        crate::reference_render::COPY_SUFFIX.set(false);
        (small, large)
    };
    let retained = measure_pair(false);
    println!("retained batch: {} -> {}", retained.0, retained.1);
    assert!(retained.1 <= 2 * retained.0 + 1024);
    let copied = measure_pair(true);
    println!(
        "copied suffix negative control: {} -> {}",
        copied.0, copied.1
    );
    assert!(copied.0 > retained.0 && copied.1 > retained.1);
}

#[test]
fn grammar_property_detects_reclassifying_the_entire_suffix() {
    let measure_pair = |replay| {
        crate::inline::REPLAY_CLASSIFIER.set(replay);
        let small = measure(&[18], 64, "unused", 1, &[0], 1).0;
        let large = measure(&[18], 64, "unused", 1, &[0], 2).0;
        crate::inline::REPLAY_CLASSIFIER.set(false);
        (small, large)
    };
    let retained = measure_pair(false);
    assert!(retained.1 <= 2 * retained.0 + 1024);
    let replayed = measure_pair(true);
    println!(
        "classifier grammar negative control: {} -> {}",
        replayed.0, replayed.1
    );
    assert!(replayed.1 > 2 * replayed.0 + 1024);
}

#[test]
fn late_reference_definition_closes_the_open_paragraph_once() {
    let input = include_bytes!("../fuzz/corpus/chunk_splits/late-reference-blank-row");
    let size = u64::from_le_bytes(input[..8].try_into().unwrap()) as usize;
    let source = std::str::from_utf8(&input[8 + size..]).unwrap();
    let destination = source.split_once("[r]: /").unwrap().1.trim_end();
    let small = measure(&[3], 42, destination, 4, &[0], 1);
    let large = measure(&[3], 42, destination, 4, &[0], 2);
    assert!(large.0 <= 2 * small.0 + 1024);
    assert!(large.1 <= 2 * small.1 + 1024);
}

#[test]
fn storage_property_detects_retaining_frozen_operation_keys() {
    let pair = |keep| {
        crate::inline_scan::KEEP_KEYS.set(keep);
        let small = measure(&[17, 26], 50, "unused", 3, &[2, 1], 1).1;
        let large = measure(&[17, 26], 50, "unused", 3, &[2, 1], 2).1;
        crate::inline_scan::KEEP_KEYS.set(false);
        (small, large)
    };
    let retained = pair(false);
    assert!(retained.1 <= 2 * retained.0 + 1024);
    let redundant = pair(true);
    assert!(redundant.1 > 2 * redundant.0 + 1024);
}

#[test]
fn mixed_styles_and_html_have_linear_total_work() {
    let small = measure(&[17, 28], 57, "unused", 3, &[2, 1], 1);
    let large = measure(&[17, 28], 57, "unused", 3, &[2, 1], 2);
    assert!(large.0 <= 2 * small.0 + 1024);
    assert!(large.1 <= 2 * small.1 + 1024);
}
