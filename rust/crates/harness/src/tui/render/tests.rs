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

fn fragmented_text_cost(old: bool) -> (Line<'static>, acyclic_test_support::AllocationInfo) {
    let mut markdown = Markdown::default();
    markdown.push("first");
    markdown.push("second");
    let row = markdown.lines().first().expect("paragraph row");
    let span = row.spans.iter().next().expect("canonical run");
    assert!(matches!(span.text.as_str(), std::borrow::Cow::Owned(_)));
    let mut rendered = Line::default();
    let cost = acyclic_test_support::measure(|| {
        rendered = if old {
            Line::from(
                row.spans
                    .iter()
                    .map(|span| {
                        Span::styled(
                            super::super::display_text(&span.text.as_str()).into_owned(),
                            Style::default(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            line(row)
        };
    });
    (rendered, cost)
}

#[test]
fn fragmented_text_conversion_preserves_the_flattened_allocation() {
    let [(baseline, before), (candidate, after)] =
        acyclic_test_support::differential::pair::<_, _, (&str, usize)>(
            || (),
            |(), old| {
                let (text, cost) = fragmented_text_cost(old);
                acyclic_test_support::differential::record(("display", text.spans.len()));
                (text, cost)
            },
        );
    assert_eq!(before, after);
    assert_eq!(baseline.0, candidate.0);
    assert_eq!(candidate.0, Line::raw("firstsecond"));
    eprintln!(
        "conversion baseline={:?} candidate={:?}",
        baseline.1, candidate.1
    );
    conversion_cost(candidate.1);
    assert_eq!((baseline.1.count_total, baseline.1.bytes_total), (6, 382));
    assert_eq!(candidate.1.count_total + 1, baseline.1.count_total);
    assert_eq!(candidate.1.bytes_total + 11, baseline.1.bytes_total);
}

fn conversion_cost(cost: acyclic_test_support::AllocationInfo) {
    assert_eq!(
        (cost.count_total, cost.bytes_total),
        (5, 371),
        "fragmented row conversion"
    );
}

#[test]
#[should_panic(expected = "fragmented row conversion")]
fn fragmented_text_allocation_control_detects_second_copy() {
    conversion_cost(fragmented_text_cost(true).1);
}
