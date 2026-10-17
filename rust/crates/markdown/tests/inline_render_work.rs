use super::REPLAY;
use crate::{Markdown, work};
use proptest::prelude::*;

fn samples(
    parts: &[&str],
    separator: &str,
    counts: &[usize],
    replay: bool,
) -> Vec<(usize, usize, [usize; 4])> {
    let mut markdown = Markdown::default();
    markdown.push("x ");
    REPLAY.set(replay);
    let before = work::rendering_work();
    let parts_before = work::rendering_parts();
    let mut bytes = 0;
    let mut snapshots = Vec::new();
    for (index, part) in parts
        .iter()
        .cycle()
        .take(*counts.last().expect("sample count"))
        .enumerate()
    {
        markdown.push(part);
        markdown.push(separator);
        bytes += part.len() + separator.len();
        if counts.contains(&(index + 1)) {
            let parts_after = work::rendering_parts();
            snapshots.push((
                work::rendering_work() - before,
                bytes,
                std::array::from_fn(|i| parts_after[i] - parts_before[i]),
            ));
        }
    }
    REPLAY.set(false);
    snapshots
}

fn measure(
    parts: &[&str],
    separator: &str,
    count: usize,
    replay: bool,
) -> (usize, usize, [usize; 4]) {
    samples(parts, separator, &[count], replay)
        .pop()
        .expect("one sample")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn inline_render_work_is_linear(
        parts in prop::collection::vec(prop::sample::select(vec![
            "*", "**", "***", "_", "__", "~", "~~", "`", "``", "```",
            "[", "]", "![", "](u)", "\\*", "\\`", "\\\\", "\\", "!", "x", " ", "¡", "‐", "🦀",
            "[x](u)", "![x](u)", "[a *b*](<u> \"t\")", "![*x* ![y](v)](u)",
            "x![](u)y", "![a ![](u)b](u)", "[x][r]", "[r]: /url\n\n",
            "<https://x>", "<a@b.example>", "&amp;", "&#10;", "&#13;", "&#0;",
            "<b>", "</b>", "<!--\n x\n-->", "\0", "word\n", "`code`", "**bold**",
        ]), 1..5),
        separator in prop::sample::select(vec!["", " ", "\n", "!", "x"]),
    ) {
        // Each smaller run is an identical prefix of the largest run. Retain
        // the counter checkpoints instead of executing those prefixes again.
        let counts = [64, 128, 256, 512, 1024, 2048];
        let snapshots = samples(&parts, separator, &counts, false);
        for count in [64, 128, 256, 1024] {
            let small = snapshots[counts.iter().position(|n| *n == count).unwrap()];
            let (used, bytes, breakdown) = snapshots[counts.iter().position(|n| *n == 2 * count).unwrap()];
            prop_assert!(used <= 128 * bytes + 4096,
                "{parts:?} separated by {separator:?}: {} chunks, {used} render units for {bytes} bytes", count * 2);
            prop_assert!(used <= 2 * small.0 + 4096,
                "{parts:?} separated by {separator:?}: {count} -> {} chunks use {} -> {used} render units; copy/visibility/feed/scan {:?} -> {breakdown:?}", count * 2, small.0, small.2);
        }
    }
}

#[test]
fn rendering_work_counts_replayed_scans_and_copies() {
    let small = measure(&["`code`"], "", 128, true);
    let large = measure(&["`code`"], "", 256, true);
    assert!(small.0 > 128 * small.1 + 4096);
    assert!(large.0 > 2 * small.0 + 4096);
}

#[test]
fn rendering_work_detects_replaying_completed_rules() {
    let retained = measure(&["***"], "\n", 128, false);
    assert!(retained.0 <= 128 * retained.1 + 4096);
    crate::SKIP_RULE_CHECKPOINT.set(true);
    let replayed = measure(&["***"], "\n", 128, false);
    crate::SKIP_RULE_CHECKPOINT.set(false);
    assert!(replayed.0 > 128 * replayed.1 + 4096);
}
