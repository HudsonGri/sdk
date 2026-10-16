use super::REPLAY;
use crate::{Markdown, work};
use proptest::prelude::*;

fn measure(parts: &[&str], separator: &str, count: usize, replay: bool) -> (usize, usize) {
    let mut markdown = Markdown::default();
    markdown.push("x ");
    REPLAY.set(replay);
    let before = work::rendering_work();
    let mut bytes = 0;
    for part in parts.iter().cycle().take(count) {
        markdown.push(part);
        markdown.push(separator);
        bytes += part.len() + separator.len();
    }
    let used = work::rendering_work() - before;
    REPLAY.set(false);
    (used, bytes)
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
        for count in [64, 128, 256, 1024] {
            let small = measure(&parts, separator, count, false);
            let (used, bytes) = measure(&parts, separator, count * 2, false);
            prop_assert!(used <= 128 * bytes + 4096,
                "{parts:?} separated by {separator:?}: {} chunks, {used} render units for {bytes} bytes", count * 2);
            prop_assert!(used <= 2 * small.0 + 4096,
                "{parts:?} separated by {separator:?}: {count} -> {} chunks use {} -> {used} render units", count * 2, small.0);
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
