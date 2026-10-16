use super::*;
use proptest::prelude::*;

fn oracle(source: &str) -> Option<(usize, String)> {
    let probe = format!("[x]{source}");
    pulldown_cmark::Parser::new_ext(&probe, crate::options())
        .into_offset_iter()
        .find_map(|(event, range)| match event {
            crate::Event::Start(crate::Tag::Link {
                dest_url,
                link_type: pulldown_cmark::LinkType::Inline,
                ..
            }) if range.start == 0 => Some((range.end - 3, dest_url.into_string())),
            _ => None,
        })
}

fn compare(source: &str, cuts: &[usize]) {
    let mut suffix = Suffix::default();
    for end in cuts.iter().copied().chain([source.len()]) {
        let prefix = &source[..end];
        let actual = suffix
            .push(prefix)
            .and_then(|end| suffix.target(prefix).map(|url| (end, url.to_string())));
        assert_eq!(actual, oracle(prefix), "prefix {prefix:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn destination_state_matches_full_parse_at_every_split(
        parts in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(String::from),
            prop::sample::select(vec![
                "(", ")", "<", ">", "\\", "\\(", "\\)", "\\<", "\\>",
                "u", " ", "\t", "\n", "\r\n", "\u{b}", "\0", "'", "\"", "&amp;", "&#40;", "&#41;",
                "<u>", "(u)", "a(b)c", " \'title\'", " \"title\"", " (title)",
            ]).prop_map(str::to_owned),
        ], 0..48),
        cuts in prop::collection::vec(any::<usize>(), 0..48),
    ) {
        let source = format!("({})tail", parts.concat());
        let mut cuts: Vec<_> = cuts.into_iter().map(|cut| cut % (source.len()+1))
            .filter(|cut| source.is_char_boundary(*cut)).collect();
        cuts.sort_unstable();
        cuts.dedup();
        compare(&source, &cuts);
        compare(&source, &source.char_indices().map(|(at,_)| at).collect::<Vec<_>>());
    }
}

fn used(count: usize, replay: bool) -> (usize, usize) {
    let mut suffix = Suffix {
        replay,
        ..Suffix::default()
    };
    let mut source = String::from("(<");
    let before = crate::work::current();
    for _ in 0..count {
        source.push_str("x\\> ");
        assert_eq!(suffix.push(&source), None);
    }
    source.push_str("> 'title')");
    assert_eq!(suffix.push(&source), Some(source.len()));
    assert_eq!(
        suffix.target(&source).as_deref(),
        oracle(&source).map(|(_, url)| url).as_deref()
    );
    (crate::work::current() - before, source.len())
}

#[test]
fn destination_feed_and_validation_are_linear() {
    for count in [64, 128, 256, 1024] {
        let small = used(count, false);
        let large = used(count * 2, false);
        assert!(large.0 <= 128 * large.1 + 4096, "{small:?} -> {large:?}");
        assert!(large.0 <= small.0 * 2 + 4096, "{small:?} -> {large:?}");
    }
}

#[test]
fn destination_properties_detect_replay_and_lost_escape_state() {
    let small = used(64, true);
    let large = used(128, true);
    assert!(large.0 > small.0 * 2 + 4096);
    let source = r"(u\)v)";
    let mut suffix = Suffix {
        forget_escape: true,
        ..Suffix::default()
    };
    let end = suffix.push(source);
    assert_ne!(end, oracle(source).map(|(end, _)| end));
}
