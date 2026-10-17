use super::*;
use proptest::prelude::*;

#[test]
fn syntax_and_exact_serialization() {
    for (raw, kind, valid) in [
        (":file[src/lib.rs#L10-L20]", Kind::Inline, true),
        (
            "::file[src/lib.rs#L10-20]{lines=2-4 chars=0:8 symbol=main}",
            Kind::Block,
            true,
        ),
        ("::file[path]{future=value}", Kind::Block, true),
        (":file[]", Kind::Inline, false),
        (":file[path#L0]", Kind::Inline, false),
        (":file[path#L4-2]", Kind::Inline, false),
        (
            ":file[path#L999999999999999999999999999]",
            Kind::Inline,
            false,
        ),
        (":file[path]{lines=1}", Kind::Inline, false),
        ("::file[path]{chars=4:2}", Kind::Block, false),
        ("::file[path]{lines=1-x}", Kind::Block, false),
    ] {
        let node = Directive::parse(raw, kind);
        assert_eq!(node.raw(), raw);
        assert_eq!(node.target().is_some(), valid, "{raw}");
        assert_eq!(Directive::parse(node.raw(), kind), node);
    }
    let node = Directive::parse(
        "::file[ p#L10-20]{lines=2-4 chars=0:8 symbol=main}",
        Kind::Block,
    );
    assert_eq!(
        node.target(),
        Some(&Target {
            path: "p".into(),
            lines: Some(Lines { start: 2, end: 4 }),
            chars: Some(0..8),
            symbol: Some("main".into()),
        })
    );
}

#[test]
fn partial_directives_stay_provisional() {
    let mut stream = Stream::default();
    for chunk in ["prefix :", ":fi", "le[path#L", "2-4", "]"] {
        stream.push(chunk);
        assert!(stream.nodes().is_empty());
    }
    stream.push("{lines=1");
    assert!(stream.nodes().is_empty());
    stream.push("-2} suffix :file[x]");
    assert_eq!(stream.nodes().len(), 2);
    assert_eq!(
        stream.nodes()[0].directive.target().unwrap().lines,
        Some(Lines { start: 1, end: 2 })
    );
    let mut pending = Stream::default();
    pending.push("::file[path]");
    pending.finish();
    assert_eq!(pending.nodes().len(), 1);
}

fn parsed(source: &str, cuts: &[usize]) -> Stream {
    let mut stream = Stream::default();
    let mut previous = 0;
    for end in cuts.iter().copied().chain(std::iter::once(source.len())) {
        if end >= previous && source.is_char_boundary(end) {
            stream.push(&source[previous..end]);
            previous = end;
        }
    }
    stream.finish();
    stream
}

fn source() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            ".{0,20}",
            Just(":file[p#L2-L4]".to_owned()),
            Just("::file[p]{lines=1-2 chars=0:8}".to_owned()),
            Just("\\:file[escaped]".to_owned()),
            Just("::file[bad#L0]".to_owned()),
            Just("::file[p]{symbol=main}".to_owned()),
        ],
        0..20,
    )
    .prop_map(|parts| parts.concat())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn arbitrary_chunks_preserve_ast_and_raw_text(source in source(), cuts in prop::collection::vec(0usize..512, 0..40)) {
        let mut cuts = cuts;
        cuts.sort_unstable();
        let whole = parsed(&source, &[]);
        let streamed = parsed(&source, &cuts);
        prop_assert_eq!(streamed.nodes(), whole.nodes());
        prop_assert_eq!(streamed.source(), source.as_str());
        for node in streamed.nodes() {
            prop_assert_eq!(&source[node.range.clone()], node.directive.raw());
            prop_assert_eq!(Directive::parse(node.directive.raw(), node.directive.kind()), node.directive.clone());
        }
        prop_assert!(streamed.work <= 2 * source.len());
    }
}

#[test]
fn unfinished_candidates_and_replay_negative_control() {
    for count in [64, 128, 256, 1024] {
        for unit in ["::file[", "::file[p]{lines=", ":file[p] ", "!", "\\"] {
            let source = unit.repeat(count);
            let mut retained = Stream::default();
            let mut replay_work = 0;
            for end in 1..=source.len() {
                retained.push(&source[end - 1..end]);
                if count == 64 {
                    let mut replay = Stream::default();
                    replay.push(&source[..end]);
                    replay.finish();
                    replay_work += replay.work;
                }
            }
            retained.finish();
            assert!(retained.work <= 2 * source.len());
            if count == 64 {
                assert!(replay_work > 2 * source.len() + 1024);
            }
        }
    }
}

#[test]
fn losing_pending_escape_is_detected() {
    let mut stream = Stream::default();
    stream.push("\\");
    let mut broken = stream.clone();
    broken.escaped = false;
    stream.push(":file[x]");
    broken.push(":file[x]");
    stream.finish();
    broken.finish();
    assert!(stream.nodes().is_empty());
    assert_ne!(stream.nodes(), broken.nodes());
}
