use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn tokenization_is_independent_of_chunk_splits(
        characters in prop::collection::vec(prop_oneof![
            any::<char>(),
            prop::sample::select("\\!*_~`&;[]<>\"'()\n#abc123".chars().collect::<Vec<_>>()),
        ], 0..256),
    ) {
        let mut prefix = String::new();
        let mut streamed = Tokens::default();
        let mut used = 0;
        for character in characters {
            prefix.push(character);
            let before = crate::work::current();
            streamed.push(&prefix);
            used += crate::work::current() - before;
            if let Some(token) = streamed.preview() {
                prop_assert!(!streamed.neutral());
                prop_assert_eq!(token.range.end, prefix.len());
            } else {
                prop_assert!(streamed.neutral());
            }
            let mut full = Tokens::default();
            full.push(&prefix);
            prop_assert_eq!(&streamed.ready, &full.ready);
            prop_assert_eq!(streamed.preview(), full.preview());
        }
        let token_bytes = streamed.ready.len() * std::mem::size_of::<Token>();
        prop_assert!(used <= prefix.len() + 4 * token_bytes);
        let mut full = Tokens::default();
        full.push(&prefix);
        prop_assert_eq!(&streamed.ready, &full.ready);
        prop_assert_eq!(streamed.preview(), full.preview());
        let mut opener = 0;
        for (ordinal, token) in full.ready.iter().enumerate() {
            if ordinal < full.ordinal_origin || !full.indexed { continue; }
            prop_assert_eq!(full.ordinal_at(token.range.start), Some(ordinal));
            prop_assert_eq!(full.after_absolute(token.range.end), Some(ordinal + 1));
            if matches!(token.kind, Kind::Open { .. } | Kind::AngleOpen) {
                prop_assert_eq!(full.anchor_at(token.range.start), Some(opener));
                opener += 1;
            } else {
                prop_assert_eq!(full.anchor_at(token.range.start), None);
            }
        }
        let mut next = 0;
        for token in full.ready.iter().cloned().chain(full.preview()) {
            prop_assert_eq!(token.range.start, next);
            prop_assert!(prefix.is_char_boundary(token.range.end));
            next = token.range.end;
        }
        prop_assert_eq!(next, prefix.len());
    }
}

#[test]
#[should_panic(expected = "EOF changed tokenization")]
fn token_property_detects_finalizing_eof() {
    let mut streamed = Tokens::default();
    streamed.push("\\");
    // Deliberately commit the provisional EOF token, as the old checkpoint did.
    let token = streamed.preview().expect("pending escape");
    streamed.pending = Pending::None;
    streamed.emit(token.range, token.kind);
    streamed.push("\\*");
    let mut full = Tokens::default();
    full.push("\\*");
    assert!(
        streamed.ready == full.ready && streamed.preview() == full.preview(),
        "EOF changed tokenization"
    );
}
