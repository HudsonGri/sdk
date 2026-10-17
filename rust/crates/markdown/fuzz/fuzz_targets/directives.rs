#![no_main]
use acyclic_markdown::directive::{Directive, Stream};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&stride, bytes)) = data.split_first() else {
        return;
    };
    let Ok(source) = std::str::from_utf8(bytes) else {
        return;
    };
    let mut whole = Stream::default();
    whole.push(source);
    whole.finish();
    let mut streamed = Stream::default();
    let mut start = 0;
    for (index, _) in source.char_indices() {
        if index - start >= usize::from(stride).max(1) {
            streamed.push(&source[start..index]);
            start = index;
        }
    }
    streamed.push(&source[start..]);
    streamed.finish();
    assert_eq!(whole.nodes(), streamed.nodes());
    for node in streamed.nodes() {
        assert_eq!(&source[node.range.clone()], node.directive.raw());
        assert_eq!(
            Directive::parse(node.directive.raw(), node.directive.kind()),
            node.directive
        );
        if let Some(target) = node.directive.target() {
            if let Ok(excerpt) = target.select(source) {
                assert!(excerpt.content.len() <= source.len());
            }
        }
    }
});
