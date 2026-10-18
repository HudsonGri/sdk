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
    let mut markdown = acyclic_markdown::Markdown::default();
    let mut streamed = Stream::default();
    let mut start = 0;
    for (index, _) in source.char_indices() {
        if index - start >= usize::from(stride).max(1) {
            streamed.push(&source[start..index]);
            markdown.push(&source[start..index]);
            start = index;
        }
    }
    streamed.push(&source[start..]);
    markdown.push(&source[start..]);
    streamed.finish();
    assert_eq!(whole.nodes(), streamed.nodes());
    let values = || {
        whole
            .nodes()
            .iter()
            .filter(|node| node.directive.kind() == acyclic_markdown::directive::Kind::Block)
            .filter_map(|node| {
                let target = node.directive.target()?;
                let resolution = match target.select(source) {
                    Ok(content) => acyclic_markdown::directive::Resolution::Content(content),
                    Err(error) => acyclic_markdown::directive::Resolution::Error(error),
                };
                Some((
                    node.range.clone(),
                    node.directive.raw().to_owned(),
                    resolution,
                ))
            })
            .collect::<Vec<_>>()
    };
    markdown.set_file_resolutions(values());
    let mut fresh = acyclic_markdown::Markdown::default();
    fresh.push(source);
    fresh.set_file_resolutions(values());
    assert_eq!(markdown.lines(), fresh.lines());
    for prefix in [
        ":file[unfinished\n",
        "::file[unfinished\r",
        ":file[unfinished[",
    ] {
        let mut recovered = Stream::default();
        recovered.push(prefix);
        recovered.push(source);
        recovered.finish();
        assert_eq!(recovered.nodes().len(), whole.nodes().len());
        for (actual, expected) in recovered.nodes().iter().zip(whole.nodes()) {
            assert_eq!(actual.directive, expected.directive);
            assert_eq!(actual.range.start, expected.range.start + prefix.len());
            assert_eq!(actual.range.end, expected.range.end + prefix.len());
        }
    }
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
