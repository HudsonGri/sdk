use std::cell::Cell;
thread_local! {
    pub(crate) static EAGER_RENDER_PAGE: Cell<bool> = const { Cell::new(false) };
    static SOURCE_GROWTH: Cell<usize> = const { Cell::new(0) };
    static SCAN: Cell<usize> = const { Cell::new(0) };
    static COPY: Cell<usize> = const { Cell::new(0) };
    static VISIBILITY: Cell<usize> = const { Cell::new(0) };
    static RENDER_FEED: Cell<usize> = const { Cell::new(0) };
    static RENDER_WORK: Cell<usize> = const { Cell::new(0) };
    static RENDER_PARTS: Cell<[usize; 4]> = const { Cell::new([0; 4]) };
    static RENDERING: Cell<bool> = const { Cell::new(false) };
}
thread_local! { pub(crate) static TRACE: std::cell::RefCell<Option<std::collections::BTreeMap<String, usize>>> = const { std::cell::RefCell::new(None) }; }
#[track_caller]
pub(crate) fn copy(bytes: usize) {
    let location = std::panic::Location::caller();
    TRACE.with_borrow_mut(|trace| if let Some(trace) = trace { *trace.entry(format!("copy {}:{}", location.file(), location.line())).or_default() += bytes; });
    COPY.set(COPY.get() + bytes);
    render_work(0, bytes);
}
pub(crate) fn visibility(count: usize) {
    VISIBILITY.set(VISIBILITY.get() + count);
    render_work(1, count);
}
pub(crate) fn visibility_current() -> usize {
    VISIBILITY.get()
}
pub(crate) fn parts() -> [usize; 5] {
    [
        SCAN.get(),
        COPY.get(),
        VISIBILITY.get(),
        RENDER_FEED.get(),
        crate::references::PARSED_BYTES.get(),
    ]
}

pub(crate) fn current() -> usize {
    parts().into_iter().sum()
}

pub(crate) fn render_feed(bytes: usize) {
    RENDER_FEED.set(RENDER_FEED.get() + bytes);
    render_work(2, bytes);
}

pub(crate) fn rendered_bytes() -> usize {
    RENDER_FEED.get()
}

fn render_work(part: usize, work: usize) {
    if RENDERING.get() {
        RENDER_WORK.set(RENDER_WORK.get() + work);
        let mut parts = RENDER_PARTS.get();
        parts[part] += work;
        RENDER_PARTS.set(parts);
    }
}

pub(crate) fn rendering(active: bool) -> bool {
    RENDERING.replace(active)
}

pub(crate) fn rendering_parts() -> [usize; 4] {
    RENDER_PARTS.get()
}

pub(crate) fn rendering_work() -> usize {
    RENDER_WORK.get()
}

pub(crate) fn event(event: &pulldown_cmark::Event<'_>) {
    use pulldown_cmark::{CodeBlockKind, Event, Tag};
    copy(match event {
        Event::Text(text)
        | Event::Code(text)
        | Event::Html(text)
        | Event::InlineHtml(text)
        | Event::InlineMath(text)
        | Event::DisplayMath(text)
        | Event::FootnoteReference(text) => text.len(),
        Event::Start(
            Tag::Link {
                dest_url,
                title,
                id,
                ..
            }
            | Tag::Image {
                dest_url,
                title,
                id,
                ..
            },
        ) => dest_url.len() + title.len() + id.len(),
        Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(info))) => info.len(),
        _ => 0,
    });
}

pub(crate) fn source_growth() -> usize {
    SOURCE_GROWTH.get()
}
pub(crate) fn grow_source(bytes: usize) {
    copy(bytes);
    SOURCE_GROWTH.set(SOURCE_GROWTH.get() + bytes);
}

#[track_caller]
pub(crate) fn scan(bytes: usize) {
    let location = std::panic::Location::caller();
    TRACE.with_borrow_mut(|trace| if let Some(trace) = trace { *trace.entry(format!("scan {}:{}", location.file(), location.line())).or_default() += bytes; });
    SCAN.set(SCAN.get() + bytes);
    render_work(3, bytes);
}
