use super::*;
use proptest::prelude::*;

// Independent eager reference: old whole-transcript wrapping, initialized
// separately from the candidate's source table, layouts and row cache.
fn eager(source: &[Line<'_>], width: u16) -> (Vec<Line<'static>>, Vec<RowPosition>) {
    let mut rows = Vec::new();
    let mut positions = Vec::new();
    for (line, text) in source.iter().enumerate() {
        let text: String = text
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        let layout = Layout::new(&Editor::new(&text), usize::from(width));
        for (row, (_, wrapped)) in visible_rows(&layout).enumerate() {
            let text: String = wrapped
                .cells
                .iter()
                .map(|cell| cell.symbol.as_str())
                .collect();
            rows.push(Line::raw(text));
            positions.push(RowPosition { line, row });
        }
    }
    (rows, positions)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Property: lazy page navigation clamps exactly like ordered eager row
    // navigation, and appending at the tail never moves a retained anchor.
    #[test]
    fn source_anchors_match_the_eager_view(
        text in prop::collection::vec("[a-z日🙂 ]{0,48}", 1..40),
        width in 1u16..30,
        height in 1usize..12,
        motions in prop::collection::vec(-20isize..20, 1..20),
    ) {
        let mut source: Vec<_> = text.into_iter().map(Line::raw).collect();
        let (old, positions) = eager(&source, width);
        let maximum = old.len().saturating_sub(height);
        let mut expected = maximum;
        let mut anchor = None;
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, width);
        for motion in motions {
            expected = expected.saturating_add_signed(motion).min(maximum);
            anchor = candidate.move_by(anchor, motion, height);
            let top = anchor.map_or(Top::Tail, Top::At);
            let window = candidate.view(top, height);
            prop_assert_eq!(window.top, positions[expected]);
            let contents = |rows: &[Line<'_>]| rows.iter().map(|row| {
                row.spans.iter().map(|span| span.content.as_ref()).collect::<String>()
            }).collect::<Vec<_>>();
            prop_assert_eq!(contents(candidate.shown()), contents(&old[expected..(expected + height).min(old.len())]));
            prop_assert_eq!(anchor.is_none(), expected == maximum);
        }
        if let Some(anchor) = anchor {
            let before = candidate.shown().to_vec();
            let changed = source.len();
            source.push(Line::raw("appended output"));
            candidate.update(&source, changed, width);
            candidate.view(Top::At(anchor), height);
            prop_assert_eq!(candidate.shown(), before.as_slice());
        }
    }
}

fn visible_work(length: usize, eager_control: bool) -> (usize, usize, usize) {
    let source = vec![Line::raw("plain transcript row"); length];
    let mut candidate = WrappedLines::default();
    candidate.update(&source, 0, 80);
    WORK.with(|work| work.set((0, 0, 0)));
    if eager_control {
        candidate.wrap_all();
    } else {
        candidate.view(Top::Tail, 10);
    }
    WORK.with(|work| work.get())
}

#[test]
fn first_frame_and_resize_wrap_exactly_the_visible_rows() {
    for length in [1000, 2000] {
        assert_eq!(visible_work(length, false), (10, 200, 10));
        let source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, 80);
        candidate.view(Top::Tail, 10);
        candidate.update(&source, length, 40);
        WORK.with(|work| work.set((0, 0, 0)));
        candidate.view(Top::Tail, 10);
        assert_eq!(WORK.with(|work| work.get()), (10, 200, 10));
    }
}

#[test]
fn negative_control_eager_wrapping_grows_with_the_archive() {
    assert_eq!(visible_work(1000, true), (1000, 20000, 1000));
    assert_eq!(visible_work(2000, true), (2000, 40000, 2000));
}

fn assert_equal_frame_costs(eager_control: bool) {
    let mut costs = Vec::new();
    for length in [1000, 2000] {
        let source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, 80);
        for width in [80, 40] {
            candidate.update(&source, length, width);
            WORK.with(|work| work.set((0, 0, 0)));
            let allocations = acyclic_test_support::measure(|| {
                if eager_control {
                    candidate.wrap_all();
                } else {
                    candidate.view(Top::Tail, 10);
                }
            });
            costs.push((
                WORK.with(|work| work.get()),
                allocations.count_total,
                allocations.bytes_total,
            ));
            if !eager_control {
                WORK.with(|work| work.set((0, 0, 0)));
                let repeated = acyclic_test_support::measure(|| {
                    candidate.view(Top::Tail, 10);
                });
                assert_eq!(WORK.with(|work| work.get()), (0, 0, 0));
                assert_eq!((repeated.count_total, repeated.bytes_total), (0, 0));
            }
        }
    }
    assert_eq!(
        &costs[..2],
        &costs[2..],
        "frame work and allocations depend on the archive"
    );
}

#[test]
fn first_paint_and_resize_have_equal_work_and_allocations_at_n_and_2n() {
    assert_equal_frame_costs(false);
}

#[test]
#[should_panic(expected = "frame work and allocations depend on the archive")]
fn negative_control_eager_work_fails_the_same_cost_assertion() {
    assert_equal_frame_costs(true);
}

#[test]
fn an_anchored_wheel_step_wraps_only_the_three_new_rows() {
    let mut costs = Vec::new();
    for length in [1000, 2000] {
        let mut source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, 80);
        let from = RowPosition {
            line: length / 2,
            row: 0,
        };
        candidate.view(Top::At(from), 10);
        WORK.with(|work| work.set((0, 0, 0)));
        let allocations = acyclic_test_support::measure(|| {
            assert_eq!(
                candidate.move_by(Some(from), -3, 10),
                Some(RowPosition {
                    line: length / 2 - 3,
                    row: 0,
                })
            );
        });
        assert_eq!(WORK.with(|work| work.get()), (3, 60, 3));
        costs.push((allocations.count_total, allocations.bytes_total));
        let anchor = candidate.window().top;
        source.push(Line::raw("streamed output"));
        candidate.update(&source, length, 80);
        WORK.with(|work| work.set((0, 0, 0)));
        let unchanged = acyclic_test_support::measure(|| {
            candidate.view(Top::At(anchor), 10);
        });
        assert_eq!(WORK.with(|work| work.get()), (0, 0, 0));
        assert_eq!((unchanged.count_total, unchanged.bytes_total), (0, 0));
    }
    assert_eq!(costs[0], costs[1]);
}

#[test]
fn reflow_preserves_the_source_and_search_index() {
    let mut candidate = WrappedLines::default();
    candidate.replace(&[Line::raw("searchable source")], 0);
    candidate.reflow(80);
    let text = candidate.text().to_owned();
    let revision = candidate.index.revision;
    let measured = acyclic_test_support::measure(|| candidate.reflow(40));
    assert_eq!(candidate.text(), text);
    assert_eq!(candidate.index.revision, revision);
    assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
}
