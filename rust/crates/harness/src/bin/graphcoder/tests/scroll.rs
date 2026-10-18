use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, scroll_tui, tui_anchor},
    task_volume::{Query, TaskVolume},
    tui::{
        Action, ClientRenderer, Editor, InputEffect, PromptInput, RowPosition, TranscriptAnchor,
        TranscriptPart, Viewport,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
        ratatui::{Terminal, backend::TestBackend, layout::Rect},
    },
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Source navigation has the same cells and ordered clamp behaviour as an
    // independently initialized eager Screen, survives reopen, and never writes
    // for a zero move. The prompt remains untouched.
    #[test]
    fn transcript_pages_are_bounded_followed_files(
        sequence in prop::collection::vec(any::<bool>(), 1..12), height in 6u16..32,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("scroll").unwrap()).await.unwrap());
            let identity = OperationId::from_bytes([11;16]).to_string();
            let mut files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id:"task".into() };
            let text = format!("```text\n{}```\n", (0..60).map(|row|format!("Transcript row {row:03}\n")).collect::<String>());
            let mut views = super::file_views::empty();
            super::file_views::append_text(&mut views, &owner.id, false, &text);
            let mut renderer = ClientRenderer::default();
            renderer.update(&views, &owner);
            let mut baseline_views = super::file_views::empty();
            super::file_views::append_text(&mut baseline_views, &owner.id, false, &text);
            let mut baseline = ClientRenderer::default();
            baseline.update(&baseline_views, &owner);
            // These short code rows were the eager renderer's unchanged source.
            // They require one physical row each; no candidate wrap cache is used.
            let mut reference = baseline.screen();
            reference.scrollbar = Some((0..0, 0));
            let source_lengths: Vec<_> = reference.transcript.iter().map(|row| row.spans.iter().map(|span|span.content.len()).sum::<usize>()).collect();
            let prefix: Vec<_> = std::iter::once(0).chain(source_lengths.iter().scan(0,|sum,length| { *sum += length + 1; Some(*sum) })).collect();
            let total = prefix.last().copied().unwrap() + usize::from(baseline.is_busy());
            assert!(reference.transcript.iter().all(|row| row.width() <= 79));
            let mut input = PromptInput::new(Editor::new("draft"));
            input.busy = renderer.is_busy();
            let mut reference_input = PromptInput::new(Editor::new("draft"));
            reference_input.busy = baseline.is_busy();
            let area = Rect::new(0, 0, 80, height);
            let page = usize::from(reference.areas(area, &reference_input).transcript.height);
            let maximum = reference.tail_scroll(area, &reference_input);
            let mut expected = maximum;
            for up in sequence {
                renderer.layout(&files, &input, 80, height).unwrap();
                let key = KeyEvent::new(if up { KeyCode::PageUp } else { KeyCode::PageDown }, KeyModifiers::NONE);
                let effects = input.key(key, 1, 78);
                prop_assert_eq!(effects, vec![InputEffect::Host(if up { Action::TranscriptUp } else { Action::TranscriptDown })]);
                scroll_tui(&mut files, up, page, 0, |from, rows| renderer.move_view(from, rows)).await.unwrap();
                expected = expected.saturating_add_signed(if up { -(page as isize) } else { page as isize }).min(maximum);
                let expected_anchor = (expected < maximum).then_some(TranscriptAnchor { part:TranscriptPart::Answer, position:RowPosition{line:expected,row:0} });
                prop_assert_eq!(tui_anchor(&files).unwrap(), expected_anchor);
                prop_assert_eq!(input.editor.text(), "draft");
                renderer.layout(&files, &input, 80, height).unwrap();
                let screen = renderer.screen_for(&files).unwrap();
                let mut actual = Terminal::new(TestBackend::new(80, height)).unwrap();
                let end = (expected + page).min(source_lengths.len());
                let visible_end = if expected == maximum { total } else if end > expected {
                    prefix[end - 1] + source_lengths[end - 1].max(1)
                } else { prefix[expected.min(source_lengths.len())] };
                reference.scrollbar = Some((prefix[expected.min(source_lengths.len())]..visible_end, total));
                let mut old = Terminal::new(TestBackend::new(80, height)).unwrap();
                actual.draw(|frame| { screen.render(frame, &input, &mut Viewport::default(), 0, 0); }).unwrap();
                old.draw(|frame| { reference.render(frame, &reference_input, &mut Viewport::default(), expected, 0); }).unwrap();
                prop_assert_eq!(actual.backend().buffer(), old.backend().buffer());
                let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
                scroll_tui(&mut files, up, 0, 0, |from, rows| renderer.move_view(from, rows)).await.unwrap();
                prop_assert_eq!(volume.snapshot(&Query::Root, 64).await.unwrap().generation, before);
                let reopened = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
                prop_assert_eq!(tui_anchor(&reopened).unwrap(), expected_anchor);
            }
            renderer.layout(&files, &input, 80, 32).unwrap();
            scroll_tui(&mut files, true, usize::MAX, 0, |from, rows| renderer.move_view(from, rows)).await.unwrap();
            prop_assert_eq!(tui_anchor(&files).unwrap().unwrap().position, RowPosition::default());
            files.write_state(CommandFileRoot::Session, "layout.json", &serde_json::json!({"transcript_top":false})).await.unwrap();
            let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
            prop_assert!(scroll_tui(&mut files, true, page, 0, |from, rows| renderer.move_view(from, rows)).await.is_err());
            prop_assert_eq!(volume.snapshot(&Query::Root, 64).await.unwrap().generation, before);
            Ok(())
        })?;
    }
}
