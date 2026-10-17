use acyclic_harness::{
    OperationId,
    client::{
        CommandFileRoot, CommandSession, input_burst, scroll_tui, transcript_motion, tui_scroll,
    },
    task_volume::{Query, TaskVolume},
    tui::{
        Action, Editor, InputEffect, PromptInput, Screen, Viewport, WHEEL_ROWS,
        key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind},
        ratatui::{Terminal, backend::TestBackend, layout::Rect, text::Line},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Decoded input batches: `None` ends a batch (one Pending, then the next).
struct Batches {
    script: VecDeque<Option<InputEvent>>,
    polls: usize,
}
impl futures::Stream for Batches {
    type Item = acyclic_harness::Result<InputEvent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls += 1;
        match self.script.pop_front() {
            Some(Some(event)) => Poll::Ready(Some(Ok(event))),
            Some(None) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}

fn notch(up: bool) -> InputEvent {
    let kind = if up {
        MouseEventKind::ScrollUp
    } else {
        MouseEventKind::ScrollDown
    };
    InputEvent::Mouse(MouseEvent {
        kind,
        column: 3,
        row: 1,
        modifiers: KeyModifiers::NONE,
    })
}

/// Transcript rows per page in the driven layout.
const PAGE: usize = 8;

/// Drive the GraphCoder input step over `script`, returning frames drawn,
/// layout positions appended, stream polls and the final offset.
async fn drive(script: Vec<Option<InputEvent>>, coalesce: bool) -> (usize, u64, usize, usize) {
    use futures::StreamExt;
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("burst").unwrap(),
        )
        .await
        .unwrap(),
    );
    let identity = OperationId::from_bytes([12; 16]).to_string();
    let mut files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64)
        .await
        .unwrap();
    let mut input = PromptInput::new(Editor::new("draft"));
    let mut events = Batches {
        script: script.into(),
        polls: 0,
    };
    let before = volume
        .snapshot(&Query::Root, 64)
        .await
        .unwrap()
        .generation
        .position;
    let mut frames = 0;
    while let Some(event) = events.next().await {
        frames += 1;
        let mut route =
            |event| input.event(event, Rect::new(0, 9, 80, 1), &mut Viewport::default(), 1);
        let effects = if coalesce {
            input_burst(event.unwrap(), &mut events, PAGE, route).unwrap()
        } else {
            route(event.unwrap())
        };
        for effect in effects {
            if let Some(rows) = transcript_motion(&effect, PAGE) {
                scroll_tui(&mut files, rows, 10_000).await.unwrap();
            }
        }
    }
    let appended = volume
        .snapshot(&Query::Root, 64)
        .await
        .unwrap()
        .generation
        .position
        - before;
    (frames, appended, events.polls, tui_scroll(&files).unwrap())
}

#[test]
fn a_wheel_flick_or_held_page_key_is_one_scroll_and_one_frame_per_input_batch() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let key = |code| Some(InputEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
        let one = drive(vec![Some(notch(true))], true).await;
        let per_write = one.1;
        assert!(per_write > 0);
        assert_eq!(one, (1, per_write, 3, WHEEL_ROWS.unsigned_abs()));
        for motions in [32usize, 64] {
            // One batch: N motions (wheel notches, one reversed, and a
            // PageUp), then a typed key. Exactly one frame and one layout
            // write whatever N; N + 2 polls including the end of input.
            let mut script: Vec<_> = (0..motions)
                .map(|at| match at {
                    5 => Some(notch(false)),
                    7 => key(KeyCode::PageUp),
                    _ => Some(notch(true)),
                })
                .collect();
            script.push(key(KeyCode::Char('x')));
            let rows = WHEEL_ROWS.unsigned_abs() * (motions - 2) - WHEEL_ROWS.unsigned_abs() + PAGE;
            assert_eq!(
                drive(script.clone(), true).await,
                (1, per_write, motions + 2, rows)
            );
            // Negative control: per-event handling draws N + 1 frames and
            // writes the layout N times for the same final offset.
            let per_event = drive(script, false).await;
            assert_eq!(
                per_event,
                (motions + 1, per_write * motions as u64, motions + 2, rows)
            );
        }
        // A non-motion event ends the burst: later motions are a new frame.
        let script = vec![
            Some(notch(true)),
            key(KeyCode::Char('x')),
            Some(notch(true)),
        ];
        assert_eq!(drive(script, true).await, (2, 2 * per_write, 5, 6));
        // Batches are never merged across a read boundary: one frame each.
        let script = vec![
            Some(notch(true)),
            Some(notch(true)),
            None,
            Some(notch(true)),
        ];
        assert_eq!(drive(script, true).await, (2, 2 * per_write, 6, 9));
    });
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: page keys and wheel notches decode to bounded row motions that
    // preserve the prompt, paint the selected rows, survive reopen, and append
    // the same single layout write per change and none for a clamped motion.
    #[test]
    fn transcript_pages_and_wheel_notches_are_bounded_followed_files(sequence in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY), 1..16), height in 6u16..32) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("scroll").unwrap()).await.unwrap());
            let identity = OperationId::from_bytes([11;16]).to_string();
            let mut files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
            let lines: Vec<_> = (0..60).map(|row| Line::raw(format!("Transcript row {row:03}"))).collect();
            let screen = Screen { transcript: &lines, ..Screen::default() };
            let mut input = PromptInput::new(Editor::new("draft"));
            let area = Rect::new(0, 0, 80, height);
            let areas = screen.areas(area, &input);
            let page = usize::from(areas.transcript.height);
            let maximum = screen.tail_scroll(area, &input);
            let mut expected = 0usize;
            let mut per_write = None;
            for (up, wheel) in sequence {
                let (event, effect, step) = if wheel {
                    let kind = if up { MouseEventKind::ScrollUp } else { MouseEventKind::ScrollDown };
                    let mouse = MouseEvent { kind, column: 3, row: areas.transcript.y, modifiers: KeyModifiers::NONE };
                    (InputEvent::Mouse(mouse), InputEffect::Scroll(if up { WHEEL_ROWS } else { -WHEEL_ROWS }), WHEEL_ROWS.unsigned_abs())
                } else {
                    let key = KeyEvent::new(if up { KeyCode::PageUp } else { KeyCode::PageDown }, KeyModifiers::NONE);
                    (InputEvent::Key(key), InputEffect::Host(if up { Action::TranscriptUp } else { Action::TranscriptDown }), page)
                };
                let effects = input.event(event, areas.prompt, &mut Viewport::default(), 1);
                proptest::prop_assert_eq!(&effects, &vec![effect]);
                let rows = transcript_motion(&effects[0], page).unwrap();
                let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
                scroll_tui(&mut files, rows, maximum).await.unwrap();
                let previous = expected;
                expected = if up { expected.saturating_add(step).min(maximum) } else { expected.saturating_sub(step) };
                proptest::prop_assert_eq!(tui_scroll(&files).unwrap(), expected);
                let appended = volume.snapshot(&Query::Root, 64).await.unwrap().generation.position - before.position;
                if previous == expected {
                    proptest::prop_assert_eq!(appended, 0);
                } else {
                    proptest::prop_assert!(appended > 0);
                    proptest::prop_assert_eq!(*per_write.get_or_insert(appended), appended);
                }
                proptest::prop_assert_eq!(input.editor.text(), "draft");
                let mut terminal = Terminal::new(TestBackend::new(80, height)).unwrap();
                terminal.draw(|frame| { screen.render(frame, &input, &mut Viewport::default(), maximum - expected, 0); }).unwrap();
                proptest::prop_assert!(super::screen_text(&terminal).contains(&format!("Transcript row {:03}", maximum - expected)), "selected transcript row is visible");
                let reopened = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
                proptest::prop_assert_eq!(tui_scroll(&reopened).unwrap(), expected);
            }
            // Resize clamps stale offsets before the next movement.
            scroll_tui(&mut files, isize::MAX, 2).await.unwrap();
            proptest::prop_assert_eq!(tui_scroll(&files).unwrap(), 2);
            files.write_state(CommandFileRoot::Session, "layout.json", &serde_json::json!({"transcript_back":false})).await.unwrap();
            let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
            proptest::prop_assert!(scroll_tui(&mut files, WHEEL_ROWS, maximum).await.is_err());
            proptest::prop_assert_eq!(volume.snapshot(&Query::Root, 64).await.unwrap().generation, before);
            Ok(())
        })?;
    }
}
