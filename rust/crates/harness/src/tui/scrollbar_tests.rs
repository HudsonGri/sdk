use super::super::{
    PromptInput, Screen, Viewport,
    key_event::{InputEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind},
    ratatui::{Terminal, backend::TestBackend, text::Line},
};
use crate::client::{CommandFileRoot, CommandSession, file_tui_input, tui_scroll};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;
struct NoFiles;
impl crate::conversation::ContentResidencyVerifier for NoFiles {}

async fn scenario<const BOUND: bool>() {
    let volume = Arc::new(
        crate::task_volume::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("scrollbar").unwrap(),
        )
        .await
        .unwrap(),
    );
    let id = uuid::Uuid::from_bytes([27; 16]);
    let mut files = CommandSession::open(volume.clone(), id, 65_536, 128)
        .await
        .unwrap();
    let transcript: Vec<_> = (0..80)
        .map(|row| Line::raw(format!("message {row}")))
        .collect();
    let screen = Screen {
        transcript: &transcript,
        ..Screen::default()
    };
    let mut input = PromptInput::default();
    input.editor.set_text("unsent draft", 0);
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
    let maximum = screen.tail_scroll(terminal.get_frame().area(), &input);
    let mut areas = super::super::ScreenAreas::default();
    terminal
        .draw(|frame| {
            areas = screen.render(frame, &input, &mut viewport, maximum, 0);
        })
        .unwrap();
    assert!(
        terminal
            .backend()
            .buffer()
            .content
            .iter()
            .any(|cell| cell.symbol() == "█")
    );
    for (kind, row, expected) in [
        (
            MouseEventKind::Down(MouseButton::Left),
            areas.scrollbar.y,
            maximum,
        ),
        (
            MouseEventKind::Drag(MouseButton::Left),
            areas.scrollbar.bottom() - 1,
            0,
        ),
        (
            MouseEventKind::Drag(MouseButton::Left),
            areas.scrollbar.y,
            maximum,
        ),
    ] {
        let event = InputEvent::Mouse(MouseEvent {
            kind,
            column: areas.scrollbar.x,
            row,
            modifiers: KeyModifiers::NONE,
        });
        let effects = if BOUND {
            areas.event(&mut input, event, &mut viewport, 1)
        } else {
            input.event(
                event,
                crate::tui::PromptBox::input_area(areas.prompt),
                &mut viewport,
                1,
            )
        };
        assert_eq!(
            effects.len(),
            1,
            "scrollbar hit must produce one file-write intent"
        );
        for effect in effects {
            file_tui_input(
                &mut files,
                &NoFiles,
                effect,
                usize::from(areas.transcript.height),
                maximum,
            )
            .await
            .unwrap();
        }
        assert_eq!(tui_scroll(&files).unwrap(), expected);
        assert_eq!(input.editor.text(), "unsent draft");
        assert!(files.take_request().is_none());
        terminal
            .draw(|frame| {
                areas = screen.render(frame, &input, &mut viewport, maximum - expected, 2);
            })
            .unwrap();
    }
    let release = InputEvent::Mouse(MouseEvent {
        kind: MouseEventKind::Up(MouseButton::Left),
        column: areas.scrollbar.x,
        row: areas.scrollbar.y,
        modifiers: KeyModifiers::NONE,
    });
    assert!(
        areas
            .event(&mut input, release, &mut viewport, 3)
            .is_empty()
    );
    let drag = InputEvent::Mouse(MouseEvent {
        kind: MouseEventKind::Drag(MouseButton::Left),
        column: areas.scrollbar.x,
        row: areas.scrollbar.bottom() - 1,
        modifiers: KeyModifiers::NONE,
    });
    assert!(areas.event(&mut input, drag, &mut viewport, 4).is_empty());
    let reopened = CommandSession::open(volume, id, 65_536, 128).await.unwrap();
    assert_eq!(tui_scroll(&reopened).unwrap(), maximum);
    let text: String = terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect();
    assert!(text.contains("message 0"));
    assert!(!text.contains("message 79"));
    assert!(
        files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()["transcript_back"]
            .is_u64()
    );
}

#[test]
fn click_drag_and_replay_follow_the_layout_file() {
    futures::executor::block_on(scenario::<true>());
}

#[test]
#[should_panic(expected = "scrollbar hit must produce one file-write intent")]
fn missing_mouse_binding_is_detected_negative_control() {
    futures::executor::block_on(scenario::<false>());
}
