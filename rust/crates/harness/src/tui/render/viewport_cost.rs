use super::*;
use crate::{
    client::{CommandSession, TaskFileClient},
    task_volume::{Channel, Component, FileKey, FileValue, TaskFile, TaskVolume},
    tui::{
        Editor, Layout, PromptInput, Top, Viewport, WrappedLines,
        ratatui::layout::Size,
        term::{Capabilities, FrameTerminal, RestoreGuard},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::{
    cell::RefCell,
    io::{self, Write},
    rc::Rc,
    sync::Arc,
};

#[derive(Clone, Default)]
struct Device(Rc<RefCell<Vec<u8>>>);
impl Write for Device {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        acyclic_test_support::differential::record(bytes.to_vec());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Fixture {
    _runtime: tokio::runtime::Runtime,
    files: CommandSession<MemoryStream>,
    renderer: ClientRenderer,
    input: PromptInput,
    device: Device,
    terminal: FrameTerminal<RestoreGuard<Device>>,
}

fn fixture(length: usize) -> Fixture {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (files, renderer) = runtime.block_on(async {
        let volume = Arc::new(
            TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("viewport-cost").unwrap(),
            )
            .await
            .unwrap(),
        );
        let task = crate::TaskId::from_bytes([51; 16]);
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: task.to_string(),
        };
        let mut chunk = FileValue::new(
            FileKey::Task {
                task: Component::new(task.to_string()).unwrap(),
                file: TaskFile::Chunk {
                    turn: 0,
                    sequence: 0,
                },
            },
            format!("```text\n{}```\n", "plain transcript row\n".repeat(length)).into_bytes(),
        );
        chunk.channel = Some(Channel::Text);
        volume
            .append(volume.head().await.unwrap(), &chunk)
            .await
            .unwrap();
        let status = FileValue::new(
            FileKey::parse(&format!(".tasks/{task}/status.json")).unwrap(),
            br#"{"state":"completed"}"#.to_vec(),
        );
        volume
            .append(volume.head().await.unwrap(), &status)
            .await
            .unwrap();
        let files = CommandSession::open(
            volume.clone(),
            uuid::Uuid::from_bytes([52; 16]),
            131072,
            128,
        )
        .await
        .unwrap();
        let client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 131072)
            .await
            .unwrap();
        let mut renderer = ClientRenderer::default();
        renderer.update_files(&client, &owner);
        renderer.configure(client.views(), &owner, &files).unwrap();
        assert!(!renderer.is_busy());
        (files, renderer)
    });
    let device = Device::default();
    let terminal = FrameTerminal::new(
        RestoreGuard::new(device.clone(), &Capabilities::default()),
        Size::new(80, 20),
    )
    .unwrap();
    Fixture {
        _runtime: runtime,
        files,
        renderer,
        input: PromptInput::default(),
        device,
        terminal,
    }
}

fn costs<const EAGER_CONTROL: bool>(length: usize) -> Vec<((usize, usize, usize), u64, u64)> {
    // Both sides open their own provider, followed views, parser, renderer,
    // terminal, journal and device. No candidate cache supplies the old source.
    let [old, new] = acyclic_test_support::differential::pair::<_, _, Vec<u8>>(
        || fixture(length),
        |mut fixture, old| {
            let mut costs = Vec::new();
            for width in [80, 40] {
                let size = Size::new(width, 20);
                let source = &fixture.renderer.transcript;
                assert!(
                    source
                        .iter()
                        .all(|line| line.width() < usize::from(width - 1))
                );
                let page = usize::from(
                    fixture
                        .renderer
                        .screen()
                        .areas(
                            super::super::ratatui::layout::Rect::new(0, 0, width, 20),
                            &fixture.input,
                        )
                        .transcript
                        .height,
                );
                let shown = &source[source.len().saturating_sub(page)..];
                let expected = (
                    shown.len(),
                    shown
                        .iter()
                        .flat_map(|line| &line.spans)
                        .map(|span| span.content.chars().count())
                        .sum::<usize>(),
                    shown.len(),
                );
                WrappedLines::take_work();
                let allocations = acyclic_test_support::measure(|| {
                    if old {
                        // The old eager path laid out every source line at the
                        // new width and retained every row. These ASCII fixture
                        // lines each fit one row; preserve their original styles.
                        let layouts = fixture
                            .renderer
                            .transcript
                            .iter()
                            .map(|line| {
                                let text: String = line
                                    .spans
                                    .iter()
                                    .map(|span| span.content.as_ref())
                                    .collect();
                                let layout =
                                    Layout::new(&Editor::new(&text), usize::from(width - 1));
                                assert_eq!(layout.rows.len(), 1);
                                layout
                            })
                            .collect::<Vec<_>>();
                        let rows = fixture.renderer.transcript.clone();
                        let total = rows
                            .iter()
                            .map(|line| {
                                line.spans
                                    .iter()
                                    .map(|span| span.content.len())
                                    .sum::<usize>()
                                    + 1
                            })
                            .sum::<usize>();
                        let start = rows
                            .iter()
                            .take(rows.len().saturating_sub(page))
                            .map(|line| {
                                line.spans
                                    .iter()
                                    .map(|span| span.content.len())
                                    .sum::<usize>()
                                    + 1
                            })
                            .sum::<usize>();
                        let mut screen = fixture.renderer.screen_for(&fixture.files).unwrap();
                        screen.transcript = &rows;
                        screen.scrollbar = Some((start..total, total));
                        fixture
                            .terminal
                            .draw(size, |frame| {
                                let top = screen.tail_scroll(frame.area(), &fixture.input);
                                screen.render(
                                    frame,
                                    &fixture.input,
                                    &mut Viewport::default(),
                                    top,
                                    0,
                                );
                            })
                            .unwrap();
                        std::hint::black_box(layouts);
                    } else {
                        fixture
                            .renderer
                            .layout(&fixture.files, &fixture.input, width, 20)
                            .unwrap();
                        if EAGER_CONTROL {
                            fixture.renderer.wrapped_transcript.wrap_all();
                            fixture.renderer.wrapped_transcript.view(Top::Tail, page);
                        }
                        let screen = fixture.renderer.screen_for(&fixture.files).unwrap();
                        fixture
                            .terminal
                            .draw(size, |frame| {
                                screen.render(
                                    frame,
                                    &fixture.input,
                                    &mut Viewport::default(),
                                    0,
                                    0,
                                );
                            })
                            .unwrap();
                    }
                });
                let work = WrappedLines::take_work();
                if !old {
                    assert_eq!(
                        work, expected,
                        "production wrapping work is exactly the visible rows"
                    );
                }
                costs.push((work, allocations.count_total, allocations.bytes_total));
            }
            let bytes = fixture.device.0.borrow().clone();
            (bytes, costs)
        },
    );
    assert_eq!(old.0.0, new.0.0, "full terminal output bytes");
    assert_eq!(old.1, new.1, "device operation and output observer trace");
    new.0.1
}

#[test]
fn production_first_frame_and_resize_have_exact_work_and_allocations_at_n_and_2n() {
    assert_eq!(
        costs::<false>(1000),
        costs::<false>(2000),
        "production frame work and allocations must be archive independent"
    );
}

#[test]
#[should_panic(expected = "production wrapping work is exactly the visible rows")]
fn negative_control_production_eager_reflow_fails_the_same_assertion() {
    costs::<true>(1000);
}
