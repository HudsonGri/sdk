//! Region scrolls: a band of rows that moved is scrolled on the device in
//! place (DECSTBM + SU/SD) inside the frame, and only rows that still differ
//! are repainted. The screen is always the one the per-command path shows.
#![cfg(feature = "tui")]

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{self, Write};
use std::rc::Rc;

use acyclic_harness_tui::ratatui::{
    Frame, Terminal, TerminalOptions, Viewport,
    backend::CrosstermBackend,
    layout::{Rect, Size},
    text::Line,
    widgets::Paragraph,
};
use acyclic_harness_tui::term::{Capabilities, FrameTerminal, RegionScroll, RestoreGuard};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line as Row, Point};
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use proptest::prelude::*;

const WIDTH: u16 = 24;
const HEIGHT: u16 = 8;

#[derive(Clone, Default)]
struct Device(Rc<RefCell<Vec<u8>>>);
impl Write for Device {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Device {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}

/// Rows of text and the cursor.
type Scene = (Vec<String>, Option<(u16, u16)>);

fn paint(frame: &mut Frame<'_>, (rows, cursor): &Scene) {
    let lines: Vec<Line<'_>> = rows.iter().map(|row| Line::raw(row.as_str())).collect();
    frame.render_widget(Paragraph::new(lines), frame.area());
    if let Some(position) = cursor {
        frame.set_cursor_position(*position);
    }
}

/// The independent baseline: ratatui writing each command to the device.
fn per_command(scenes: &[Scene]) -> Vec<Vec<u8>> {
    let device = Device::default();
    let mut terminal = Terminal::with_options(
        CrosstermBackend::new(device.clone()),
        TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, WIDTH, HEIGHT)),
        },
    )
    .unwrap();
    scenes
        .iter()
        .map(|scene| {
            terminal.draw(|frame| paint(frame, scene)).unwrap();
            device.take()
        })
        .collect()
}

fn scrolled(steps: &[(Scene, Option<RegionScroll>)], size: Size) -> Vec<Vec<u8>> {
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &Capabilities::default());
    let mut terminal = FrameTerminal::new(guard, size).unwrap();
    steps
        .iter()
        .map(|(scene, moved)| {
            terminal
                .draw_scrolled(size, *moved, |frame| paint(frame, scene))
                .unwrap();
            device.take()
        })
        .collect()
}

#[derive(Clone, Copy)]
struct Grid;
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        usize::from(HEIGHT)
    }
    fn screen_lines(&self) -> usize {
        usize::from(HEIGHT)
    }
    fn columns(&self) -> usize {
        usize::from(WIDTH)
    }
}
#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}

const DEFAULT_COLOURS: &str = "Named(Foreground)Named(Background)";

/// Emulated screens after each frame: text, colours and a shown cursor.
fn screens(frames: &[Vec<u8>]) -> Vec<String> {
    let mut term = Term::new(Config::default(), &Grid, Events);
    let mut parser: Processor = Processor::new();
    frames
        .iter()
        .map(|bytes| {
            parser.advance(&mut term, bytes);
            let mut text = String::new();
            for row in 0..usize::from(HEIGHT) {
                for column in 0..usize::from(WIDTH) {
                    let cell = &term.grid()[Point::new(Row(row as i32), Column(column))];
                    text.push(cell.c);
                    if format!("{:?}{:?}", cell.fg, cell.bg) != DEFAULT_COLOURS {
                        text.push_str(&format!("{:?}{:?}", cell.fg, cell.bg));
                    }
                }
                text.push('\n');
            }
            // A hidden cursor's position is not observable.
            if term.mode().contains(TermMode::SHOW_CURSOR) {
                let cursor = term.grid().cursor.point;
                text.push_str(&format!("{cursor:?}"));
            }
            text
        })
        .collect()
}

/// Lines `top..top + height` of a long transcript.
fn window(top: usize, height: u16) -> Scene {
    let rows = (top..top + usize::from(height))
        .map(|line| format!("transcript line {line:04}"))
        .collect();
    (rows, Some((2, height - 1)))
}

fn hint() -> impl Strategy<Value = Option<RegionScroll>> {
    prop::option::of((0u16..2, 0..HEIGHT, 0..=HEIGHT, -10isize..10).prop_map(
        |(x, y, height, rows)| RegionScroll {
            region: Rect::new(x, y, WIDTH - x, height),
            rows,
        },
    ))
}

fn steps() -> impl Strategy<Value = Vec<(Scene, Option<RegionScroll>)>> {
    let scene = prop_oneof![
        (0usize..40).prop_map(|top| window(top, HEIGHT)),
        (
            prop::collection::vec("[a-z日 ]{0,30}", 0..=usize::from(HEIGHT)),
            prop::option::of((0..WIDTH, 0..HEIGHT)),
        ),
    ];
    prop::collection::vec((scene, hint()), 1..10)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: any hint, right, wrong or out of bounds, leaves exactly the
    // screen the per-command path shows after every frame.
    #[test]
    fn every_hint_shows_the_per_command_screen(steps in steps()) {
        let scenes: Vec<Scene> = steps.iter().map(|(scene, _)| scene.clone()).collect();
        let old = screens(&per_command(&scenes));
        let new = screens(&scrolled(&steps, Size::new(WIDTH, HEIGHT)));
        prop_assert_eq!(new, old);
    }
}

/// Rows a frame addresses with cursor moves (`CSI row;col H`).
fn rows_addressed(bytes: &[u8]) -> BTreeSet<u16> {
    String::from_utf8_lossy(bytes)
        .split("\x1b[")
        .filter_map(|sequence| {
            let (position, _) = sequence.split_once('H')?;
            let (row, column) = position.split_once(';')?;
            column.parse::<u16>().ok()?;
            row.parse().ok()
        })
        .collect()
}

/// The last frame of scrolling a full-screen transcript by `rows` lines.
fn scroll_frame(height: u16, rows: isize, hinted: bool) -> Vec<u8> {
    let region = Rect::new(0, 0, WIDTH, height);
    let to = usize::try_from(20 + rows).unwrap();
    let steps = [
        (window(20, height), None),
        (
            window(to, height),
            hinted.then_some(RegionScroll { region, rows }),
        ),
    ];
    scrolled(&steps, Size::new(WIDTH, height)).remove(1)
}

#[test]
fn a_scrolled_band_repaints_only_the_rows_it_exposes() {
    for height in [HEIGHT, 2 * HEIGHT] {
        let down = scroll_frame(height, 3, true);
        let scroll = format!("\x1b[1;{height}r\x1b[3S\x1b[r");
        assert!(down.starts_with(scroll.as_bytes()), "{down:?}");
        // The three exposed rows, whatever the height; the cursor row is
        // among them.
        let exposed: BTreeSet<u16> = (height - 2..=height).collect();
        assert_eq!(rows_addressed(&down), exposed);
        let up = scroll_frame(height, -2, true);
        let scroll = format!("\x1b[1;{height}r\x1b[2T\x1b[r");
        assert!(up.starts_with(scroll.as_bytes()), "{up:?}");
        assert_eq!(rows_addressed(&up), BTreeSet::from([1, 2, height]));
    }
}

#[test]
fn negative_control_without_a_hint_every_row_is_repainted() {
    for height in [HEIGHT, 2 * HEIGHT] {
        let rows = rows_addressed(&scroll_frame(height, 3, false));
        assert_eq!(rows, (1..=height).collect());
    }
}

#[test]
fn a_band_moved_by_its_height_or_not_full_width_is_repainted() {
    let size = Size::new(WIDTH, HEIGHT);
    let full = Rect::new(0, 0, WIDTH, HEIGHT);
    for moved in [
        RegionScroll {
            region: full,
            rows: isize::try_from(HEIGHT).unwrap(),
        },
        RegionScroll {
            region: Rect::new(1, 0, WIDTH - 1, HEIGHT),
            rows: 1,
        },
        RegionScroll {
            region: Rect::new(0, 4, WIDTH, HEIGHT),
            rows: 1,
        },
    ] {
        let steps = [(window(0, HEIGHT), None), (window(1, HEIGHT), Some(moved))];
        let frame = scrolled(&steps, size).remove(1);
        assert!(
            !frame.windows(3).any(|bytes| bytes == b"\x1b[r"),
            "{moved:?}"
        );
    }
    // A resize clears the device: the hint is not applied to the old frame.
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &Capabilities::default());
    let mut terminal = FrameTerminal::new(guard, size).unwrap();
    terminal
        .draw(size, |frame| paint(frame, &window(0, HEIGHT)))
        .unwrap();
    let larger = Size::new(WIDTH, HEIGHT + 2);
    let moved = RegionScroll {
        region: Rect::new(0, 0, WIDTH, HEIGHT + 2),
        rows: 1,
    };
    device.take();
    terminal
        .draw_scrolled(larger, Some(moved), |frame| {
            paint(frame, &window(1, HEIGHT + 2))
        })
        .unwrap();
    assert!(!device.take().windows(3).any(|bytes| bytes == b"\x1b[r"));
}

/// A device that fails one write at byte `at`, after writing the prefix.
struct FailAt {
    bytes: Vec<u8>,
    at: usize,
    failed: bool,
}
impl Write for FailAt {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.failed && self.bytes.len() + bytes.len() > self.at {
            self.failed = true;
            let keep = self.at - self.bytes.len();
            self.bytes.extend_from_slice(&bytes[..keep]);
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Whether the screen scrolls as a whole after `bytes`: a line at the top
/// leaves the screen when a line feed is written on the bottom row, unless
/// stuck margins confine scrolling to a band that excludes both.
fn scrolls_whole_screen(bytes: &[u8]) -> bool {
    let probe = format!("\x1b[1;1HZ\x1b[{HEIGHT};1H\n");
    let screen = screens(&[bytes.to_vec(), probe.into_bytes()]).remove(1);
    !screen.starts_with('Z')
}

#[test]
fn a_region_scroll_cut_short_at_any_byte_restores_full_margins() {
    let size = Size::new(WIDTH, HEIGHT);
    let moved = RegionScroll {
        region: Rect::new(0, 2, WIDTH, 4),
        rows: 1,
    };
    let mut cut = 0;
    for at in 0.. {
        let mut device = FailAt {
            bytes: Vec::new(),
            at,
            failed: false,
        };
        let mut guard = RestoreGuard::new(&mut device, &Capabilities::default());
        {
            let mut terminal = FrameTerminal::new(&mut guard, size).unwrap();
            // Either frame may be the one cut short.
            let _ = terminal.draw(size, |frame| paint(frame, &window(0, HEIGHT)));
            let _ =
                terminal.draw_scrolled(size, Some(moved), |frame| paint(frame, &window(1, HEIGHT)));
        }
        let _ = guard.restore();
        drop(guard);
        assert!(scrolls_whole_screen(&device.bytes), "failure at byte {at}");
        if !device.failed {
            break;
        }
        cut += 1;
    }
    // Every byte of both frames was a failure point, the margins included.
    assert!(cut > 40, "{cut}");
    // Negative control: margins set and never reset confine the scroll.
    assert!(!scrolls_whole_screen(b"\x1b[3;6r"));
}
