use super::*;
use acyclic_harness_tui::term::ScreenMode;

fn check_contents<const KEEP_CELLS: bool>(during_render: bool, alternate: bool) {
    let scene: Scene = (vec!["Z".into()], Some((3, 2)));
    let expected = per_command(&[scene.clone()]).unwrap().concat().concat();
    let device = Device::default();
    let mut profile = caps(true);
    profile.modes.insert(1049, ModeState::Reset);
    let guard = Rc::new(RefCell::new(RestoreGuard::new(device.clone(), &profile)));
    if alternate {
        guard.borrow_mut().setup(ScreenMode::Alternate).unwrap();
    }
    let size = Size::new(WIDTH, HEIGHT);
    let mut terminal = FrameTerminal::new(Shared(Rc::clone(&guard), true), size).unwrap();
    terminal.draw(size, |frame| paint(frame, &scene)).unwrap();
    let mut bytes = device.take().concat();
    let outside = || {
        let mut guard = guard.borrow_mut();
        if alternate {
            guard.restore().unwrap();
            guard.setup(ScreenMode::Alternate).unwrap();
        } else {
            guard.writer().write_all(b"\x1b[Hforeign").unwrap();
        }
    };
    if !during_render {
        outside();
    }
    terminal
        .draw(size, |frame| {
            paint(frame, &scene);
            if during_render {
                outside();
            }
        })
        .unwrap();
    let mut recovered = device.take().concat();
    if !KEEP_CELLS {
        // Preserve real outside writes, checked presentation, attributes and
        // cursor recovery; omit only the retained cell's recovery paint.
        let at = recovered.iter().rposition(|&byte| byte == b'Z').unwrap();
        recovered.remove(at);
    }
    bytes.extend(recovered);
    assert_eq!(
        screen(&bytes),
        screen(&expected),
        "outside output must not invalidate retained screen contents"
    );
    terminal.draw(size, |frame| paint(frame, &scene)).unwrap();
    assert!(
        device.take().is_empty(),
        "successful recovery retains the screen baseline"
    );
}

#[test]
fn outside_writes_and_alternate_reentry_repaint_retained_cells() {
    for during_render in [false, true] {
        for alternate in [false, true] {
            check_contents::<true>(during_render, alternate);
        }
    }
}

#[test]
#[should_panic(expected = "outside output must not invalidate retained screen contents")]
fn negative_control_omitting_retained_cell_recovery_is_detected() {
    check_contents::<false>(true, true);
}
