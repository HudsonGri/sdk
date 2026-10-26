//! Solver harness for the production agent control kernel.
//!
//! Run: `kani tests/formal/agent_control.rs`. The model drives the kernel through
//! every sequence of `STEPS` wakes (any record class or the alarm), any turn
//! driver result and any queue/approval/children shape, and checks:
//! - `ChildDone` is sent at most once, and exactly once after archive drains;
//! - no turn step runs after completion or archive;
//! - no lost wake-up: whenever work is runnable without new input, an alarm is set;
//! - every alarm is a real deadline (at least `CONTINUE_NOW`).

#[path = "../../src/agent/kernel.rs"]
mod production;

use production::{
    Admit, CONTINUE_NOW, Control, MAX_FANOUT, Mode, Pick, Wait, admit, alarm, cascade, pick,
    settle,
};

const STEPS: usize = 6;
const MAX_CHILDREN: usize = 2 * MAX_FANOUT + 1;

#[derive(Clone, Copy)]
pub struct Model {
    pub control: Control,
    pub queued: u8,
    pub approval_queued: bool,
    pub pending: usize,
    pub reports: u8,
}

fn any_wait() -> Wait {
    match kani::any::<u8>() % 5 {
        0 => Wait::Continue,
        1 => Wait::Approval,
        2 => Wait::Until(kani::any()),
        3 => Wait::Input,
        _ => Wait::Done,
    }
}

/// One step of the shell in `src/agent/machine.rs`, over counts instead of data.
pub fn step(model: &mut Model, wake: Option<Admit>, open_children: usize) {
    if let Some(kind) = wake {
        let admitted = admit(model.control, kind);
        model.control = admitted.control;
        if admitted.clear_queue {
            model.queued = 0;
            model.approval_queued = false;
        }
        if kind == Admit::Archive {
            model.pending = open_children;
        }
        if admitted.enqueue && model.queued < 3 {
            model.queued += 1;
            model.approval_queued |= kani::any::<bool>();
        }
    }
    if model.control.closed {
        let step = cascade(model.control, model.pending);
        assert!(step.archive <= MAX_FANOUT && step.archive <= model.pending);
        model.pending -= step.archive;
        model.control = step.control;
        model.reports += u8::from(step.report);
        return;
    }
    let picked = pick(
        model.control,
        wake.is_none(),
        model.queued > 0,
        model.approval_queued,
    );
    if matches!(model.control.mode, Mode::Done) {
        assert_eq!(picked, Pick::Nothing);
    }
    match picked {
        Pick::Nothing => return,
        Pick::Resume => {}
        Pick::Front => model.queued -= 1,
        Pick::Approval => {
            model.queued -= 1;
            model.approval_queued = false;
        }
    }
    let (next, send) = settle(model.control, any_wait());
    model.control = next;
    model.reports += u8::from(send);
    if matches!(next.mode, Mode::Done) {
        model.queued = 0;
        model.approval_queued = false;
    }
    if matches!(next.mode, Mode::Approval) {
        // A new request: an earlier queued response may or may not match it.
        model.approval_queued = model.queued > 0 && kani::any::<bool>();
    }
}

fn any_wake() -> Option<Admit> {
    match kani::any::<u8>() % 4 {
        0 => Some(Admit::Input),
        1 => Some(Admit::Cancel),
        2 => Some(Admit::Archive),
        _ => None,
    }
}

fn check_alarm(model: &Model) {
    let set = alarm(
        model.control,
        model.queued > 0,
        model.approval_queued,
        model.pending,
    );
    if let Some(deadline) = set {
        assert!(deadline >= CONTINUE_NOW);
    }
    let control = model.control;
    let runnable = if control.closed {
        model.pending > 0
    } else {
        match control.mode {
            Mode::Done => false,
            Mode::Approval => control.turn && model.approval_queued,
            Mode::Running | Mode::Alarm(_) => control.turn,
            Mode::Idle => model.queued > 0,
        }
    };
    assert_eq!(set.is_some(), runnable, "lost or spurious wake-up");
    if control.closed || matches!(control.mode, Mode::Done) {
        assert!(!control.turn);
    }
}

#[kani::proof]
#[kani::unwind(8)]
fn agent_control_is_exactly_once_and_never_loses_a_wake() {
    let open_children: usize = kani::any();
    kani::assume(open_children <= MAX_CHILDREN);
    let mut model = Model {
        control: Control::ADOPTED,
        queued: kani::any::<u8>() % 2,
        approval_queued: false,
        pending: 0,
        reports: 0,
    };
    for _ in 0..STEPS {
        let closed_before = model.control.closed;
        step(&mut model, any_wake(), open_children);
        assert!(model.reports <= 1, "ChildDone sent twice");
        if closed_before {
            assert!(model.control.closed);
        }
        check_alarm(&model);
    }
}

#[kani::proof]
#[kani::unwind(8)]
fn archive_cascade_drains_and_reports_exactly_once() {
    let open_children: usize = kani::any();
    kani::assume(open_children <= MAX_CHILDREN);
    let reported: bool = kani::any();
    let mut model = Model {
        control: Control {
            reported,
            ..Control::ADOPTED
        },
        queued: 0,
        approval_queued: false,
        pending: 0,
        reports: 0,
    };
    step(&mut model, Some(Admit::Archive), open_children);
    // Only the alarm wakes an archived agent with no further input.
    for _ in 0..(MAX_CHILDREN / MAX_FANOUT + 1) {
        if alarm(model.control, false, false, model.pending).is_none() {
            break;
        }
        step(&mut model, None, open_children);
    }
    assert_eq!(model.pending, 0);
    assert!(model.control.reported);
    assert_eq!(model.reports, u8::from(!reported));
    assert_eq!(alarm(model.control, false, false, model.pending), None);
}

fn main() {}
