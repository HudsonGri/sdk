use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const CHILDREN: u64 = 10_000;

// Each child is constructed, used and settled in turn. Returns the views and
// index entries left behind, and the most ever live at once.
fn cycle(settle: bool) -> (usize, usize, usize) {
    let views = StateArc::new(ChildViews::<u64>::default());
    let (live, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
    for child in 0..CHILDREN {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&child.to_le_bytes());
        let task = TaskId::from_bytes(bytes);
        let view = futures::executor::block_on(ChildViews::acquire(&views, task, async || {
            peak.fetch_max(live.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            Ok(child)
        }))
        .unwrap();
        assert_eq!(*view, child);
        drop(view);
        if settle {
            futures::executor::block_on(ChildViews::settle(&views, task, async |_| {
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        }
    }
    let entries = views.lock().entries.len();
    (views.live(), entries, peak.into_inner())
}

#[test]
fn ten_thousand_settled_children_leave_no_view_or_index_entry() {
    assert_eq!(cycle(true), (0, 0, 1));
}

#[test]
fn negative_control_children_that_never_settle_keep_every_view() {
    let children = usize::try_from(CHILDREN).unwrap();
    assert_eq!(cycle(false), (children, children, children));
}

// A child that settles during a call keeps its view until that call ends,
// then retires at the next settlement.
#[test]
fn a_view_in_use_retires_after_its_last_call() {
    let views = StateArc::new(ChildViews::<u8>::default());
    let (first, second) = (TaskId::from_bytes([1; 16]), TaskId::from_bytes([2; 16]));
    let retired = AtomicUsize::new(0);
    let retire = async |_: &u8| {
        retired.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let busy =
        futures::executor::block_on(ChildViews::acquire(&views, first, async || Ok(1))).unwrap();
    futures::executor::block_on(ChildViews::settle(&views, first, retire)).unwrap();
    assert_eq!((views.live(), retired.load(Ordering::SeqCst)), (1, 0));
    drop(busy);
    futures::executor::block_on(ChildViews::settle(&views, second, retire)).unwrap();
    assert_eq!((views.live(), retired.load(Ordering::SeqCst)), (0, 1));
}

// A refused retirement keeps the view installed for the next settlement.
#[test]
fn a_refused_retirement_keeps_the_view() {
    let views = StateArc::new(ChildViews::<u8>::default());
    let task = TaskId::from_bytes([1; 16]);
    drop(futures::executor::block_on(ChildViews::acquire(&views, task, async || Ok(1))).unwrap());
    let refused = futures::executor::block_on(ChildViews::settle(&views, task, async |_| {
        Err(Error::Conflict("busy".into()))
    }));
    assert!(refused.is_err());
    assert_eq!(views.live(), 1);
    futures::executor::block_on(ChildViews::settle(&views, task, async |_| Ok(()))).unwrap();
    assert_eq!(views.live(), 0);
}
