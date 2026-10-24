use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn both_child_orders_seek_direct_children_and_preserve_historical_hierarchy_versions()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut state = root::Root::default();
    let parent = StreamPath::new("tasks")?;
    let mut old = Vec::new();
    for (position, path) in ["z", "tasks", "tasks/z", "tasks/a", "tasks/a/deep", "a"]
        .into_iter()
        .enumerate()
    {
        state.decision = position as u64 + 1;
        let commit = CommitId::from_bytes([u8::try_from(position)? + 1; 32]);
        let path = StreamPath::new(path)?;
        insert(&mut disk, &mut state, &path, commit)?;
        assert_eq!(state.hierarchy_version, commit);
        old.push(state);
    }
    for (order, expected) in [
        (Order::Alphabetical, vec!["tasks/a", "tasks/z"]),
        (Order::Creation, vec!["tasks/z", "tasks/a"]),
    ] {
        let mut previous: Option<Child> = None;
        for path in expected {
            let child = next(
                &mut disk,
                state,
                Some(&parent),
                order,
                previous.as_ref().map(|child| (&child.path, child.decision)),
            )?
            .ok_or("missing child")?;
            assert_eq!(child.path.as_str(), path);
            previous = Some(child);
        }
        assert!(
            next(
                &mut disk,
                state,
                Some(&parent),
                order,
                previous.as_ref().map(|child| (&child.path, child.decision))
            )?
            .is_none()
        );
    }
    let before = *old.get(2).ok_or("missing historical root")?;
    let child = next(&mut disk, before, Some(&parent), Order::Alphabetical, None)?
        .ok_or("missing historical child")?;
    assert_eq!(child.path.as_str(), "tasks/z");
    assert!(
        next(
            &mut disk,
            before,
            Some(&parent),
            Order::Alphabetical,
            Some((&child.path, child.decision))
        )?
        .is_none()
    );
    assert_ne!(before.hierarchy_version, state.hierarchy_version);
    let missing = StreamPath::new("missing")?;
    assert!(next(&mut disk, state, Some(&missing), Order::Creation, None)?.is_none());
    let unrelated = StreamPath::new("other/child")?;
    assert!(
        next(
            &mut disk,
            state,
            Some(&parent),
            Order::Creation,
            Some((&unrelated, 1))
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn child_resume_seeks_one_anchor_and_refunds_all_traversal_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let parent = StreamPath::new("tasks")?;
    let mut state = root::Root::default();
    for decision in 1..=128 {
        state.decision = decision;
        let path = StreamPath::new(format!("tasks/agent-{decision:03}"))?;
        insert(&mut disk, &mut state, &path, CommitId::from_bytes([1; 32]))?;
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    let anchor = StreamPath::new("tasks/agent-127")?;
    disk.reads = 0;
    let last = next(
        &mut disk,
        state,
        Some(&parent),
        Order::Creation,
        Some((&anchor, 127)),
    )?
    .ok_or("missing successor")?;
    assert_eq!(last.path.as_str(), "tasks/agent-128");
    let seek_reads = disk.reads;
    assert!(seek_reads < 32);
    assert!(
        meter.held.load(Ordering::SeqCst) > baseline,
        "returned path keeps its original funding"
    );
    drop(last);
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    // Replaying the same public child order is the previous resume shape.
    disk.reads = 0;
    let mut previous: Option<Child> = None;
    for _ in 0..128 {
        previous = next(
            &mut disk,
            state,
            Some(&parent),
            Order::Creation,
            previous.as_ref().map(|child| (&child.path, child.decision)),
        )?;
    }
    assert!(
        disk.reads > seek_reads * 32,
        "scan control must expose history-dependent work"
    );
    drop(previous);
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    drop(disk);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn child_index_rejects_authenticated_cross_parent_and_cross_order_bindings()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let parent = StreamPath::new("tasks")?;
    let path = StreamPath::new("tasks/agent")?;
    let other = StreamPath::new("other/agent")?;
    for forged_path in [&other, &path] {
        let mut state = root::Root {
            decision: 2,
            ..Default::default()
        };
        let reference = fact(&mut disk, forged_path, 2)?;
        let key = key(&disk, Some(&parent), Order::Creation, Some((&path, 1)))?;
        state.children = Some(index::put(&mut disk, None, &key.bytes, reference)?);
        assert!(next(&mut disk, state, Some(&parent), Order::Creation, None).is_err());
    }
    Ok(())
}
