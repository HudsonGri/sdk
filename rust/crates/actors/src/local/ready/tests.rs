use super::*;

#[cfg(not(acyclic_loom))]
#[test]
fn replay_coalescing_and_hibernation_have_bounded_ownership() -> Result<(), super::super::Error> {
    let set = ReadySet::default();
    let registration = set.register(7)?;
    assert_eq!(set.counts(), (1, 1), "registered subscriptions must start with replay");
    let wake = registration.waker();
    for _ in 0..100 { wake.wake_by_ref(); }
    assert_eq!(set.counts(), (1, 1));
    let work = set.take().ok_or(super::super::Error::Invalid("missing initial replay"))?;
    assert_eq!(*work.key(), 7);
    wake.wake_by_ref();
    drop(work);
    assert_eq!(set.counts(), (1, 1), "append during replay must survive parking");
    drop(set.take());
    assert_eq!(set.counts(), (1, 0));
    drop(registration);
    wake.wake_by_ref();
    assert_eq!(set.counts(), (0, 0), "retired wake cannot recreate registration");
    Ok(())
}

#[cfg(not(acyclic_loom))]
#[test]
fn dormant_population_retains_no_work_and_unlinks_cancelled_ready_entries() -> Result<(), super::super::Error> {
    let set = ReadySet::default();
    let mut registrations = Vec::new();
    for id in 0..100_000 { registrations.push(set.register(id)?); }
    assert_eq!(set.counts(), (100_000, 100_000));
    while let Some(work) = set.take() { drop(work); }
    assert_eq!(set.counts(), (100_000, 0));
    for registration in &registrations { registration.waker().wake(); }
    // Removal unlinks any position directly; no tombstone is retained in a queue.
    registrations.reverse();
    drop(registrations);
    assert_eq!(set.counts(), (0, 0));
    Ok(())
}

#[cfg(acyclic_loom)]
mod schedules {
    use super::*;
    use loom::{sync::atomic::{AtomicUsize, Ordering}, thread};

    #[test]
    fn append_racing_replay_and_park_cannot_lose_ready_work() {
        loom::model(|| {
            let set = ReadySet::default();
            let registration = set.register(7).unwrap();
            let work = set.take().unwrap();
            let wake = registration.waker();
            let tail = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));
            let reader_tail = Arc::clone(&tail);
            let reader_consumed = Arc::clone(&consumed);
            let reader = thread::spawn(move || {
                reader_consumed.store(reader_tail.load(Ordering::SeqCst), Ordering::SeqCst);
                drop(work);
            });
            let writer = thread::spawn(move || {
                tail.store(1, Ordering::SeqCst);
                wake.wake();
            });
            reader.join().unwrap();writer.join().unwrap();
            assert!(consumed.load(Ordering::SeqCst) == 1 || set.counts().1 == 1,
                "committed input must be replayed or remain ready across parking");
        });
    }

    #[test]
    fn cancelled_registration_cannot_wake_a_new_placement() {
        loom::model(|| {
            let set = ReadySet::default();
            let old = set.register(1).unwrap();
            drop(set.take());
            let stale = old.waker();
            let current = set.register(2).unwrap();
            let wake = stale.clone();
            let writer = thread::spawn(move || wake.wake());
            let cancel = thread::spawn(move || drop(old));
            writer.join().unwrap();cancel.join().unwrap();
            stale.wake();
            assert_eq!(set.counts(), (1, 1));
            let work = set.take().unwrap();
            assert_eq!(*work.key(), 2, "stale wake cannot substitute old placement");
            drop(work);drop(current);
            assert_eq!(set.counts(), (0, 0));
        });
    }

    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    }

    #[test]
    fn dispatcher_registers_its_waiter_with_the_empty_check() {
        loom::model(|| {
            let mut set = ReadySet::default();
            let registration = set.register(1).unwrap();
            drop(set.take());
            let wake = registration.waker();
            let counter = Arc::new(Counter(AtomicUsize::new(0)));
            let consumer_counter = Arc::clone(&counter);
            let consumer = thread::spawn(move || {
                let waiter = Waker::from(consumer_counter);
                let pending = set.poll_ready(&mut Context::from_waker(&waiter)).is_pending();
                (set, pending)
            });
            let producer = thread::spawn(move || wake.wake());
            let (_set, pending) = consumer.join().unwrap();producer.join().unwrap();
            assert!(!pending || counter.0.load(Ordering::SeqCst) > 0, "empty-to-wait handoff must retain append notification");
            drop(registration);
        });
    }
}
