//! Provider publication racing the real runnable-source scheduler parking.
use super::*;
mod visibility;
struct Root(AtomicU64);
impl ArcWake for Root {
    fn wake_by_ref(root: &Arc<Self>) {
        root.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn admitted(latest: bool, pending: usize) -> Option<usize> {
    Channel {
        generation: 1,
        latest,
        children: false,
        pending: (0..pending)
            .map(|_| Pending {
                frame: Default::default(),
                key: None,
            })
            .collect(),
        indexed: None,
        credit: wire::SessionWindow {
            frames: 1,
            bytes: 100,
        },
        sent: Default::default(),
        applied: Default::default(),
        ending: None,
    }
    .source_mode()
}

fn waiting(identity: wire::SessionChannel, records: Arc<signal::Signal<u8>>) -> Source {
    futures::stream::once(async move { (identity, Ok(Event::Opened(None))) })
        .chain(futures::stream::once(async move {
            let value = records.next().await.unwrap();
            (
                identity,
                Ok(Event::Record(crate::Record {
                    sequence: 0,
                    value: vec![value].into(),
                    commit_id: crate::CommitId::from_bytes([1; 32]),
                    committed_at_micros: 0,
                })),
            )
        }))
        .boxed()
}

/// One blocked channel must leave the reserved lossless slot runnable.
pub fn verify_source_saturation(broken: bool) {
    for latest in [false, true] {
        loom::model(move || {
            let slow = wire::SessionChannel {
                channel_id: 1,
                generation: 1,
            };
            let healthy = wire::SessionChannel {
                channel_id: 2,
                generation: 1,
            };
            let mut sources = Sources::new();
            sources.insert(
                slow,
                waiting(slow, Arc::new(signal::Signal::new())),
                admitted(latest, if latest { 3 } else { 1 }),
            );
            let records = Arc::new(signal::Signal::new());
            sources.insert(
                healthy,
                waiting(healthy, records.clone()),
                admitted(false, 0),
            );
            let root = Arc::new(Root(AtomicU64::new(0)));
            let waker = futures::task::waker(root);
            let mut cx = Context::from_waker(&waker);
            for _ in 0..2 {
                assert!(matches!(
                    sources.poll((true, true, true), &mut cx),
                    Poll::Ready(Ok(Some((_, Ok(Event::Opened(_))))))
                ));
            }
            let room = policy::room(3, 1000, 2, 200, 100, true);
            let selected = if broken {
                (room.0, room.0, room.2)
            } else {
                room
            };
            let writer = loom::thread::spawn(move || records.send(7).unwrap());
            let first = sources.poll(selected, &mut cx);
            writer.join().unwrap();
            let result = if first.is_pending() {
                sources.poll(selected, &mut cx)
            } else {
                first
            };
            assert!(
                matches!(result, Poll::Ready(Ok(Some((identity, Ok(Event::Record(record)))))) if identity == healthy && record.value.as_ref() == [7]),
                "the reserved slot must enable the healthy channel"
            );
        });
    }
}

/// Disabling a parked provider preserves a publication until it is re-admitted.
pub fn verify_source_disable(broken: bool) {
    loom::model(move || {
        let identity = wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        };
        let records = Arc::new(signal::Signal::new());
        let mut sources = Sources::new();
        sources.insert(
            identity,
            waiting(identity, records.clone()),
            admitted(false, 0),
        );
        let root = Arc::new(Root(AtomicU64::new(0)));
        let waker = futures::task::waker(root);
        let mut cx = Context::from_waker(&waker);
        assert!(sources.poll((true, true, true), &mut cx).is_ready());
        assert!(sources.poll((true, true, true), &mut cx).is_pending());
        let writer = loom::thread::spawn(move || records.send(7).unwrap());
        if !broken {
            sources.mode(1, None);
        }
        writer.join().unwrap();
        assert!(
            sources.poll((true, true, true), &mut cx).is_pending(),
            "a disabled source must not consume a record"
        );
        sources.mode(1, admitted(false, 0));
        assert!(
            matches!(sources.poll((true, true, true), &mut cx), Poll::Ready(Ok(Some((actual, Ok(Event::Record(record)))))) if actual == identity && record.value.as_ref() == [7])
        );
    });
}

/// Explores provider publication racing source and session waiter registration.
pub fn verify_source_wakes(omit_signal: bool) {
    loom::model(move || {
        let identity = wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        };
        let published = Arc::new(gate::Gate::new());
        let waiting = published.clone();
        let source = futures::stream::once(async move { (identity, Ok(Event::Opened(None))) })
            .chain(futures::stream::once(async move {
                waiting.changed(false).await.unwrap();
                (identity, Ok(Event::Opened(None)))
            }))
            .boxed();
        let mut sources = Sources::new();
        sources.insert(identity, source, Some(ALL));
        Arc::get_mut(&mut sources.entries.get_mut(&1).unwrap().wake)
            .unwrap()
            .omit_signal = omit_signal;
        let root = Arc::new(Root(AtomicU64::new(0)));
        let waker = futures::task::waker(root.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            sources.poll((true, true, true), &mut cx),
            Poll::Ready(Ok(Some(_)))
        ));
        sources.mode(1, None);
        let writer = loom::thread::spawn(move || published.set(true).unwrap());
        sources.mode(1, Some(ALL));
        sources.mode(1, Some(ALL));
        let first = sources.poll((true, true, true), &mut cx);
        let result = if first.is_pending() {
            // Consume the cooperative yield before checking the parked wakeup.
            root.0.store(0, Ordering::SeqCst);
            sources.poll((true, true, true), &mut cx)
        } else {
            first
        };
        writer.join().unwrap();
        if result.is_pending() {
            assert!(
                root.0.load(Ordering::SeqCst) > 0,
                "provider source lost its publication wake"
            );
            assert!(matches!(
                sources.poll((true, true, true), &mut cx),
                Poll::Ready(Ok(Some(_)))
            ));
        } else {
            assert!(matches!(result, Poll::Ready(Ok(Some(_)))));
        }
        sources.remove(1);
        assert!(sources.is_empty());
    });
}

/// Repeated/coalesced wakes can be spurious: the provider may recheck Pending.
/// Both notifications and rechecks use the production source scheduler.
pub fn verify_source_recheck(fault: u8) {
    if fault == 2 {
        return visibility::verify(true);
    }
    if fault == 0 {
        visibility::verify(false);
    }
    loom::model(move || {
        let identity = wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        };
        let mut polls = 0;
        let source = futures::stream::poll_fn(move |cx| {
            let current = polls;
            polls += 1;
            if current == 1 || current == 2 {
                // The second wake observes queued==1 and must be coalesced.
                cx.waker().wake_by_ref();
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Some((identity, Ok(Event::Opened(None)))))
            }
        })
        .boxed();
        let mut sources = Sources::new();
        sources.insert(identity, source, Some(ALL));
        let wake = Arc::get_mut(&mut sources.entries.get_mut(&1).unwrap().wake).unwrap();
        wake.clear_after_recheck = fault == 1;
        let root = Arc::new(Root(AtomicU64::new(0)));
        let waker = futures::task::waker(root.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(sources.poll((true, true, true), &mut cx).is_ready());
        assert!(sources.poll((true, true, true), &mut cx).is_pending());
        let wake = sources.entries[&1].wake.clone();
        let writer = loom::thread::spawn(move || {
            Wake::wake_by_ref(&wake);
            Wake::wake_by_ref(&wake);
        });
        // Admission updates and their no-op fast path race provider wakes too.
        sources.mode(1, None);
        sources.mode(1, Some(ALL));
        sources.mode(1, Some(ALL));
        // Drain old hints and recheck while publication is racing the drain.
        assert!(sources.poll((true, true, true), &mut cx).is_pending());
        writer.join().unwrap();
        root.0.store(0, Ordering::SeqCst);
        let next = sources.poll((true, true, true), &mut cx);
        let next = if next.is_pending() && root.0.load(Ordering::SeqCst) > 0 {
            root.0.store(0, Ordering::SeqCst);
            sources.poll((true, true, true), &mut cx)
        } else {
            next
        };
        assert!(
            matches!(next, Poll::Ready(Ok(Some(_)))),
            "provider recheck lost its wakeup"
        );
    });
}
