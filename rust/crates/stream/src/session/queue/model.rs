use super::*;
use crate::session::sync::Arc as ModelArc;
use fixture::{add_channel, empty_session, event, grant};

/// One owner serializes Session state as in serve(). Sender threads race the
/// real shim-backed Signal's wait registration with credit and cancellation.
/// Root credit is fixed here: it has no shared synchronization or cached root
/// eligibility. The independent stateful property varies root grants as well.
/// No dependency mutex is used. Lock order remains Signal and Gate separately;
/// neither survives a provider poll, index mutation, or waker invocation.
pub fn verify_queued_admission(fault: u8) {
    loom::model(move || {
        let mut session = empty_session(true);
        add_channel(&mut session, 3, 1, true);
        grant(&mut session, 0, 1_000_000, 4);

        for sequence in 0..3 {
            session
                .record(
                    wire::SessionChannel {
                        channel_id: 1,
                        generation: 1,
                    },
                    Ok(event(sequence, 1, true)),
                )
                .unwrap();
        }
        session
            .record(
                wire::SessionChannel {
                    channel_id: 3,
                    generation: 1,
                },
                Ok(event(0, 1, false)),
            )
            .unwrap();
        let input = ModelArc::new(signal::Signal::new());
        let credit = ModelArc::clone(&input);
        let credited = loom::thread::spawn(move || {
            credit.send(0).unwrap();
        });
        let cancel = ModelArc::clone(&input);
        let cancelled = loom::thread::spawn(move || cancel.send(1).unwrap());
        let mut delivered = 0;
        for _ in 0..2 {
            match loom::future::block_on(input.next()).unwrap() {
                0 => {
                    grant(&mut session, 3, 1_000_000, 1);
                    if fault == 1 {
                        let slot = session.channels[&3].slot.unwrap();
                        session.ready.set(slot, None).unwrap();
                    }
                }
                1 => {
                    session
                        .receive(frame(
                            0,
                            0,
                            Body::Interest(wire::SessionInterest {
                                revision: 1,
                                remove: vec![wire::SessionChannel {
                                    channel_id: 1,
                                    generation: 1,
                                }],
                                ..Default::default()
                            }),
                        ))
                        .unwrap();
                    if fault == 2 {
                        session.ready.insert(1).unwrap();
                    }
                    assert!(
                        session.ready.owners().all(|(_, id)| id != 1),
                        "cancelled head must release its owned index slot"
                    );
                    session
                        .record(
                            wire::SessionChannel {
                                channel_id: 1,
                                generation: 1,
                            },
                            Ok(event(100, 1, true)),
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
            while let Some(frame) = session.ready().unwrap() {
                if frame.channel_id == 3 {
                    delivered += 1;
                } else {
                    assert!(session.channels.contains_key(&frame.channel_id));
                }
            }
        }
        credited.join().unwrap();
        cancelled.join().unwrap();
        assert_eq!(
            delivered, 1,
            "credit must publish the independent queued head"
        );
        assert!(session.pending.is_empty());
        assert_eq!(session.pending_bytes, 0);
        assert!(session.ready().unwrap().is_none());
    });
}
