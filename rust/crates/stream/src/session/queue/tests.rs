use super::*;
use fixture::{add_channel, empty_session, event, grant};
use proptest::prelude::*;

fn record(
    session: &mut Session<crate::MemoryStream>,
    id: u64,
    sequence: u64,
    key: u8,
    attention: bool,
) {
    session
        .record(
            wire::SessionChannel {
                channel_id: id,
                generation: session.channels[&id].generation,
            },
            Ok(event(sequence, key, attention)),
        )
        .unwrap();
}
fn bounded(work: usize, bound: usize) -> Result<(), &'static str> {
    if work > bound {
        Err("unrelated queued followers were inspected")
    } else {
        Ok(())
    }
}
#[test]
fn ten_thousand_channels_bound_queued_candidate_work() {
    let mut session = empty_session(true);
    for id in (1..20_000).step_by(2) {
        if id != 1 {
            add_channel(&mut session, id, 1, true);
        }
    }
    // Keep the real 1,024-frame admission bound, with its reserved healthy slot.
    // All ten thousand subscriptions remain present; only admitted heads buffer.
    let background = session.limits.pending_frames - 1;
    for id in (1..1 + 2 * background as u64).step_by(2) {
        record(&mut session, id, 0, 1, false);
    }
    assert_eq!(session.channels.len(), 10_000);
    let bound = 4 * session.ready.height() + 1;
    // Paired old and indexed selection use the same queue and target channel.
    WORK.with(|work| work.set(0));
    find_key(
        &session.pending,
        session.pending.keys(),
        1,
        &Bytes::from_static(&[1]),
    )
    .unwrap();
    let old_update = WORK.with(std::cell::Cell::get);
    assert_eq!(
        bounded(old_update, bound),
        Err("unrelated queued followers were inspected")
    );

    WORK.with(|work| work.set(0));
    record(&mut session, 1, 1, 1, false);
    let update = WORK.with(std::cell::Cell::get);
    assert_eq!(bounded(update, bound), Ok(()));

    grant(&mut session, 19_999, 1_000_000, 1);
    grant(&mut session, 0, 1_000_000, 1);
    record(&mut session, 19_999, 0, 1, false);
    // Mutant: scan the same pending queue for the same eligible head.
    WORK.with(|work| work.set(0));
    let old_head = session
        .pending
        .values()
        .find(|pending| {
            examined();
            let channel = &session.channels[&pending.frame.channel_id];
            channel.credit.frames > 0 && channel.credit.bytes >= pending.frame.encoded_len() as u64
        })
        .unwrap()
        .frame
        .channel_id;
    let old_dispatch = WORK.with(std::cell::Cell::get);
    WORK.with(|work| work.set(0));
    let value = session.ready().unwrap().unwrap();
    assert_eq!(value.channel_id, old_head);
    assert_eq!(value.channel_id, 19_999);
    let dispatch = WORK.with(std::cell::Cell::get);
    assert_eq!(bounded(dispatch, bound), Ok(()));
    assert_eq!(
        bounded(old_dispatch, bound),
        Err("unrelated queued followers were inspected")
    );
    eprintln!(
        "pending_index channels=10000 queued={} latest_candidates={old_update}->{update} dispatch_candidates={old_dispatch}->{dispatch} bound={bound}",
        background + 1
    );
}

fn invariants(session: &Session<crate::MemoryStream>, expected: &[wire::SessionFrame]) {
    let actual = session
        .pending
        .values()
        .map(|pending| pending.frame.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "pending data differs from independent history"
    );
    assert_eq!(
        session.pending_bytes,
        expected
            .iter()
            .map(|frame| frame.encoded_len())
            .sum::<usize>()
    );
    assert_eq!(session.pending.len(), expected.len());
    for (id, channel) in &session.channels {
        assert_eq!(
            channel
                .pending
                .iter()
                .map(|token| &session.pending[token].frame)
                .collect::<Vec<_>>(),
            expected
                .iter()
                .filter(|frame| frame.channel_id == *id)
                .collect::<Vec<_>>()
        );
    }
    for (slot, id) in session.ready.owners() {
        assert_eq!(
            session.channels.get(&id).and_then(|channel| channel.slot),
            Some(slot),
            "head index retained a cancelled owner"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Independent history projection, credit arithmetic and per-channel FIFO:
    // replacement, grants, byte starvation, terminal drain, cancel and slot reuse
    // cannot corrupt another channel or expose a later record before its head.
    #[test]
    fn indexed_queues_match_the_independent_history_and_credit_ledger(
        steps in prop::collection::vec((0u8..6, 0usize..3, 1u8..5, any::<bool>(), 0u16..256, 0u32..3), 0..65),
    ) {
        let mut session = empty_session(true);
        add_channel(&mut session, 3, 1, false);
        add_channel(&mut session, 5, 1, true);
        let mut expected: Vec<wire::SessionFrame> = Vec::new();
        let mut next = [0u64; 3];
        let mut generation = [1u64; 3];
        let mut credit = [(0u64, 0u32); 4];
        let mut ending = [false; 3];
        for (action, agent, key, attention, bytes, frames) in steps {
            let id = 1 + 2 * agent as u64;
            match action {
                0 if !ending[agent] => {
                    let event = event(next[agent], key, attention);
                    let Event::Record(record) = &event else { unreachable!() };
                    let item = if agent == 1 { Item::Record(wire_codec::record_wire(record.clone())) }
                        else { Item::LatestValue(wire::SessionLatestValue::decode(record.value.as_ref()).unwrap()) };
                    let after = next[agent] + 1;
                    let data = frame(id, generation[agent], Body::Data(wire::SessionData { after: Some(cursor(after)), accepted: after, durable: 0, item: Some(item) }));
                    if agent != 1 && !attention {
                        expected.retain(|old| !(old.channel_id == id && matches!(&old.body, Some(Body::Data(wire::SessionData { item: Some(Item::LatestValue(value)), .. })) if !value.attention && value.key.as_ref() == [key])));
                    }
                    expected.push(data);
                    session.record(wire::SessionChannel {channel_id:id, generation:generation[agent]}, Ok(event)).unwrap();
                    next[agent] += 1;
                }
                1 | 2 => {
                    let target = if action == 1 { 0 } else { id };
                    let entry = if action == 1 { 3 } else { agent };
                    credit[entry].0 += u64::from(bytes);
                    credit[entry].1 += frames;
                    if session.channels.contains_key(&id) || target == 0 {
                        grant(&mut session, target, u64::from(bytes), frames);
                    }
                }
                3 => {
                    let eligible = expected.iter().enumerate().filter(|(position, frame)| {
                        let agent = (frame.channel_id / 2) as usize;
                        let size = frame.encoded_len() as u64;
                        credit[3].1 > 0 && credit[3].0 >= size && credit[agent].1 > 0 && credit[agent].0 >= size
                            && !expected[..*position].iter().any(|old| old.channel_id == frame.channel_id)
                    }).map(|(_, frame)| frame.clone()).collect::<Vec<_>>();
                    let actual = session.ready().unwrap();
                    if let Some(actual) = actual {
                        prop_assert!(eligible.contains(&actual), "delivered an ineligible or reordered head");
                        let agent = (actual.channel_id / 2) as usize;
                        let size = actual.encoded_len() as u64;
                        credit[3].0 -= size; credit[3].1 -= 1;
                        credit[agent].0 -= size; credit[agent].1 -= 1;
                        expected.remove(expected.iter().position(|frame| frame == &actual).unwrap());
                    } else { prop_assert!(eligible.is_empty(), "an eligible head was stranded"); }
                }
                4 => {
                    session.remove(id).unwrap();
                    expected.retain(|frame| frame.channel_id != id);
                    generation[agent] += 1;
                    add_channel(&mut session, id, generation[agent], agent != 1);
                    next[agent] = 0; credit[agent] = (0,0); ending[agent] = false;
                    session.record(wire::SessionChannel {channel_id:id, generation:generation[agent]-1}, Ok(event(99, key, attention))).unwrap();
                }
                5 => {
                    if session.channels.contains_key(&id) {
                        session.record(wire::SessionChannel {channel_id:id, generation:generation[agent]}, Err(StreamError::Unavailable)).unwrap();
                        ending[agent] = true;
                    }
                }
                _ => {}
            }
            invariants(&session, &expected);
            prop_assert_eq!((session.credit.bytes, session.credit.frames), credit[3]);
            for (index, id) in [1,3,5].into_iter().enumerate() {
                if let Some(channel) = session.channels.get(&id) {
                    prop_assert_eq!((channel.credit.bytes, channel.credit.frames), credit[index]);
                }
            }
        }
    }
}

#[test]
fn tiny_aggregate_grants_revisit_only_one_index_path_and_remain_fair() {
    let mut session = empty_session(false);
    add_channel(&mut session, 3, 1, false);
    for id in [1, 3] {
        record(&mut session, id, 0, 1, false);
        grant(&mut session, id, 1_000_000, 10);
    }
    let mut order = Vec::new();
    let mut next = [1u64; 2];
    for _ in 0..12 {
        let bytes = session
            .pending
            .values()
            .map(|pending| pending.frame.encoded_len())
            .max()
            .unwrap() as u64;
        grant(&mut session, 0, bytes, 1);
        order.push(session.ready().unwrap().unwrap().channel_id);
        let id = *order.last().unwrap();
        let agent = id as usize / 2;
        record(&mut session, id, next[agent], 1, false);
        next[agent] += 1;
    }
    assert!(order.windows(2).all(|ids| ids[0] != ids[1]));
}

#[test]
fn refused_replacements_and_token_exhaustion_leave_the_accepted_queue_intact() {
    for overflow in [false, true] {
        let mut session = empty_session(true);
        record(&mut session, 1, 0, 1, false);
        let before = session
            .pending
            .values()
            .map(|pending| pending.frame.clone())
            .collect::<Vec<_>>();
        session.limits.pending_bytes = session.pending_bytes;
        session.limits.pending_frames = 1;
        if overflow {
            session.next_pending = u64::MAX;
        }
        let mut replacement = event(1, 1, false);
        let Event::Record(record) = &mut replacement else {
            unreachable!()
        };
        record.value = crate::OwnedBytes::from(
            wire::SessionLatestValue {
                key: Bytes::from_static(&[1]),
                value: Bytes::from_static(b"larger"),
                attention: false,
            }
            .encode_to_vec(),
        );
        assert_eq!(
            session.record(
                wire::SessionChannel {
                    channel_id: 1,
                    generation: 1
                },
                Ok(replacement)
            ),
            Err(StreamError::Capacity)
        );
        invariants(&session, &before);
    }
    let mut session = empty_session(true);
    record(&mut session, 1, 0, 1, false);
    session.limits.pending_bytes = session.pending_bytes;
    session.limits.pending_frames = 1;
    // Same-size replacement needs no second frame slot or double byte reservation.
    record(&mut session, 1, 1, 1, false);
    assert_eq!(session.pending.len(), 1);
}

#[test]
#[should_panic(expected = "pending data differs from independent history")]
fn negative_control_conflates_an_attention_record() {
    let mut good = empty_session(true);
    let mut broken = empty_session(true);
    for (sequence, attention) in [(0, false), (1, true), (2, false)] {
        record(&mut good, 1, sequence, 1, attention);
        // Broken platform preparation drops the non-conflatable attention bit.
        record(&mut broken, 1, sequence, 1, false);
    }
    let expected = good
        .pending
        .values()
        .map(|pending| pending.frame.clone())
        .collect::<Vec<_>>();
    invariants(&broken, &expected);
}

#[test]
#[should_panic(expected = "index must not select a head beyond byte credit")]
fn negative_control_index_omits_the_encoded_head_size() {
    let mut session = empty_session(false);
    grant(&mut session, 1, 1_000_000, 1);
    record(&mut session, 1, 0, 1, false);
    grant(&mut session, 0, 1, 1);
    let slot = session.channels[&1].slot.unwrap();
    session.ready.set(slot, Some(0)).unwrap();
    session
        .ready()
        .expect("index must not select a head beyond byte credit");
}
