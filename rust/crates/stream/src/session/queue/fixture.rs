use super::*;
use crate::{CommitId, MemoryStream, OwnedBytes, Record};

pub(in crate::session) fn empty_session(latest: bool) -> Session<MemoryStream> {
    let mut session = Session::new(
        Arc::new(MemoryStream::default()),
        "node".into(),
        Limits::default(),
        stream::empty().boxed(),
    )
    .unwrap();
    session.hello = true;
    add_channel(&mut session, 1, 1, latest);
    session
}
pub(super) fn add_channel(
    session: &mut Session<MemoryStream>,
    id: u64,
    generation: u64,
    latest: bool,
) {
    session.channels.insert(
        id,
        Channel {
            generation,
            latest,
            children: false,
            pending: VecDeque::new(),
            slot: None,
            enabled: Arc::new(gate::Gate::new()),
            credit: wire::SessionWindow::default(),
            sent: cursor(0),
            applied: cursor(0),
            ending: None,
            cancel: None,
        },
    );
}
pub(super) fn event(sequence: u64, key: u8, attention: bool) -> Event {
    Event::Record(Record {
        sequence,
        commit_id: CommitId::from_bytes([key; 32]),
        committed_at_micros: 0,
        value: OwnedBytes::from(
            wire::SessionLatestValue {
                key: Bytes::copy_from_slice(&[key]),
                value: Bytes::copy_from_slice(&[sequence as u8]),
                attention,
            }
            .encode_to_vec(),
        ),
    })
}
pub(super) fn grant(session: &mut Session<MemoryStream>, id: u64, bytes: u64, frames: u32) {
    let generation = if id == 0 {
        0
    } else {
        session.channels[&id].generation
    };
    session
        .receive(frame(
            id,
            generation,
            Body::Window(wire::SessionWindow { bytes, frames }),
        ))
        .unwrap();
}
