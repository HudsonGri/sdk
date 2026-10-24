use super::*;
use futures::TryStreamExt as _;

fn decode_chunks(input: &[u8], width: usize, maximum: usize) -> Result<Vec<String>> {
    let chunks = input
        .chunks(width)
        .map(Bytes::copy_from_slice)
        .map(Ok)
        .collect::<Vec<_>>();
    futures::executor::block_on(
        decode(Box::pin(futures::stream::iter(chunks)), maximum).try_collect(),
    )
}

#[test]
fn chunking_cannot_change_framing_utf8_or_truncation() {
    let input = "data: 猫\r\ndata: 🦀\r\n\r\n: keepalive\n\ndata: last\n\n";
    for width in 1..=input.len() {
        assert_eq!(
            decode_chunks(input.as_bytes(), width, 128).unwrap(),
            ["猫\n🦀", "last"]
        );
        // A complete JSON value is still not a complete SSE event.
        assert!(decode_chunks(b"data: {}\n", width, 128).is_err());
        assert!(decode_chunks(b"data: {}", width, 128).is_err());
        assert!(decode_chunks(b"data: \xff\n\n", width, 128).is_err());
    }
}

#[test]
fn each_event_has_its_own_bound_before_buffer_growth() {
    for width in 1..32 {
        assert_eq!(
            decode_chunks(b"data: a\n\ndata: b\n\n", width, 9).unwrap(),
            ["a", "b"]
        );
        assert!(decode_chunks(b"data: ab\n\n", width, 9).is_err());
        assert!(decode_chunks(b"ignored: 12345678\n\n", width, 9).is_err());
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_frames_hold_the_original_ledger_until_consumed() {
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 1_000_000),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    let initial = work.live_allocation_bytes();
    let frame = {
        let body = Box::pin(futures::stream::iter([Ok(Bytes::from_static(
            b"data: native\n\n",
        ))]));
        let stream = frames(body, 1024, Decoder::native(&work).unwrap());
        futures::pin_mut!(stream);
        let frame = futures::executor::block_on(stream.next()).unwrap().unwrap();
        assert!(futures::executor::block_on(stream.next()).is_none());
        frame
    };
    assert_eq!(frame.data, "native");
    assert_eq!(
        work.live_allocation_bytes() - initial,
        frame.data.capacity() as u64
    );
    drop(frame);
    assert_eq!(work.live_allocation_bytes(), initial);
}

#[test]
fn subscriptions_keep_exact_cursors_and_reset_event_names() {
    let input = "id: 9007199254740993\r\nevent: update\r\ndata: {\r\ndata: \"value\":\"猫\"}\r\n\r\nid: bad\0cursor\ndata: 2\n\nid: 7\n\ndata\n\n";
    for width in 1..=input.len() {
        let chunks = input
            .as_bytes()
            .chunks(width)
            .map(Bytes::copy_from_slice)
            .map(Ok)
            .collect::<Vec<_>>();
        let frames = futures::executor::block_on(
            frames(
                Box::pin(futures::stream::iter(chunks)),
                256,
                Decoder::subscription(),
            )
            .try_collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(frames.len(), 3);
        let first = frames[0].metadata.as_ref().unwrap();
        assert_eq!(first.id, "9007199254740993");
        assert_eq!(first.event, "update");
        assert_eq!(frames[0].data, "{\n\"value\":\"猫\"}");
        let second = frames[1].metadata.as_ref().unwrap();
        assert_eq!(second.id, first.id);
        assert_eq!(second.event, "message");
        assert_eq!(frames[2].metadata.as_ref().unwrap().id, "7");
        assert_eq!(frames[2].data, "");
    }
}

#[test]
fn every_line_ending_counts_physical_bytes_before_publication() {
    for ending in ["\n", "\r", "\r\n"] {
        let input = format!("data: {{}}{ending}{ending}");
        for width in 1..=input.len() {
            assert_eq!(
                decode_chunks(input.as_bytes(), width, input.len()).unwrap(),
                ["{}"]
            );
            assert!(decode_chunks(input.as_bytes(), width, input.len() - 1).is_err());
        }
    }
}
