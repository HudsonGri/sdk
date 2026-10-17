//! The projection uses the same instrumented query/index/provider locks as files.
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn scenario(broken: bool) {
    let volume = Arc::new(
        loom::future::block_on(Box::pin(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("projection-model").unwrap(),
        )))
        .unwrap(),
    );
    let key = FileKey::parse(".ui/config/attention.json").unwrap();
    let query = Query::File(key.clone());
    let first = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
    let second = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
    let peer = Arc::clone(&volume);
    let peer_key = key.clone();
    let writer = loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(publish_changed(
            &peer,
            &first,
            &FileValue::new(peer_key, b"new".to_vec()),
        )))
    });
    let stale = FileValue::new(key, b"old".to_vec());
    let local = loom::future::block_on(Box::pin(publish_changed(&volume, &second, &stale)));
    let remote = writer.join().unwrap();
    // Deliberately broken: retry a stale projection against a fresh cut instead
    // of returning to the caller's canonical-state read. Join before asserting.
    let local = if broken && local.is_err() {
        let cut = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
        loom::future::block_on(Box::pin(publish_changed(&volume, &cut, &stale)))
    } else {
        local
    };
    assert_eq!(
        usize::from(local.is_ok()) + usize::from(remote.is_ok()),
        1,
        "stale projection published across a newer cut"
    );
    let cut = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
    assert_eq!(cut.generation.position, second.generation.position + 1);
    loom::future::block_on(Box::pin(publish_changed(&volume, &cut, &cut.files[0]))).unwrap();
    assert_eq!(
        loom::future::block_on(Box::pin(volume.head())).unwrap(),
        cut.generation,
        "unchanged projection appended another event"
    );
}

#[test]
fn concurrent_projection_writers_retain_their_canonical_state_cut() {
    loom::model(|| scenario(false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true)))
        .expect_err("stale-state retry was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("stale projection published across a newer cut"),
        "{message}"
    );
}
