use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

// Production volume/provider synchronization uses query_sync's Loom locks.
const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn scroll_race<const CACHED: bool>() {
    loom::model(|| {
        loom::future::block_on(Box::pin(async {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("scroll-model").unwrap(),
                )
                .await
                .unwrap(),
            );
            let id = uuid::Uuid::from_bytes([1; 16]);
            let mut input = CommandSession::open(volume.clone(), id, 8192, 32)
                .await
                .unwrap();
            input
                .write_state(
                    CommandFileRoot::Session,
                    "layout.json",
                    &json!({"transcript_back":2}),
                )
                .await
                .unwrap();
            let mut owner = CommandSession::open(volume.clone(), id, 8192, 32)
                .await
                .unwrap();
            let peer = loom::thread::spawn(move || {
                loom::future::block_on(Box::pin(async move {
                    owner
                        .update_state(CommandFileRoot::Session, "layout.json", |layout| {
                            layout["peer"] = json!(true);
                            Ok(true)
                        })
                        .await
                        .unwrap();
                }))
            });
            if CACHED {
                // Force the old read/CAS handoff: an owner layout change wins
                // after this observer's snapshot, before its stale write.
                peer.join().unwrap();
                input
                    .write_state(
                        CommandFileRoot::Session,
                        "layout.json",
                        &json!({"transcript_back":5}),
                    )
                    .await
                    .expect("scroll must re-resolve the new cut");
            } else {
                crate::client::scroll_tui(&mut input, [3], 100)
                    .await
                    .unwrap();
                peer.join().unwrap();
            }
            let replay = CommandSession::open(volume, id, 8192, 32).await.unwrap();
            let layout = replay
                .state(CommandFileRoot::Session, "layout.json")
                .unwrap()
                .unwrap();
            assert_eq!(layout["transcript_back"], 5);
            assert_eq!(layout["peer"], true);
        }));
    });
}

#[test]
fn scrolling_and_owner_layout_edits_preserve_both_changes() {
    scroll_race::<false>();
}

#[test]
#[should_panic(expected = "scroll must re-resolve the new cut")]
fn negative_control_cached_layout_fails_after_owner_publication() {
    scroll_race::<true>();
}
