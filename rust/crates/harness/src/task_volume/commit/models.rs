use super::*;
use acyclic_stream::MemoryStream;
use std::{
    future::Future as _,
    task::{Context, Waker},
};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn competing_batch(broken: bool) {
    loom::model(move || {
        let volume = Arc::new(
            loom::future::block_on(Box::pin(TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("batch-fence-model").unwrap(),
            )))
            .unwrap(),
        );
        let cut = loom::future::block_on(Box::pin(volume.head())).unwrap();
        let writer = volume.clone();
        model_checking::pause_next_publication();
        let mut single = Box::pin(async move {
            let write = FileValue::new(
                FileKey::parse(".ui/config/peer.json").unwrap(),
                b"peer".to_vec(),
            );
            writer.append(cut, &write).await
        });
        assert!(
            single
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let writes = ["first", "second"].map(|name| {
            FileValue::new(
                FileKey::parse(&format!(".ui/config/{name}.json")).unwrap(),
                name.as_bytes().to_vec(),
            )
        });
        model_checking::pause_next_publication();
        let mut batch = Box::pin(volume.append_many(cut, &writes));
        assert!(
            batch
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if broken {
            model_checking::set_fault(model_checking::Fault::OmitPublicationFence);
        }
        let other = loom::thread::spawn(move || {
            if broken {
                model_checking::set_fault(model_checking::Fault::OmitPublicationFence);
            }
            loom::future::block_on(single).is_ok()
        });
        let accepted = loom::future::block_on(batch).is_ok();
        let peer = other.join().unwrap();
        assert_ne!(
            accepted, peer,
            "one generation fence has exactly one winner"
        );
        let snapshot = loom::future::block_on(Box::pin(volume.snapshot(&Query::Root, 3))).unwrap();
        assert_eq!(snapshot.files.len(), if accepted { 2 } else { 1 });
        assert_eq!(
            snapshot.generation.position,
            cut.position + snapshot.files.len() as u64
        );
    });
}

#[test]
fn batch_and_single_write_share_the_publication_fence() {
    competing_batch(false);
}

#[test]
fn missing_batch_generation_fence_is_detected_negative_control() {
    let caught = std::panic::catch_unwind(|| competing_batch(true));
    let panic = caught.expect_err("removing the production publication fence must fail");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("exactly one winner"), "{message}");
}
