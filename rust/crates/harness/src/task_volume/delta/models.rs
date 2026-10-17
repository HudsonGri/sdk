use super::*;
use acyclic_stream::MemoryStream;

fn scenario(broken: bool) {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("atomic-model-delta").unwrap();
    let volume =
        loom::future::block_on(Box::pin(TaskVolume::open(provider.clone(), path.clone()))).unwrap();
    let expected = loom::future::block_on(Box::pin(volume.head())).unwrap();
    let mut writes: Vec<_> = (0..2)
        .map(|sequence| {
            let mut write = Write::new(
                FileKey::Task {
                    task: Component::new("task").unwrap(),
                    file: TaskFile::Chunk { turn: 0, sequence },
                },
                b"x".to_vec(),
            );
            write.channel = Some(Channel::Text);
            write
        })
        .collect();
    let peer_provider = provider.clone();
    let peer_path = path.clone();
    let reader = loom::thread::spawn(move || {
        let reopened =
            loom::future::block_on(Box::pin(TaskVolume::open(peer_provider, peer_path))).unwrap();
        let snapshot =
            loom::future::block_on(Box::pin(reopened.snapshot(&Query::Root, 4))).unwrap();
        assert!(
            matches!(snapshot.files.len(), 0 | 2),
            "delta publication was torn"
        );
    });
    if broken {
        model_checking::set_fault(model_checking::Fault::TruncateDelta);
    }
    loom::future::block_on(Box::pin(volume.append_delta_if(
        expected,
        &writes,
        vec![],
        None,
        None,
    )))
    .unwrap();
    reader.join().unwrap();
    // A fresh reader must also see the entire accepted delta, independently of
    // the publishing handle's cache. Both indexes use the production sync shim.
    let reopened = loom::future::block_on(Box::pin(TaskVolume::open(provider, path))).unwrap();
    let snapshot = loom::future::block_on(Box::pin(reopened.snapshot(&Query::Root, 4))).unwrap();
    assert_eq!(
        snapshot.files,
        std::mem::take(&mut writes),
        "delta publication was torn"
    );
}

#[test]
fn competing_reader_sees_no_partial_delta_with_negative_control() {
    loom::model(|| scenario(false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true)))
        .expect_err("a truncated delta was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("delta publication was torn"), "{message}");
}
