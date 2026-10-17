use super::*;
use acyclic_stream::MemoryStream;

fn check(broken: bool) {
    loom::model(move || {
        let source = loom::future::block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("shared-observer").unwrap(),
        ))
        .unwrap();
        let observer = loom::future::block_on(source.reopen()).unwrap();
        assert!(Arc::ptr_eq(&source.index, &observer.index));
        let cut = loom::future::block_on(source.head()).unwrap();
        let file = FileValue::new(FileKey::Goal, b"goal".to_vec());
        // Prepare outside the race: only the production commit and cache
        // acknowledgement compete with the production observer refresh.
        let (request, accepted, next) =
            loom::future::block_on(source.prepare_append(cut, &file, Vec::new(), false, None))
                .unwrap()
                .unwrap();
        let writer = loom::thread::spawn(move || {
            if broken {
                loom::future::block_on(source.provider.commit(request)).unwrap();
            } else {
                loom::future::block_on(source.commit_append(cut, request, accepted, next, None))
                    .unwrap();
            }
            source
        });
        let snapshot = loom::future::block_on(observer.snapshot(&Query::Root, 1)).unwrap();
        assert!(snapshot.files.is_empty() || snapshot.files == vec![file.clone()]);
        assert_eq!(
            snapshot.generation.position,
            cut.position + snapshot.files.len() as u64
        );
        let source = writer.join().unwrap();
        // No repair operation: a completed writer must have published the same
        // generation and bytes into every local observer's shared index.
        let index = loom::future::block_on(observer.index.lock());
        assert_eq!(
            source.generation(&index).position,
            next,
            "completed append lost from shared index"
        );
        assert_eq!(
            index.files.get(&file.key).map(|stored| &stored.file),
            Some(&file)
        );
    });
}

#[test]
fn shared_observer_refresh_and_append_are_atomic() {
    check(false);
}

#[test]
#[should_panic(expected = "completed append lost from shared index")]
fn negative_control_detects_missing_cache_acknowledgement() {
    check(true);
}
