use super::*;
use crate::{
    context::ModelContextCapacity,
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn, differential},
};
use futures::FutureExt as _;
use tokio::sync::oneshot;

async fn fixture(count: u64) -> (tempfile::TempDir, LocalHarness, crate::task_volume::Cursor) {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        ScriptedModelProvider::new(std::iter::empty::<ScriptedTurn>()).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            },
        ),
    );
    let model = Model::new(
        "scripted".into(),
        "follow-work".into(),
        "1".into(),
        serde_json::Value::Null,
    )
    .unwrap();
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider)).unwrap(),
    )
    .await
    .unwrap();
    let cursor = harness
        .task_files
        .inner
        .child_lifecycle(None)
        .await
        .unwrap()
        .cursor;
    let mut generation = harness.task_files.inner.head().await.unwrap();
    for input in 0..count {
        let file = FileValue::new(
            FileKey::Child {
                reference: Component::new("worker").unwrap(),
                file: ChildFile::Inbox(Component::new(&format!("input-{input:04}")).unwrap()),
            },
            format!("message {input}").into_bytes(),
        );
        generation = harness
            .task_files
            .inner
            .append(generation, &file)
            .await
            .unwrap();
    }
    (directory, harness, cursor)
}

async fn cancel_cached_read(
    files: &Files,
    stream: &acyclic_stream::LocalStream,
    follow: &mut Follow,
) {
    let (entered, entering) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let writer = stream.test_publication_barrier(entered, released);
    let reader = async {
        entering.await.unwrap();
        assert!(
            next(files, Query::Children, follow)
                .now_or_never()
                .is_none()
        );
        release.send(()).unwrap();
    };
    futures::join!(writer, reader);
    assert!(
        stream.test_publication_available(),
        "cancelled cached read retained its permit"
    );
}

fn one_open(work: u64, check: bool) {
    if check {
        assert_eq!(work, 1, "follow reopened after an already accepted record");
    }
}

#[test]
fn accepted_records_reuse_one_follow_and_cancellation_drops_its_pending_read() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for count in [8, 16] {
        let pair = differential::pair(
            || runtime.block_on(fixture(count)),
            |(_directory, harness, cursor), old| {
                runtime.block_on(async {
                    let files = harness.task_files.as_ref();
                    let mut follow = Follow::new(cursor);
                    let mut output = Vec::new();
                    for _ in 0..count {
                        let change = if old {
                            let mut changes = open(files, Query::Children, follow.cursor.clone())
                                .await
                                .unwrap();
                            let change = changes.next().await.unwrap().unwrap();
                            follow.cursor = change.cursor.clone();
                            change
                        } else {
                            next(files, Query::Children, &mut follow)
                                .await
                                .unwrap()
                                .unwrap()
                        };
                        output.extend(change.file.into_iter().chain(change.files));
                    }
                    assert_eq!(output.len() as u64, count);
                    if !old {
                        cancel_cached_read(files, &harness.task_stream(), &mut follow).await;
                        assert!(follow.idle.is_none());
                    }
                    harness.disconnect().await.unwrap();
                    output
                })
            },
        );
        assert_eq!(
            pair[0].0, pair[1].0,
            "follow changed emitted files or bytes"
        );
        for (index, expected) in [(0, count), (1, 1)] {
            assert_eq!(
                pair[index].1,
                vec![("child-follow-open", 1); expected as usize]
            );
        }
        one_open(pair[1].1.len() as u64, true);
        let old_work = pair[0].1.len() as u64;
        std::panic::catch_unwind(|| one_open(old_work, true))
            .expect_err("repeated anchor validation escaped the work assertion");
        one_open(old_work, false);
    }
}
