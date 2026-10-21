use super::*;
use crate::facade::FsWork as _;
use crate::{GitIgnorePolicy, NameWalk, PublicationPermit};

async fn names_hydrate(n: usize, broken: bool) -> usize {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"candidate content"));
    source.directory_entries = n;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse("file-0001.txt\n"))
        .await
        .unwrap();
    let mut seen = 0;
    while let Some(page) = walk.next_page().await.unwrap() {
        for name in page {
            assert!(name.regular);
            if broken {
                walk.read(&name.path, 1024).await.unwrap();
            }
            seen += 1;
        }
    }
    assert_eq!(seen, n - usize::from(n > 1));
    source.counts.ranges.load(Ordering::Relaxed)
}

#[tokio::test]
async fn name_search_does_not_hydrate_lazy_candidates() {
    assert_eq!(names_hydrate(64, false).await, 0);
}

#[tokio::test]
async fn name_search_hydration_counter_detects_content_reads() {
    assert!(names_hydrate(64, true).await > 0);
}

#[tokio::test]
async fn name_search_million_generated_entries_are_paged_without_retaining_the_tree() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"never hydrate"));
    source.directory_entries = 1_000_000;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut total = 0;
    let mut largest = 0;
    while let Some(page) = walk.next_page().await.unwrap() {
        total += page.len();
        largest = largest.max(page.len());
        assert!(
            walk.pending_directories() <= 1,
            "flat generated source retained a namespace frontier"
        );
    }
    assert_eq!(total, 1_000_000);
    assert_eq!(largest, 128);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn name_search_explicit_symlink_is_never_a_regular_candidate() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"/loop"));
    source.node.kind = SourceNodeKind::SymbolicLink;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, Some("loop"), GitIgnorePolicy::parse(""))
        .await
        .unwrap();
    let page = loop {
        let page = walk.next_page().await.unwrap().unwrap();
        if !page.is_empty() {
            break page;
        }
    };
    assert_eq!(page.len(), 1);
    assert!(!page[0].regular);
    assert!(!page[0].directory);
    assert!(walk.next_page().await.unwrap().is_none());
    assert!(matches!(
        walk.read("loop", 1024).await,
        Err(crate::SearchError::Lazy(
            crate::LazyWorkspaceError::NotRegularFile
        ))
    ));
    assert_eq!(source.counts.links.load(Ordering::Relaxed), 0);
    // Negative control: a generic link reader reaches the provider, which
    // this fixture deliberately rejects. Search must reject before that call.
    assert!(
        lazy.read_link(
            "/loop",
            1024,
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new())
        )
        .await
        .is_err()
    );
    assert_eq!(source.counts.links.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn name_search_index_pages_survive_reopen_without_relisting_or_hydrating() {
    use crate::{NameIndex, NameIndexManifest};
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"unread contents"));
    source.directory_entries = 513;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse(""))
        .await
        .unwrap();
    let page = loop {
        let page = walk.next_page().await.unwrap().unwrap();
        if !page.is_empty() {
            break page;
        }
    };
    assert_eq!(page.len(), 128);
    let expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let manifest = NameIndexManifest {
        schema: 1,
        cut: walk.cut(),
        observed_generation: expected.id(),
        pages: 1,
        pending_directories: walk.pending_directories(),
        complete: false,
        checkpoint: walk.checkpoint().unwrap(),
    };
    NameIndex::publish(lazy.workspace(), &expected, manifest, Some(&page))
        .await
        .unwrap()
        .unwrap();
    drop(walk);
    let before = source.counts.pages.load(Ordering::Relaxed);
    let index = NameIndex::open(lazy.workspace()).await.unwrap().unwrap();
    for _ in 0..4 {
        assert_eq!(index.page(0).await.unwrap().len(), 128);
    }
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
    let mut resumed =
        NameWalk::resume(&lazy, index.manifest.checkpoint, GitIgnorePolicy::parse(""))
            .await
            .unwrap();
    let mut remaining = 0;
    while let Some(page) = resumed.next_page().await.unwrap() {
        remaining += page.len();
    }
    assert_eq!(remaining, 513 - 128);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn name_search_checkpoint_rejects_another_volume() {
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::new())),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let checkpoint = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse(""))
        .await
        .unwrap()
        .checkpoint()
        .unwrap();
    let other_fs = Fs::memory();
    let other = attach(
        &other_fs,
        Arc::new(CountingSource::new(Bytes::new())),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    assert!(
        NameWalk::resume(&other, checkpoint, GitIgnorePolicy::parse(""))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn name_search_nested_ignore_rules_survive_checkpoint_and_are_relative() {
    let fs = Fs::memory();
    let stop = CancellationToken::new();
    let workspace = fs
        .create_workspace("nested-ignores", WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let bytes = |text: &'static [u8]| acyclic_stream::OwnedBytes::from_static(text);
    transaction
        .write_batch(
            &["/src", "/src/deep", "/src/cache", "/else"],
            &[
                (
                    "/src/.gitignore",
                    bytes(b"!visible.txt\n/only-here.rs\ncache/\n"),
                ),
                ("/src/visible.txt", bytes(b"visible")),
                ("/src/private.txt", bytes(b"ignored")),
                ("/src/only-here.rs", bytes(b"ignored")),
                ("/src/deep/only-here.rs", bytes(b"visible")),
                ("/src/cache/entry.rs", bytes(b"ignored subtree")),
                ("/else/only-here.rs", bytes(b"visible")),
                ("/else/.ignore", bytes(b"!unignored.txt\n")),
                ("/else/unignored.txt", bytes(b"visible")),
            ],
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    for control in [false, true] {
        if control {
            workspace.remove("/src/.gitignore").await.unwrap();
        }
        let head = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let mut walk = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(
            head,
            None,
            GitIgnorePolicy::parse("*.txt\n"),
        )
        .await
        .unwrap();
        let mut paths = Vec::new();
        while let Some(page) = walk.next_page().await.unwrap() {
            paths.extend(
                page.into_iter()
                    .filter(|name| name.regular)
                    .map(|name| name.path),
            );
            // Exercise the real serialized frontier, including loaded policy
            // metadata, at every directory continuation.
            let checkpoint = walk.checkpoint().unwrap();
            walk =
                NameWalk::resume_exact(&workspace, checkpoint, GitIgnorePolicy::parse("*.txt\n"))
                    .await
                    .unwrap();
        }
        assert_eq!(paths.iter().any(|path| path == "src/visible.txt"), !control);
        assert_eq!(paths.iter().any(|path| path == "src/only-here.rs"), control);
        assert_eq!(
            paths.iter().any(|path| path == "src/cache/entry.rs"),
            control
        );
        assert!(!paths.iter().any(|path| path == "src/private.txt"));
        assert!(paths.iter().any(|path| path == "src/deep/only-here.rs"));
        assert!(paths.iter().any(|path| path == "else/only-here.rs"));
        assert!(paths.iter().any(|path| path == "else/unignored.txt"));
    }
}

#[tokio::test]
async fn name_search_uses_authored_metadata_before_the_unindexed_source() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"unread"));
    source.directory_entries = 1_000_000;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    lazy.write(
        "/near.txt",
        Bytes::from_static(b"overlay").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let first = walk.next_page().await.unwrap().unwrap();
    assert_eq!(
        first
            .iter()
            .map(|name| name.path.as_str())
            .collect::<Vec<_>>(),
        ["near.txt"]
    );
    assert_eq!(
        source.counts.pages.load(Ordering::Relaxed),
        0,
        "authored seed waited for source listing"
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    // Negative control: merged directory enumeration starts with the unindexed
    // source. It cannot provide the known authored file immediately.
    let cut = lazy.snapshot(accounting!()).await.unwrap();
    let (page, _, _) = lazy
        .list_directory_at_snapshot(cut, "/", None, 128, accounting!())
        .await
        .unwrap();
    assert!(
        !page
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"near.txt")
    );
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn name_search_index_publication_cannot_rebase_over_a_workspace_write() {
    use crate::{NameIndex, NameIndexManifest};
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::new())),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let page = walk.next_page().await.unwrap().unwrap();
    let expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let manifest = NameIndexManifest {
        schema: 1,
        cut: walk.cut(),
        observed_generation: expected.id(),
        pages: 1,
        pending_directories: walk.pending_directories(),
        complete: false,
        checkpoint: walk.checkpoint().unwrap(),
    };
    lazy.write(
        "/concurrent.txt",
        Bytes::from_static(b"written").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let current = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    assert_ne!(expected.id(), current.id());
    assert!(
        NameIndex::publish(lazy.workspace(), &expected, manifest.clone(), Some(&page))
            .await
            .unwrap()
            .is_none()
    );
    // Negative control: the same index bytes are valid under the current head.
    assert!(
        NameIndex::publish(lazy.workspace(), &current, manifest, Some(&page))
            .await
            .unwrap()
            .is_some()
    );
}
