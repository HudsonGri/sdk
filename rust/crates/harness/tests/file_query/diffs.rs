use super::*;
use acyclic_harness::client::{DiffLimits, DiffView};
use proptest::{prelude::*, test_runner::TestRunner};

const LIMITS: DiffLimits = DiffLimits {
    files: 16,
    changes: 16,
    file_bytes: 4096,
    render_bytes: 65_536,
    work: 1_048_576,
};

#[test]
fn oversized_edits_and_restorations_match_the_baseline_without_body_reads()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    struct NoReads;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoReads {}
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(16));
    runner.run(&(0_usize..512, any::<usize>()), |(extra, position)| {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            let baseline = "a".repeat(65_537 + extra);
            let mut edited = baseline.clone();
            let position = position % edited.len();
            edited.replace_range(position..position + 1, "b");
            let fixture = Fixture::new().await?;
            fixture.write(&[("large.txt", Some(&baseline))]).await?;
            let cancel = acyclic_fs::CancellationToken::new();
            let workspace = fixture.filesystem.open_workspace(
                fixture.volume.storage_name(&mut None)?, BOUNDARY_BUDGET, &cancel).await?.value;
            let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
            let mut stream = fixture.source.observe_diff(base.id(), 16).await?;
            let snapshot = stream.next().await.ok_or("snapshot")??;
            let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
            let limits = DiffLimits { file_bytes: 65_536, ..LIMITS };
            view.apply(&snapshot, &NoReads, limits).await?;

            fixture.write(&[("large.txt", Some(&edited))]).await?;
            let changed = stream.next().await.ok_or("edit")??;
            assert!(!changed.paths[0].matches_base, "length alone is insufficient");
            view.apply(&changed, &NoReads, limits).await?;
            assert_eq!(view.files()["large.txt"].omitted_bytes, Some(baseline.len() as u64));

            fixture.write(&[("large.txt", Some(&baseline))]).await?;
            let restored = stream.next().await.ok_or("restore")??;
            assert!(restored.paths[0].matches_base);
            let encoded = serde_json::to_vec(&restored)?;
            assert_eq!(serde_json::from_slice::<acyclic_harness::client::DiffPublication>(&encoded)?, restored);
            // Negative control: discard the authorized equality fact, as the
            // old size-only shortcut did, and the real reducer retains its row.
            let mut broken = view.clone();
            let mut size_only = restored.clone();
            size_only.paths[0].matches_base = false;
            broken.apply(&size_only, &NoReads, limits).await?;
            assert!(!broken.files().is_empty());

            assert_eq!(view.apply(&restored, &NoReads, limits).await?, ["large.txt"]);
            assert!(view.files().is_empty());
            assert_eq!(view.line_counts(), (0, 0));
            let mut reopened = fixture.source.observe_diff(base.id(), 16).await?;
            let snapshot = reopened.next().await.ok_or("reopen")??;
            let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
            replay.apply(&snapshot, &NoReads, limits).await?;
            assert_eq!(replay.files(), view.files());
            Ok(())
        });
        result.map_err(|error| TestCaseError::fail(error.to_string()))
    })?;
    Ok(())
}

#[test]
fn diff_query_omits_large_bodies_and_follows_resize_then_delete()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    struct NoReads;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoReads {}
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(16));
    runner.run(&(16_u64..64, 1_usize..64), |(bound, extra)| {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            for baseline in [None, Some("baseline\n")] {
                let fixture = Fixture::new().await?;
                if baseline.is_some() { fixture.write(&[("note.txt", baseline)]).await?; }
                let cancel = acyclic_fs::CancellationToken::new();
                let workspace = fixture.filesystem.open_workspace(
                    fixture.volume.storage_name(&mut None)?, BOUNDARY_BUDGET, &cancel).await?.value;
                let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
                let mut stream = fixture.source.observe_diff(base.id(), 16).await?;
                let snapshot = stream.next().await.ok_or("snapshot")??;
                let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                let limits = DiffLimits { file_bytes: bound, ..LIMITS };
                view.apply(&snapshot, &NoReads, limits).await?;
                let large = "x".repeat(bound as usize + extra);
                fixture.write(&[("note.txt", Some(large.as_str()))]).await?;
                let added = stream.next().await.ok_or("addition")??;
                assert_eq!(added.paths[0].largest_file_bytes, Some(large.len() as u64));
                view.apply(&added, &NoReads, limits).await?;
                assert_eq!(view.files()["note.txt"].omitted_bytes, Some(large.len() as u64));
                assert!(view.files()["note.txt"].patch.is_empty());
                assert_eq!(view.line_counts(), (0, 0));
                fixture.write(&[("note.txt", Some("small\n"))]).await?;
                let resized = stream.next().await.ok_or("resize")??;
                view.apply(&resized, fixture.reader.as_ref(), limits).await?;
                assert_eq!(view.files()["note.txt"].omitted_bytes, None);
                fixture.write(&[("note.txt", None)]).await?;
                let deleted = stream.next().await.ok_or("deletion")??;
                assert_eq!(deleted.paths.len(), 1);
                assert!(deleted.paths[0].current.is_none());
                let changed = view.apply(&deleted, fixture.reader.as_ref(), limits).await?;
                assert_eq!(changed, ["note.txt"]);
                if baseline.is_none() {
                    assert!(deleted.paths[0].base.is_none());
                    assert!(view.files().is_empty());
                    assert_eq!(view.line_counts(), (0, 0));
                }
                let mut reopened = fixture.source.observe_diff(base.id(), 16).await?;
                let snapshot = reopened.next().await.ok_or("reopen")??;
                let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                replay.apply(&snapshot, fixture.reader.as_ref(), limits).await?;
                assert_eq!(replay.files(), view.files());
            }
            Ok(())
        });
        result.map_err(|error| TestCaseError::fail(error.to_string()))
    })?;
    Ok(())
}

#[test]
fn diff_query_snapshots_and_follow_reconstruct_every_endpoint()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(32));
    runner.run(
        &prop::collection::vec("[a-zλ雪\\n]{0,32}", 1..6),
        |texts| {
            let result: std::result::Result<(), Box<dyn std::error::Error>> =
                runtime.block_on(async {
                    for baseline_text in [None, Some("baseline\n")] {
                    let fixture = Fixture::new().await?;
                    if baseline_text.is_some() {
                        fixture.write(&[("note.txt", baseline_text)]).await?;
                    }
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let workspace = fixture
                        .filesystem
                        .open_workspace(
                            fixture.volume.storage_name(&mut None)?,
                            BOUNDARY_BUDGET,
                            &cancellation,
                        )
                        .await?
                        .value;
                    let baseline = workspace.head(BOUNDARY_BUDGET, &cancellation).await?.value;
                    let mut stream = fixture.source.observe_diff(baseline.id(), 16).await?;
                    let first = stream.next().await.ok_or("missing snapshot")??;
                    assert!(first.paths.is_empty());
                    let mut view = DiffView::new(fixture.volume.clone(), first.previous.clone())?;
                    view.apply(&first, fixture.reader.as_ref(), LIMITS).await?;
                    let values = texts
                        .iter()
                        .map(|text| Some(text.as_str()))
                        .chain(std::iter::once(None))
                        .chain(baseline_text.map(Some));
                    let mut current_text = baseline_text;
                    for text in values {
                        if text == current_text {
                            let error = fixture.write(&[("note.txt", text)]).await.unwrap_err();
                            assert!(error.to_string().contains("no pending mutation"));
                            assert!(stream.next().now_or_never().is_none());
                            continue;
                        }
                        fixture.write(&[("note.txt", text)]).await?;
                        current_text = text;
                        let publication = stream.next().await.ok_or("missing delta")??;
                        assert_eq!(&publication.previous, view.generation());
                        let bytes = serde_json::to_vec(&publication)?;
                        let decoded = serde_json::from_slice(&bytes)?;
                        assert_eq!(publication, decoded);
                        view.apply(&publication, fixture.reader.as_ref(), LIMITS)
                            .await?;
                        if text == baseline_text {
                            assert!(view.files().is_empty());
                        } else {
                            let patch = diffy::Patch::from_str(&view.files()["note.txt"].patch)?;
                            assert_eq!(
                                diffy::apply(baseline_text.unwrap_or_default(), &patch)?,
                                text.unwrap_or_default()
                            );
                        }
                        let mut reopened = fixture.source.observe_diff(baseline.id(), 16).await?;
                        let snapshot =
                            reopened.next().await.ok_or("reopened snapshot missing")??;
                        let mut replay =
                            DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                        replay
                            .apply(&snapshot, fixture.reader.as_ref(), LIMITS)
                            .await?;
                        assert_eq!(replay.files(), view.files());
                    }
                    }
                    Ok(())
                });
            result.map_err(|error| TestCaseError::fail(error.to_string()))
        },
    )?;
    Ok(())
}

#[tokio::test]
async fn diff_query_rejects_truncation_instead_of_publishing_partial_paths()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await?;
    fixture
        .write(&[("a", Some("base")), ("b", Some("base"))])
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let workspace = fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &cancellation,
        )
        .await?
        .value;
    let base = workspace.head(BOUNDARY_BUDGET, &cancellation).await?.value;
    fixture
        .write(&[("a", Some("changed")), ("b", Some("changed"))])
        .await?;
    assert!(fixture.source.observe_diff(base.id(), 1).await.is_err());
    assert!(fixture.source.observe_diff(base.id(), 0).await.is_err());
    Ok(())
}
