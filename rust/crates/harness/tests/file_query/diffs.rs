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
