//! Rebinding a captured publication must reuse its authenticated content.
use super::*;

#[tokio::test]
async fn observed_publication_does_not_reread_the_published_payload() -> Result<()> {
    let (_directory, _state, harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let bytes = vec![b'x'; 1024 * 1024];
    let (captured, _) = publisher
        .stage_once(operation, "large.txt", &bytes, "text/plain", "large.txt", None)?
        .await?;
    let view = publisher.tools.workspace.view();
    let writer = publisher.writer_for(&harness.project, view.workspace())?;
    let staged = writer
        .observe_staged_once(operation, publisher.maximum, None)?
        .await?
        .0
        .ok_or_else(|| Error::NotFound("staged publication".into()))?;
    assert_ne!(staged.version(), captured.version(), "fixture must finish its own physical capture");
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut payloads = Vec::new();
    let config = view.workspace().volume_config();
    let mut allocations = acyclic_fs::kernel::allocation::AllocationLedger::default();
    let mut counters = acyclic_fs::WorkCounters::UNCHARGED;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::borrowed_standalone(
        &mut allocations, &mut counters, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation);
    let (path, _allocation) = acyclic_fs::kernel::NamespacePath::parse_in_profile(
        "/large.txt", config.profile, config.limits, &mut accounting, &cancellation)
        .map_err(storage)?;
    for file in [&staged, &captured] {
        let generation = view.workspace().generation(publisher.staged_generation(file)?,
            acyclic_fs::WorkBudget::UNBOUNDED, &cancellation).await.map_err(storage)?.value;
        let records = generation.lookup_paths(std::slice::from_ref(&path),
            acyclic_fs::WorkBudget::UNBOUNDED, &cancellation).await.map_err(storage)?.value;
        payloads.push(records.first().and_then(Option::as_ref).map(|record| record.payload));
    }
    eprintln!("WRITE_REBIND_IDENTITIES equal={}", payloads.first() == payloads.get(1));
    let started = std::time::Instant::now();
    let original = publisher.observed(view, operation, staged.clone()).await?;
    eprintln!("WRITE_REBIND bytes={} elapsed_us={}", bytes.len(), started.elapsed().as_micros());
    assert_eq!(original, captured);
    let bounded = publisher.observed_with_budget(view, operation, staged,
        acyclic_fs::WorkBudget { object_bytes_read: 64 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED }).await;
    assert!(bounded.is_ok(), "rebinding must authenticate metadata without rereading 1 MiB of published content: {bounded:?}");
    assert_eq!(bounded?, captured);
    harness.disconnect_unfinished().await?;
    Ok(())
}
