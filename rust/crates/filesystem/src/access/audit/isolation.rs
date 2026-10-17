use crate::{
    CancellationToken, Fs, IdempotencyKey, Lifecycle, OperationId, PublicationPermit,
    TransactionCommit, VolumeConfig, VolumeInitialization, WorkBudget, WorkCounters,
    WorkspaceError,
};
use proptest::prelude::*;

async fn trace(
    name: &str,
    mode: u8,
) -> Result<(Vec<WorkCounters>, Vec<Vec<u8>>), Box<dyn std::error::Error>> {
    // Each run has fresh providers. Within a run, the independent reference
    // owner and the tested owner see identical storage; only cache ownership differs.
    let stream = std::sync::Arc::new(acyclic_stream::MemoryStream::default());
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let objects = std::sync::Arc::new(objects);
    let fs = Fs::from_memory_providers(stream.clone(), objects.clone(), bucket.clone());
    let reference = Fs::from_memory_providers(stream, objects, bucket);
    let audit = match mode {
        0 => reference,
        1 => fs.audit_storage(),
        _ => fs.audit_storage_with_shared_caches(),
    };
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            name,
            VolumeInitialization {
                config: VolumeConfig::portable(Lifecycle::Ephemeral),
                access_policy: Some(crate::access::INITIAL_POLICY),
            },
            OperationId::from_bytes([41; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?
        .value
        .workspace();
    let audit = audit
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            format!(
                "acyclic-audit-{}",
                uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes())
            ),
            VolumeInitialization {
                config: VolumeConfig::portable(Lifecycle::Ephemeral),
                access_policy: None,
            },
            OperationId::from_bytes([42; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?
        .value
        .workspace();
    let mut counters = Vec::new();
    for index in 0..3u8 {
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut transaction = workspace
            .begin_transaction(
                IdempotencyKey::from_bytes([index + 1; 16]),
                &work,
                &cancellation,
            )
            .await?;
        transaction
            .create_file(
                &format!("/file-{index}"),
                bytes::Bytes::from_static(b"unchanged bytes"),
                Default::default(),
                &work,
                &cancellation,
            )
            .await?;
        let outcome = transaction
            .commit_with_native_work::<WorkspaceError, _, _>(
                None,
                PublicationPermit::Unrestricted,
                &work,
                &cancellation,
                &mut |_work: &crate::NativeMaterializationWork| async { Ok(()) },
            )
            .await?;
        assert!(matches!(outcome, TransactionCommit::Committed(_)));
        counters.push(work.receipt());
        let mut batch = audit
            .begin_transaction(
                IdempotencyKey::from_bytes([index + 11; 16]),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        batch
            .create_file(
                &format!("/decision-{index}"),
                bytes::Bytes::from_static(b"fixed audit payload"),
                Default::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        let outcome = batch
            .commit::<WorkspaceError, _, _>(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok(crate::OperationReceipt {
                        value: (),
                        work: WorkCounters::UNCHARGED,
                    })
                },
            )
            .await?
            .value;
        assert!(matches!(outcome, TransactionCommit::Committed(_)));
    }
    let generation = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut output = Vec::new();
    for index in 0..3 {
        output.push(
            generation
                .read(
                    &format!("/file-{index}"),
                    1024,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value
                .as_ref()
                .to_vec(),
        );
    }
    Ok((counters, output))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    // Independent stores, identical requests: audit persistence cannot change
    // either the user's bytes or the work charged to user publications.
    #[test]
    fn background_audit_cannot_change_caller_publication_work(suffix in "[a-z]{1,8}") {
        let name = format!("audit-isolation-{suffix}");
        let fs = Fs::memory();
        let source = fs.workspace_id(&name).unwrap().volume_id();
        let audit = fs.workspace_id(format!("acyclic-audit-{}",
            uuid::Uuid::from_bytes(source.into_bytes()))).unwrap().volume_id();
        // This ordering deterministically exposes the extra shared-index probe.
        prop_assume!(source < audit);
        let run = |mode| futures::executor::block_on(trace(&name, mode))
            .map_err(|error| TestCaseError::fail(error.to_string()));
        let baseline = run(0)?;
        let isolated = run(1)?;
        prop_assert_eq!(&isolated, &baseline);
        let coupled = run(2)?;
        prop_assert_eq!(&coupled.1, &baseline.1);
        prop_assert_ne!(&coupled.0, &baseline.0,
            "negative control must expose background cache work charged to the caller");
    }
}
