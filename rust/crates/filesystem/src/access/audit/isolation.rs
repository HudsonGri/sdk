use crate::access::{
    Action, Capability, Context, Grant, PathTrie, Policy, Principal, PrincipalKind, Resource,
};
use crate::{
    CancellationToken, Fs, IdempotencyKey, Lifecycle, OperationId, PublicationPermit,
    TransactionCommit, VolumeConfig, VolumeInitialization, WorkBudget, WorkCounters,
    WorkspaceError,
};
use proptest::prelude::*;

async fn trace(
    name: &str,
    audited: bool,
    shared: bool,
) -> Result<(Vec<WorkCounters>, Vec<Vec<u8>>), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let fs = if shared {
        fs.with_shared_audit_caches()
    } else {
        fs
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
    let view = if audited {
        let policy: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
        let principal = Principal {
            kind: PrincipalKind::Service,
            id: "audit-isolation".into(),
            lineage: vec![],
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        };
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: Action::ALL.into_iter().collect(),
                resource: policy.layers[0][0].resource.clone(),
            }]],
            expires_at: None,
        };
        let volume = uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string();
        let view = PathTrie::compile(
            &policy,
            &principal,
            Resource {
                config: workspace.volume_config(),
                volume: &volume,
                lineage: &[],
                generation: 0,
                head: 0,
                path: "",
            },
            Context {
                now: 0,
                approvals: 0,
            },
            &capability,
        )?;
        Some(workspace.audit_view(view, principal, capability, None, None)?)
    } else {
        None
    };
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
        if let Some(view) = &view {
            assert!(
                view.decide(Action::Read, &format!("file-{index}"), 0)?
                    .allowed
            );
            fs.flush_access_audit()?;
        }
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
        let run = |audited, shared| futures::executor::block_on(trace(&name, audited, shared))
            .map_err(|error| TestCaseError::fail(error.to_string()));
        let baseline = run(false, false)?;
        let isolated = run(true, false)?;
        prop_assert_eq!(&isolated, &baseline);
        let coupled = run(true, true)?;
        prop_assert_eq!(&coupled.1, &baseline.1);
        prop_assert_ne!(&coupled.0, &baseline.0,
            "negative control must expose background cache work charged to the caller");
    }
}
