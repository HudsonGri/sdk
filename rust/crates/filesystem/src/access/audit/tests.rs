use super::*;
use crate::access::{Context, PathTrie, Policy, PrincipalKind, Resource};
use crate::{
    Fs, Lifecycle, OperationId, VolumeConfig, VolumeInitialization, WorkBudget, WorkspaceError,
};
use proptest::prelude::*;

fn check(records: &[serde_json::Value], expected: &[bool]) -> Result<(), TestCaseError> {
    prop_assert_eq!(records.len(), expected.len());
    for (index, (record, allowed)) in records.iter().zip(expected).enumerate() {
        prop_assert_eq!(record["sequence"].as_u64(), Some(index as u64));
        prop_assert!(
            record["writer_id"]
                .as_str()
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
        );
        prop_assert_eq!(
            &record["resource"]["native_path"][0][1],
            &serde_json::to_value(if *allowed { b"public" } else { b"secret" })?
        );
        prop_assert_eq!(record["allowed"].as_bool(), Some(*allowed));
        prop_assert_eq!(&record["principal"]["id"], "audit-owner");
        prop_assert_eq!(record["policy_version"].as_u64(), Some(1));
        prop_assert!(record["capability_chain"]["layers"].is_array());
        prop_assert!(
            record["credential_fingerprint"]
                .as_array()
                .is_some_and(|bytes| bytes.len() == 32)
        );
        prop_assert_eq!(
            record["resource"]["native_path"].as_array().unwrap().len(),
            1
        );
        prop_assert!(record["rule_id"].is_string());
        // File records are ordinary JSON, with lossless codec round trips.
        prop_assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&serde_json::to_vec(record)?)?,
            record.clone()
        );
    }
    Ok(())
}

async fn records(expected: &[bool]) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let principal = Principal {
        kind: PrincipalKind::Service,
        id: "audit-owner".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let mut policy: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    let mut deny = policy.layers[0][0].clone();
    deny.id = "secret".into();
    deny.effect = crate::access::Effect::Deny;
    deny.resource.path = "secret".into();
    policy.layers[0].push(deny);
    let capability = Capability {
        layers: vec![vec![crate::access::Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: policy.layers[0][0].resource.clone(),
        }]],
        expires_at: None,
    };
    let policy_bytes = serde_json::to_vec(&policy)?;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "audit",
            VolumeInitialization {
                config: VolumeConfig::portable(Lifecycle::Ephemeral),
                access_policy: Some(&policy_bytes),
            },
            OperationId::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?
        .value
        .workspace();
    let volume = uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string();
    let view = PathTrie::compile(
        &policy,
        &principal,
        Resource {
            config: workspace.volume_config(),
            volume: &volume,
            lineage: &[],
            generation: 1,
            head: 1,
            path: "",
        },
        Context {
            now: 0,
            approvals: 0,
        },
        &capability,
    )?;
    assert!(view.audit_binding(&principal, &capability)?);
    let mut forged = principal.clone();
    forged.tags.insert("unauthenticated".into());
    assert!(!view.audit_binding(&forged, &capability)?);
    let mut forged = capability.clone();
    forged.expires_at = Some(2);
    assert!(!view.audit_binding(&principal, &forged)?);
    let mut checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let (_, revision) = checkout
        .read_access_policy(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let generation_id = checkout.generation_id();
    let view = Arc::new(
        workspace.audit_view(
            view.with_policy_revision(generation_id, revision)
                .with_data_generation(generation_id),
            principal.clone(),
            capability.clone(),
            Some([1; 32]),
            None,
        )?,
    );
    let mut funding = Vec::new();
    for (index, &allowed) in expected.iter().enumerate() {
        let name = if allowed { "public" } else { "secret" };
        let decision = match index % 4 {
            0 => view.decide(Action::Read, name, 1)?,
            1 => view.prepare(name)?.decide(Action::Read, 1)?,
            2 => {
                let path = crate::kernel::NamespacePath::from_portable(
                    &crate::path::PortablePath::parse(
                        &format!("/{name}"),
                        workspace.volume_config().limits,
                    )?,
                    workspace.volume_config().limits,
                )?;
                let pinned = view.pin(&path)?;
                let decision = pinned.decide(Action::Read, 1)?;
                assert_eq!(decision.allowed, allowed);
                continue;
            }
            _ => {
                let work =
                    crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                        .map_err(|(error, _)| error)?;
                let live = work.live_allocation_bytes();
                let before = work.receipt();
                let prepared = view.prepare_with_work(name, &work)?;
                let captured = work.receipt();
                assert_eq!(
                    captured.allocation_operations,
                    before.allocation_operations + 2
                );
                let decision = prepared.decide(Action::Read, 1)?;
                assert_eq!(
                    work.receipt(),
                    captured,
                    "hot decision allocated or copied storage"
                );
                assert_eq!(decision.allowed, allowed);
                // The same exact-work comparison rejects redundant preparation.
                let redundant = view.prepare_with_work(name, &work)?;
                assert_ne!(
                    work.receipt(),
                    captured,
                    "negative control did not add capture work"
                );
                drop(redundant);
                drop(prepared);
                funding.push((work, live));
                continue;
            }
        };
        assert_eq!(decision.allowed, allowed);
    }
    let before = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    fs.flush_access_audit()?;
    for (work, live) in funding {
        assert_eq!(
            work.live_allocation_bytes(),
            live,
            "persisted audit retained request custody"
        );
    }
    let after = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    assert_eq!(
        before.id(),
        after.id(),
        "audit cannot publish into the project checkout"
    );
    let audit = writer::sink(
        &fs,
        workspace.id().volume_id(),
        workspace.volume_config().lifecycle,
    )
    .await?;
    assert!(
        audit
            .clone()
            .bind_coarse_access(&[Action::Read], &cancellation)
            .await
            .is_err(),
        "project credentials cannot disclose audit paths"
    );
    let generation = audit
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let page = generation
        .list_directory(
            writer::DIRECTORY,
            None,
            64,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value
        .0;
    let mut output: Vec<serde_json::Value> = Vec::new();
    // Operation ids are random. Each batch has no cross-file ordering contract;
    // recover admission order using the recorder sequence, never file names.
    for entry in page.entries {
        let name = std::str::from_utf8(entry.name.as_bytes())?;
        let bytes = generation
            .read(
                &format!("{}/{name}", writer::DIRECTORY),
                16_777_216,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        for line in bytes
            .value
            .as_ref()
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            output.push(serde_json::from_slice(line)?);
        }
    }
    for record in &output {
        assert_eq!(
            record["capability_chain"],
            serde_json::to_value(&capability)?
        );
        assert_eq!(
            record["resource"]["policy_revision"],
            serde_json::to_value(generation_id)?
        );
        assert_eq!(
            record["resource"]["data_revision"],
            serde_json::to_value(generation_id)?
        );
    }
    output.sort_by_key(|record: &serde_json::Value| record["sequence"].as_u64());
    Ok(output)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn each_allow_and_deny_has_exactly_one_volume_record(expected in prop::collection::vec(any::<bool>(), 1..33)) {
        let records = futures::executor::block_on(records(&expected)).map_err(|error| TestCaseError::fail(error.to_string()))?;
        check(&records, &expected)?;
        let mut missing = records.clone(); missing.pop();
        prop_assert!(check(&missing, &expected).is_err(), "negative control must detect a missing decision");
        let mut duplicate = records.clone(); duplicate.push(records[0].clone());
        prop_assert!(check(&duplicate, &expected).is_err(), "negative control must detect a duplicate decision");
    }
}
