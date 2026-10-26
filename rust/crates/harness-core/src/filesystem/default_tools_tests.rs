use super::*;
use crate::conversation::ContentGrant;
use crate::{
    AgentId, Capabilities,
    conversation::{MessageKind, VolumeClass, VolumeOwner},
    core::{AggregateKind, AuthorityIssuer, SchemaRegistry},
    filesystem::{WorkspaceMutation, workspace_ref},
    resources::ProviderRef,
};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::MemoryStream;

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(not(target_arch = "wasm32"))]
#[path = "aggregate_custody_tests.rs"]
mod aggregate_custody_tests;

struct Fixture {
    host: Arc<Host>,
    stream: Arc<MemoryStream>,
    parent: StreamAggregate<MemoryStream>,
    issuer: AuthorityIssuer,
    scope: Scope,
    project: VolumeRef,
    source: VolumeRef,
    private: VolumeRef,
    source_generation: GenerationRef,
}

impl Fixture {
    fn controller(
        &self,
    ) -> Result<ParentProjectController<'_, MemoryAuthorityBackend, MemoryObjectBackend>> {
        ParentProjectController::new(
            &self.host,
            self.parent.reducer(),
            &self.issuer.verifier(&mut None)?,
            &self.scope,
            &self.project,
            None,
        )
    }

    fn grant(&self, operation: VolumeOperation) -> Result<ContentGrant> {
        ContentGrant::verify(
            &self.issuer.verifier(&mut None)?,
            &self.scope,
            &self.private,
            operation,
            u64::MAX,
            &mut None,
        )
    }
}

async fn fixture() -> Result<Fixture> {
    let provider = ProviderRef::new(
        "recursive-default-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(
        Fs::memory(),
        &provider,
        None,
        acyclic_fs::WorkspaceLineageStorage::Memory(std::sync::Arc::new(
            acyclic_fs::MemoryWorkspaceLineageStore::new(),
        )),
    )?;
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "recursive-default-parent".into(),
    };
    let project = VolumeRef::new(
        provider.clone(),
        "recursive-default-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("parent".into()),
        &mut None,
    )?;
    let source = VolumeRef::new(
        provider.clone(),
        "recursive-default-source".into(),
        VolumeClass::Project,
        VolumeOwner::Project("child".into()),
        &mut None,
    )?;
    let private = VolumeRef::new(
        provider,
        "recursive-default-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&project).await?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new("recursive-default-owner", [4; 32], authority.clone());
    let mut grants = vec![
        "conversation:bind".to_owned(),
        "conversation:append".to_owned(),
        "fork:publish".to_owned(),
        "project:merge".to_owned(),
    ];
    for volume in [&project, &source, &private] {
        grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
        grants.push(volume.capability(VolumeOperation::Write, &mut None)?);
    }
    let scope = issuer
        .issue(
            "parent-owner",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let stream = Arc::new(MemoryStream::default());
    let mut parent = StreamAggregate::open(
        &stream,
        (
            authority,
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    parent
        .execute(
            Command {
                operation_id: OperationId::from_bytes([2; 16]),
                idempotency_key: IdempotencyKey::new("bind-parent")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            &mut None,
        )
        .await?;
    let project_ref = workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    let base = host
        .apply(
            &project_ref,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"base".to_vec().into(),
            }],
            &IdempotencyKey::new("base-value")?,
        )
        .await?;
    let controller = ParentProjectController::new(
        host.as_ref(),
        parent.reducer(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &project,
        None,
    )?;
    controller
        .fork_project(&base, &source, &IdempotencyKey::new("fork-source")?)
        .await?;
    let source_ref = workspace_ref(source.provider().clone(), &source.storage_name(&mut None)?)?;
    let source_generation = host
        .apply(
            &source_ref,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"child".to_vec().into(),
            }],
            &IdempotencyKey::new("source-value")?,
        )
        .await?;
    Ok(Fixture {
        host,
        stream,
        parent,
        issuer,
        scope,
        project,
        source,
        private,
        source_generation,
    })
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn merge_rejects_foreign_receipts_and_success_shaped_nonoutcomes() -> Result<()> {
    let fixture = fixture().await?;
    let effect = OperationId::from_bytes([31; 16]);
    let file = fixture
        .host
        .put_content(
            &fixture.private,
            &fixture.grant(VolumeOperation::Write)?,
            "merge/notice.txt",
            b"explicit project import",
            "text/plain",
            "notice.txt",
            4096,
            &IdempotencyKey::new("merge-notice")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let controller = fixture.controller()?;
    let inspected = controller
        .prepare_project_import(&fixture.source, &fixture.source_generation, None)
        .await?;
    let plan = RecursiveMergeToolPlan {
        arguments: MergeToolArguments {
            source: TaskId::from_bytes([9; 16]),
            selections: Vec::new(),
        },
        caller: TaskId::from_bytes([8; 16]),
        operation_id: effect,
        destination: fixture.parent.reducer().authority().clone(),
        source: Authority {
            kind: AggregateKind::Conversation,
            id: "recursive-default-child".into(),
        },
        source_project: fixture.source.clone(),
        source_generation: fixture.source_generation.clone(),
        target_project: fixture.project.clone(),
        expected_target_generation: fixture
            .host
            .generation_ref_id(inspected.target_head(), None)?,
        authorized_source: None,
        notice: ConversationMessage {
            id: uuid::Uuid::from_bytes([30; 16]),
            sequence: 1,
            kind: MessageKind::Merge,
            content: file,
            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        },
    };
    assert!(merge_result(&plan, &json!({"status":"success"}), None).is_err());
    assert!(merge_result(&plan, &json!({"status":"no_changes"}), None)?.is_none());
    let outcome = controller
        .apply_project_merge(&inspected, effect, BTreeMap::new())
        .await?
        .0;
    let receipt = controller
        .merge_receipt(&inspected, &outcome, &plan.source, effect, &plan.notice)?
        .0;
    let mut foreign = receipt.clone();
    foreign.operation_id = OperationId::from_bytes([98; 16]);
    assert!(merge_result(&plan, &json!({"status":"applied","receipt":foreign}), None).is_err());
    assert_eq!(
        merge_result(&plan, &json!({"status":"applied","receipt":receipt}), None)?,
        Some(receipt)
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn import_inspection_pins_endpoints_without_spending_join_planning_work() -> Result<()> {
    let fixture = fixture().await?;
    let mut costs = Vec::new();
    let mut targets = Vec::new();
    for inspect in [true, false] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let controller = ParentProjectController::new(
            &fixture.host,
            fixture.parent.reducer(),
            &fixture.issuer.verifier(&mut None)?,
            &fixture.scope,
            &fixture.project,
            Some(&io),
        )?;
        let before = work.receipt();
        let target = if inspect {
            controller
                .inspect_project_import(&fixture.source, &fixture.source_generation)
                .await?
        } else {
            controller
                .prepare_project_import(&fixture.source, &fixture.source_generation, None)
                .await?
                .target_head()
        };
        targets.push(target);
        costs.push(work.receipt().items_examined - before.items_examined);
    }
    assert_eq!(targets[0], targets[1]);
    assert!(
        costs[0] < costs[1],
        "inspection spent join planning work: inspection={} planning={}",
        costs[0],
        costs[1]
    );
    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn import_inspection_keeps_source_and_destination_access_checks() -> Result<()> {
    let fixture = fixture().await?;
    let required = [
        "project:merge".to_owned(),
        fixture
            .project
            .capability(VolumeOperation::Write, &mut None)?,
        fixture
            .source
            .capability(VolumeOperation::Read, &mut None)?,
    ];
    for denied in 0..required.len() {
        let scope = fixture.issuer.issue(
            "inspection-owner",
            &Capabilities::new(
                required
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != denied)
                    .map(|(_, grant)| grant.clone()),
            ),
            fixture.scope.agent(),
            None,
            &mut None,
        )?;
        let controller = ParentProjectController::new(
            &fixture.host,
            fixture.parent.reducer(),
            &fixture.issuer.verifier(&mut None)?,
            &scope,
            &fixture.project,
            None,
        )?;
        assert!(
            controller
                .inspect_project_import(&fixture.source, &fixture.source_generation)
                .await
                .is_err(),
            "inspection accepted missing grant {}",
            required[denied]
        );
    }
    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn import_recovery_reopens_the_original_target_after_publication() -> Result<()> {
    let fixture = fixture().await?;
    let controller = fixture.controller()?;
    let initial = controller
        .prepare_project_import(&fixture.source, &fixture.source_generation, None)
        .await?;
    let expected = fixture
        .host
        .generation_ref_id(initial.target_head(), None)?;
    let operation = OperationId::from_bytes([92; 16]);
    let (published, _) = controller
        .apply_project_merge(&initial, operation, BTreeMap::new())
        .await?;
    assert!(matches!(published, acyclic_fs::JoinOutcome::Applied(_)));
    // The former execution path cannot reopen its pinned target once published.
    assert!(
        controller
            .prepare_project_import(&fixture.source, &fixture.source_generation, Some(&expected))
            .await
            .is_err()
    );
    let recovered = controller
        .prepare_project_import_at(
            &fixture.source,
            &fixture.source_generation,
            &expected,
            operation,
        )
        .await?;
    assert_eq!(recovered.target_head(), initial.target_head());
    let (replayed, _) = controller
        .apply_project_merge(&recovered, operation, BTreeMap::new())
        .await?;
    assert!(matches!(
        replayed,
        acyclic_fs::JoinOutcome::AlreadyApplied(_)
    ));
    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn fork_initialization_retains_raw_child_message_and_independent_source_choice() -> Result<()> {
    let arguments = ForkToolArguments {
        id: AgentId::from_bytes([40; 16]),
        msg: "exact Ã©\0ðŸ¦€\r\nchild task".into(),
        source: Some(TaskId::from_bytes([41; 16])),
    };
    let value = encode(&arguments, None)?;
    recursive_byte_bound(&value, 4096, None)?;
    let retained: ForkToolArguments = decode(&value, None)?;
    assert_eq!(encode(&retained, None)?, value);
    assert_eq!(
        arguments.source_task(
            TaskId::from_bytes([42; 16]),
            crate::conversation::Limits::default(),
            &mut None
        )?,
        TaskId::from_bytes([41; 16])
    );
    let mut oversized = arguments;
    oversized.msg = "x".repeat(4097);
    assert!(recursive_byte_bound(&encode(&oversized, None)?, 4096, None).is_err());
    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn actual_registry_keeps_shared_file_tools_and_refuses_contextless_recursive_execution()
-> Result<()> {
    let ordinary = crate::tools::defaults()?;
    let mut registry =
        recursive_tools::<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>(None, 65536)?;
    registry.extend(crate::tools::defaults()?)?;
    for expected in [
        crate::tools::shell::PLATFORM.name,
        "fs",
        "read",
        "write",
        "edit",
        "acyclic.fork",
        "acyclic.merge",
    ] {
        assert!(registry.get(expected, None, None)?.is_some());
        if let Some(tool) = ordinary.get(expected, None, None)? {
            assert_eq!(
                tool.definition,
                registry.get(expected, None, None)?.unwrap().definition
            );
        }
    }
    for name in [
        crate::tools::shell::PLATFORM.name,
        "fs",
        "acyclic.fork",
        "acyclic.merge",
    ] {
        let tool = registry
            .get(name, None, None)?
            .ok_or_else(|| Error::NotFound(name.into()))?;
        let invocation = ToolInvocation {
            operation_id: OperationId::from_bytes([50; 16]),
            call_id: "exact-call".into(),
            name: name.into(),
            arguments: json!({}),
        };
        assert!(
            tool.executor
                .authorize(None, None, &invocation, None)
                .is_err()
        );
        assert!(
            tool.executor
                .execute(invocation.clone(), None)
                .await
                .is_err()
        );
        assert!(tool.executor.reconcile(invocation, None).await.is_err());
    }
    Ok(())
}


#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn parent_merge_reuses_admitted_authority_and_rechecks_live_trust() -> Result<()> {
    for native in [false, true] {
        let fixture = fixture().await?;
        let cancel = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancel)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native {
            crate::filesystem::PreparationIo::from_native_work(&work)?
        } else {
            crate::filesystem::PreparationIo::from_budget(budget, &cancel)?
        };
        let controller = ParentProjectController::new(
            &fixture.host, fixture.parent.reducer(), &fixture.issuer.verifier(&mut None)?,
            &fixture.scope, &fixture.project, Some(&io),
        )?;
        let (result, signatures): (_, Vec<acyclic_auth::crypto::SignatureVerification>) =
            acyclic_test_support::differential::observe_async(async {
                controller.content_grant(&fixture.private, VolumeOperation::Write, Some(&io))?;
                controller.inspect_project_import(&fixture.source, &fixture.source_generation).await?;
                let plan = controller.prepare_project_import(
                    &fixture.source, &fixture.source_generation, None,
                ).await?;
                controller.apply_project_merge(
                    &plan, OperationId::from_bytes([209; 16]), BTreeMap::new(),
                ).await?;
                Ok::<_, Error>(plan)
            }).await;
        let plan = result?;
        assert!(signatures.is_empty(), "parent merge repeated signatures: native={native} count={}", signatures.len());
        fixture.issuer.revoke_workload(&fixture.scope)?;
        assert!(controller.inspect_project_import(&fixture.source, &fixture.source_generation).await.is_err());
        assert!(controller.content_grant(&fixture.private, VolumeOperation::Write, Some(&io)).is_err());
        assert!(controller.apply_project_merge(&plan, OperationId::from_bytes([210; 16]), BTreeMap::new()).await.is_err());
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn parent_publication_storage_owns_all_buffers_and_releases_partial_copies() -> Result<()> {
    let fixture = fixture().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancel)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    io.retain_owned(17)?;
    let controller = ParentProjectController::new(
        &fixture.host, fixture.parent.reducer(), &verifier, &fixture.scope, &fixture.project, Some(&io),
    )?;
    let before = io.remaining()?;
    let unrelated = io.owned_bytes()?;
    let owner = controller.publication_authority(&io)?;
    let after = io.remaining()?;
    let cuts = before.allocation_operations - after.allocation_operations;
    let funded = before.peak_allocation_bytes - after.peak_allocation_bytes;
    let expected = owner.0.0.logical_owned_bytes()?
        + owner.0.1.logical_owned_bytes()?
        + acyclic_native_runtime::arc_allocation_bytes::<(
            (crate::core::VerifiedScope, VolumeRef), acyclic_fs::NativeAllocation,
        )>() as u64;
    assert_eq!(funded, expected, "parent publication storage charge omitted the Arc header");
    let freed = acyclic_test_support::measure(|| drop(owner)).bytes_current.unsigned_abs();
    assert!(freed > 0 && funded >= freed, "parent publication underfunded: funded={funded} physical={freed}");
    assert_eq!(io.owned_bytes()?, unrelated, "parent publication consumed unrelated custody");
    for cut in 0..cuts {
        let mut limited = budget;
        limited.allocation_operations = budget.allocation_operations - before.allocation_operations + cut;
        let cancel = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(limited, &cancel)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let limited_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        limited_io.retain_owned(17)?;
        let controller = ParentProjectController::new(
            &fixture.host, fixture.parent.reducer(), &verifier, &fixture.scope, &fixture.project, Some(&limited_io),
        )?;
        let owned = limited_io.owned_bytes()?;
        let result = controller.publication_authority(&limited_io);
        assert!(result.is_err(), "parent publication bypassed allocation cut {cut}");
        drop(result);
        assert_eq!(limited_io.owned_bytes()?, owned, "parent publication leaked partial copy {cut}");
        drop(controller);
        assert_eq!(limited_io.owned_bytes()?, 17);
    }
    let foreign_work = acyclic_fs::NativeMaterializationWork::new(budget, &cancel)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let foreign_io = crate::filesystem::PreparationIo::from_native_work(&foreign_work)?;
    assert!(controller.publication_authority(&foreign_io).is_err());
    assert!(controller.content_grant(&fixture.private, VolumeOperation::Write, Some(&foreign_io)).is_err());
    let owner = controller.publication_authority(&io)?;
    fixture.issuer.revoke_workload(&fixture.scope)?;
    assert!(owner.0.0.recheck(&mut Some(&mut io.history_io())).is_err());
    assert!(controller.publication_authority(&io).is_err());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn project_workspace_adapter_keeps_the_authenticated_parent_and_live_revocation() -> Result<()> {
    use crate::merge::ProjectWorkspaceProvider;
    let fixture = fixture().await?;
    let adapter = crate::filesystem::FilesystemProjectWorkspaces::new(
        &fixture.host, fixture.parent.reducer(), &fixture.issuer.verifier(&mut None)?,
        &fixture.scope, fixture.project.clone(),
    )?;
    let source = Authority { kind: AggregateKind::Conversation, id: "adapter-child".into() };
    let (result, signatures): (_, Vec<acyclic_auth::crypto::SignatureVerification>) =
        acyclic_test_support::differential::observe_async(adapter.prepare_project_import(
            &fixture.scope, fixture.parent.reducer(), &source, &fixture.source, &fixture.source_generation,
        )).await;
    result?;
    assert!(signatures.is_empty(), "project workspace adapter repeated signatures: {}", signatures.len());
    fixture.issuer.revoke_workload(&fixture.scope)?;
    assert!(adapter.prepare_project_import(
        &fixture.scope, fixture.parent.reducer(), &source, &fixture.source, &fixture.source_generation,
    ).await.is_err());
    Ok(())
}
