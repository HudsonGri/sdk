use super::*;
use acyclic_test_support::differential;
use proptest::prelude::*;

async fn initialized(
    reason: &str,
    io: Option<&PreparationIo>,
) -> Result<(Journal, InteractionId, OperationId, Vec<u8>, u64)> {
    let fixture = Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    fixture.running(TASK).await?;
    let conversation = conversation();
    bind(&fixture, &conversation).await?;
    let files = Arc::new(
        TaskFiles::open(
            fixture.stream.clone(),
            &fixture.storage,
            &fixture.issuer.verifier(&mut None)?,
            &owner_scope(&fixture).await?,
            65_536,
        )
        .await?,
    );
    let journal = journal(&fixture, &conversation, files.clone()).await?;
    let id = InteractionId::from_bytes([86; 16]);
    let operation = OperationId::from_bytes([TASK; 16]);
    let request = Interaction::approval("Install".into(), operation, [5; 32])?;
    journal.open_interaction(id, &request, None)?.await?;
    let request = approvals(&files).await?.remove(0).bytes;
    let sha = FileDescriptor::from_bytes(&request, "application/json", &mut None)?;
    let body = ApprovalDecisionFile::new("human", sha.sha256(), 1, true, Some(reason.into()))?;
    let decision = decision(id, "human", &body);
    journal
        .write_approval_decision(&decision, &responder(&conversation, "human", &[id])?)
        .await?;
    let journal = if io.is_some() {
        journal_with_io(&fixture, &conversation, files, io).await?
    } else {
        journal
    };
    Ok((
        journal,
        id,
        operation,
        decision.bytes.to_vec(),
        request.len() as u64,
    ))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn verdict_reconstructs_once_and_only_flushes_a_resolved_approval(reason in ".{0,256}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for native in [false, true] {
            for pending in [false, true] {
                for duplicate in [false, true] {
                    let (outcome, trace) = differential::observe::<_,_,(&'static str,u64)>(
                        || runtime.block_on(async {
                            let cancellation = acyclic_fs::CancellationToken::new();
                            let work = acyclic_fs::NativeMaterializationWork::new(
                                acyclic_fs::performance::work_budget(65_536, 64 << 20), &cancellation,
                            ).unwrap();
                            let io = native.then(|| PreparationIo::from_native_work(&work).unwrap());
                            let (journal, approved, operation, _, _) = initialized(&reason, io.as_ref()).await.unwrap();
                            let id = if pending {
                                let id = InteractionId::from_bytes([87;16]);
                                journal.open_interaction(id, &Interaction::approval(
                                    "Pending".into(), operation, [5;32],
                                ).unwrap(), io.as_ref()).unwrap().await.unwrap();
                                id
                            } else { approved };
                            drop(io);
                            (journal, id, work)
                        }),
                        |(journal, id, work)| {
                            let io = native.then(|| PreparationIo::from_native_work(&work).unwrap());
                            if duplicate {
                                // Restore the removed, real history reconstruction
                                // before the ordinary verdict/proof path.
                                let host = journal.interactions().unwrap();
                                let aggregate = runtime.block_on(host.aggregate(io.as_ref())).unwrap();
                                host.read_outcome(&aggregate.0, id, io.as_ref()).unwrap();
                            }
                            runtime.block_on(journal.interaction_outcome_with_io(id, io.as_ref())).unwrap()
                        });
                    prop_assert_eq!(outcome, (!pending).then_some(InteractionOutcome::Approved {}));
                    let reconstructions = trace.iter().filter(|(name,_)| *name == "interaction-reconstruction").count();
                    prop_assert_eq!(reconstructions, 1 + usize::from(duplicate));
                    prop_assert_eq!(reconstructions == 1, !duplicate);
                    prop_assert_eq!(trace.iter().filter(|(name,_)| *name == "approval-receipt").count(), usize::from(!pending));
                }
            }
        }
    }
}

fn once(trace: &[(&str, u64)], request: u64, decision: u64, allocations: u64, bytes: u64) -> bool {
    let values = |event| {
        trace
            .iter()
            .filter_map(|(name, count)| (*name == event).then_some(*count))
            .collect::<Vec<_>>()
    };
    values("interaction-file-read") == [request, decision]
        && values("approval-receipt") == [1]
        && values("decision-decode-allocations") == [allocations]
        && values("decision-decode-bytes") == [bytes]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn installation_reads_and_allocates_one_decision(reason in ".{0,1024}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for duplicate in [false, true] {
            // Each run owns fresh conversation, decoder and task-file state.
            let ((result, rejected, expected, request, decision), trace) = differential::observe::<_,_,(&'static str,u64)>(
                || runtime.block_on(async {
                    let ordinary = initialized(&reason, None).await.unwrap();
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let work = acyclic_fs::NativeMaterializationWork::new(
                        acyclic_fs::performance::work_budget(65_536, 64 << 20), &cancellation,
                    ).unwrap();
                    let io = PreparationIo::from_native_work(&work).unwrap();
                    let native = initialized(&reason, Some(&io)).await.unwrap();
                    drop(io);
                    (ordinary, native.0, work)
                }),
                |((journal, id, operation, encoded, request), native_journal, work)| {
                    let expected = acyclic_test_support::measure(|| {
                        drop(crate::contract::json_from_slice::<ApprovalDecisionFile>(&encoded).unwrap());
                    });
                    let rejected = matches!(runtime.block_on(
                        journal.verify_tool_approval_with_io(id, operation, [6;32], None)),
                        Err(Error::Unauthorized(ref reason)) if reason == "approval does not authorize this definition");
                    let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
                    let held = work.live_allocation_bytes();
                    let owned = io.owned_bytes().unwrap();
                    let native = runtime.block_on(native_journal.verify_tool_approval_with_io(
                        id, operation, [6;32], Some(&io),
                    ));
                    let rejected = rejected && matches!(native, Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "approval does not authorize this definition",
                    }))
                        && io.owned_bytes().unwrap() == owned && work.live_allocation_bytes() == held;
                    drop(io);
                    // Negative control restores the redundant verification on
                    // the real path, including its read, decode and allocation.
                    if duplicate {
                        runtime.block_on(journal.verify_tool_approval_with_io(id, operation, [5;32], None)).unwrap();
                    }
                    let result = runtime.block_on(journal.verify_tool_approval_with_io(id, operation, [5;32], None));
                    (result, rejected, expected, request, encoded.len() as u64)
                });
            prop_assert!(result.is_ok());
            prop_assert!(rejected);
            prop_assert!(expected.count_total > 0);
            prop_assert_eq!(once(&trace, request, decision, expected.count_total, expected.bytes_total), !duplicate);
        }
    }
}
