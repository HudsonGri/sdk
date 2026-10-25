use super::*;
use crate::facade::FsWork;

async fn prepare<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    parent: &Workspace<A, O>,
    child: &Workspace<A, O>,
    permit: crate::PublicationPermit,
) -> Result<crate::PreparedJoin<A, O>, WorkspaceError> {
    let plan = unbounded!(child.join_into(parent), plan())?;
    let expected_target = plan.target_head();
    unbounded!(
        plan,
        prepare(
            ApplyOptions {
                if_target: expected_target,
                idempotency_key: IdempotencyKey::new()
            },
            permit
        )
    )
}

#[cfg(feature = "local")]
#[tokio::test]
async fn prepared_join_checkpoint_survives_restart_collection_and_exact_retirement()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let options = crate::LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let checkpoint;
    {
        let fs = Fs::local(options.clone()).await?;
        let parent = unbounded!(fs, create_workspace("parent"))?;
        let child = fork_workspace(
            &parent,
            "child",
            ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
        )
        .await?;
        child.write_text("/child", "child").await?;
        parent.write_text("/parent", "parent").await?;
        let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
        checkpoint = unbounded!(prepared, checkpoint())?;
        assert_eq!(
            checkpoint.candidate,
            prepared.candidate().ok_or("missing candidate")?
        );
        let encoded = serde_json::to_vec(&checkpoint)?;
        assert_eq!(
            serde_json::from_slice::<crate::PreparedJoinCheckpoint>(&encoded)?,
            checkpoint
        );
        drop((prepared, child, parent));
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing release barrier")?;
        drop(fs);
        released.wait_async().await;
    }
    let fs = Fs::local(options).await?;
    let parent = unbounded!(fs, open_workspace("parent"))?;
    fs.collect_local_garbage(None, &cancellation).await?;
    let resumed = unbounded!(parent, resume_prepared_join(&checkpoint))?;
    let budget = WorkBudget {
        object_bytes_written: 0,
        page_writes: 0,
        ..WorkBudget::UNBOUNDED
    };
    let publication = resumed.publish(budget, &cancellation).await?;
    publication.work.verify(budget)?;
    let JoinOutcome::Applied(application) = publication.value else {
        return Err("missing publication".into());
    };
    assert_eq!(application.generation.id(), checkpoint.candidate);
    assert_eq!(unbounded!(parent, read("/child", 16))?.as_ref(), b"child");
    assert_eq!(unbounded!(parent, read("/parent", 16))?.as_ref(), b"parent");
    unbounded!(parent, release_prepared_join(&checkpoint))?;
    fs.collect_local_garbage(None, &cancellation).await?;
    assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_err());
    unbounded!(parent, release_prepared_join(&checkpoint))?;
    Ok(())
}

#[tokio::test]
async fn prepared_join_checkpoint_cannot_resume_after_its_exact_retirement()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("parent"))?;
    let child = fork_workspace(
        &parent,
        "child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
    )
    .await?;
    child.write_text("/child", "child").await?;
    let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
    let checkpoint = unbounded!(prepared, checkpoint())?;
    assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_ok());
    unbounded!(parent, release_prepared_join(&checkpoint))?;
    assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_err());
    assert!(unbounded!(prepared, checkpoint()).is_err());
    Ok(())
}

#[tokio::test]
async fn prepared_join_checkpoint_authenticates_every_publication_field_and_retention_class()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("parent"))?;
    let child = fork_workspace(
        &parent,
        "child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
    )
    .await?;
    child.write_text("/child", "child").await?;
    let permit = crate::PublicationPermit::Lease {
        authority_id: [11; 16],
        workspace_id: [12; 16],
        lease_id: [13; 16],
        expires_at_millis: 14,
    };
    let prepared = prepare(&parent, &child, permit).await?;
    let checkpoint = unbounded!(prepared, checkpoint())?;
    for field in 0..13 {
        let mut forged = checkpoint;
        match field {
            0 => forged.target = WorkspaceId::from_bytes([71; 16]),
            1 => forged.expected.epoch = crate::Epoch::new(72)?,
            2 => forged.expected.sequence = crate::Sequence::new(73),
            3 => forged.expected.digest = crate::Digest::from_bytes([74; 32]),
            4 => forged.operation = crate::OperationId::from_bytes([75; 16]),
            5 => forged.candidate = GenerationId::new(crate::Digest::from_bytes([76; 32])),
            6 => forged.context = crate::Digest::from_bytes([77; 32]),
            7 => forged.resolutions = crate::Digest::from_bytes([78; 32]),
            8 => forged.permit = crate::PublicationPermit::Unrestricted,
            _ => {
                let crate::PublicationPermit::Lease {
                    authority_id,
                    workspace_id,
                    lease_id,
                    expires_at_millis,
                } = &mut forged.permit
                else {
                    unreachable!();
                };
                match field {
                    9 => authority_id[0] ^= 1,
                    10 => workspace_id[0] ^= 1,
                    11 => lease_id[0] ^= 1,
                    _ => *expires_at_millis += 1,
                }
            }
        }
        assert!(
            unbounded!(parent, resume_prepared_join(&forged)).is_err(),
            "forged field {field}"
        );
        assert!(
            unbounded!(parent, release_prepared_join(&forged)).is_err(),
            "forged release {field}"
        );
    }
    // A public generation pin is not a planner-issued preparation, even when
    // its generation and label are exactly those of the forged descriptor.
    let mut forged = checkpoint;
    forged.operation = crate::OperationId::from_bytes([79; 16]);
    let cancellation = CancellationToken::new();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let label = forged.label(&mut accounting)?;
    let generation = unbounded!(parent, generation(checkpoint.candidate))?;
    unbounded!(generation, pin(&label.value))?;
    label.retire(&mut accounting)?;
    assert!(unbounded!(parent, resume_prepared_join(&forged)).is_err());
    assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_ok());
    unbounded!(parent, release_prepared_join(&checkpoint))?;
    // The authentic creation still exists after release. Only the terminal
    // retention head prevents this exact descriptor from being resumed.
    assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_err());
    assert!(unbounded!(prepared, checkpoint()).is_err());
    let reservation = crate::PublicationPermit::Reservation {
        operation_id: [81; 16],
        gate_tail: 82,
        expected: checkpoint.expected,
    };
    let prepared = prepare(&parent, &child, reservation).await?;
    let checkpoint = unbounded!(prepared, checkpoint())?;
    for field in 0..5 {
        let mut forged = checkpoint;
        let crate::PublicationPermit::Reservation {
            operation_id,
            gate_tail,
            expected,
        } = &mut forged.permit
        else {
            unreachable!();
        };
        match field {
            0 => operation_id[0] ^= 1,
            1 => *gate_tail += 1,
            2 => expected.epoch = crate::Epoch::new(83)?,
            3 => expected.sequence = crate::Sequence::new(84),
            _ => expected.digest = crate::Digest::from_bytes([85; 32]),
        }
        assert!(
            unbounded!(parent, resume_prepared_join(&forged)).is_err(),
            "reservation field {field}"
        );
    }
    assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_ok());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn prepared_join_checkpoint_uses_original_custody_at_every_allocation_cut()
-> Result<(), Box<dyn Error>> {
    assert_checkpoint_custody(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn pending_join_checkpoint_uses_original_custody_at_every_allocation_cut()
-> Result<(), Box<dyn Error>> {
    assert_checkpoint_custody(true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn assert_checkpoint_custody(conflicted: bool) -> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let mut required = None;
    let mut setup = 0;
    let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
    // First measure the production operation, then deny each individual
    // allocation at its original boundary, including store-owned allocations.
    let mut cut = 0;
    loop {
        let limit = required.map_or(u64::MAX, |_| setup + cut);
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: limit,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        {
            let fs = Fs::memory();
            let parent = unbounded!(fs, create_workspace("parent"))?;
            let child = fork_workspace(
                &parent,
                "child",
                ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
            )
            .await?;
            child.write_text("/child", "child").await?;
            if conflicted {
                parent.write_text("/collision", "ours").await?;
                child.write_text("/collision", "theirs").await?;
            }
            let plan = unbounded!(child.join_into(&parent), plan())?;
            let expected = plan.target_head();
            let prepared = plan
                .prepare(
                    ApplyOptions {
                        if_target: expected,
                        idempotency_key: IdempotencyKey::new(),
                    },
                    crate::PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                )
                .await?;
            let before = original.receipt().allocation_operations;
            if required.is_none() {
                setup = before;
            } else {
                assert_eq!(before, setup);
            }
            let result = prepared.checkpoint(&original, &cancellation).await;
            if required.is_none() {
                let checkpoint = result?;
                required = Some(original.receipt().allocation_operations - setup);
                assert!(required.is_some_and(|count| count > 0));
                let foreign =
                    crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                        .map_err(|(error, _)| error)?;
                assert!(prepared.checkpoint(&foreign, &cancellation).await.is_err());
                assert!(unbounded!(prepared, checkpoint()).is_err());
                let resumed = parent
                    .resume_prepared_join(&checkpoint, &original, &cancellation)
                    .await?;
                assert_eq!(resumed.candidate(), prepared.candidate());
                assert!(
                    resumed
                        .publish(WorkBudget::UNBOUNDED, &cancellation)
                        .await
                        .is_err()
                );
            } else {
                assert!(result.is_err(), "allocation cut {cut} was bypassed");
                assert!(original.receipt().allocation_operations <= limit);
                cut += 1;
            }
        }
        assert_eq!(
            original.live_allocation_bytes(),
            baseline,
            "cut {cut} leaked custody"
        );
        if Some(cut) == required {
            break;
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_join_publishes_without_writing_or_planning_another_candidate()
-> Result<(), Box<dyn Error>> {
    for history in [JoinHistory::Merge, JoinHistory::Rebase] {
        let fs = Fs::memory();
        let parent = unbounded!(fs, create_workspace("parent"))?;
        parent.write_text("/base", "base").await?;
        let base = unbounded!(parent, head())?;
        let child = fork_workspace(
            &parent,
            "child",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
        child.write_text("/child", "child").await?;
        parent.write_text("/parent", "parent").await?;
        let plan = unbounded!(child.join_into(&parent).history(history), plan())?;
        let cancellation = CancellationToken::new();
        let expected_target = plan.target_head();
        let prepared = plan
            .prepare(
                ApplyOptions {
                    if_target: expected_target,
                    idempotency_key: IdempotencyKey::new(),
                },
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let candidate = prepared.candidate().ok_or("missing candidate")?;
        assert!(prepared.conflicts().is_none());
        assert_eq!(unbounded!(parent, head())?.id(), expected_target);
        assert!(unbounded!(parent, read("/child", 16)).is_err());
        let budget = WorkBudget {
            object_bytes_written: 0,
            page_writes: 0,
            ..WorkBudget::UNBOUNDED
        };
        let published = prepared.publish(budget, &cancellation).await?;
        published.work.verify(budget)?;
        let JoinOutcome::Applied(application) = published.value else {
            return Err("not published".into());
        };
        assert_eq!(application.generation.id(), candidate);
        assert_eq!(unbounded!(parent, read("/child", 16))?.as_ref(), b"child");
        assert_eq!(unbounded!(parent, read("/parent", 16))?.as_ref(), b"parent");
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn prepared_join_retains_original_custody_and_rechecks_the_live_publication_seal()
-> Result<(), Box<dyn Error>> {
    for boundary in 0..4 {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let foreign =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
        {
            let fs = Fs::memory();
            let parent = unbounded!(fs, create_workspace("parent"))?;
            let child = fork_workspace(
                &parent,
                "child",
                ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
            )
            .await?;
            child.write_text("/child", "child").await?;
            let plan = unbounded!(child.join_into(&parent), plan())?;
            let expected_target = plan.target_head();
            let prepared = plan
                .prepare(
                    ApplyOptions {
                        if_target: expected_target,
                        idempotency_key: IdempotencyKey::new(),
                    },
                    crate::PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                )
                .await?;
            let mut allow = |_: &crate::NativeMaterializationWork| {
                std::future::ready(Ok::<(), WorkspaceError>(()))
            };
            match boundary {
                0 => assert!(
                    prepared
                        .publish_with_native_work(&foreign, &cancellation, &mut allow)
                        .await
                        .is_err()
                ),
                1 => assert!(
                    prepared
                        .publish(WorkBudget::UNBOUNDED, &cancellation)
                        .await
                        .is_err()
                ),
                2 => {
                    let mut deny = |_: &crate::NativeMaterializationWork| {
                        std::future::ready(Err::<(), WorkspaceError>(
                            WorkspaceError::IncompatibleWorkspace,
                        ))
                    };
                    assert!(
                        prepared
                            .publish_with_native_work(&original, &cancellation, &mut deny)
                            .await
                            .is_err()
                    );
                }
                _ => {
                    let (outcome, allocation) = prepared
                        .publish_with_native_work(&original, &cancellation, &mut allow)
                        .await?;
                    assert!(allocation.work().shares_ledger(&original));
                    assert!(matches!(outcome, JoinOutcome::Applied(_)));
                    drop((outcome, allocation));
                }
            }
            if boundary != 3 {
                assert_eq!(unbounded!(parent, head())?.id(), expected_target);
            }
        }
        // The in-memory store owns the newly encoded objects until it is
        // dropped, alongside the original ledger that paid for them.
        assert_eq!(
            original.live_allocation_bytes(),
            baseline,
            "boundary {boundary}"
        );
        assert_eq!(
            foreign.live_allocation_bytes(),
            baseline,
            "boundary {boundary}"
        );
    }
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn prepared_join_keeps_candidate_live_without_blocking_collection()
-> Result<(), Box<dyn Error>> {
    for history in [JoinHistory::Merge, JoinHistory::Rebase] {
        let directory = tempfile::tempdir()?;
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let parent = unbounded!(fs, create_workspace("parent"))?;
        parent.write_text("/base", "base").await?;
        let child = fork_workspace(
            &parent,
            "child",
            ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
        )
        .await?;
        child.write_text("/child", "child").await?;
        parent.write_text("/parent", "parent").await?;
        let plan = unbounded!(child.join_into(&parent).history(history), plan())?;
        let cancellation = CancellationToken::new();
        let expected_target = plan.target_head();
        let prepared = plan
            .prepare(
                ApplyOptions {
                    if_target: expected_target,
                    idempotency_key: IdempotencyKey::new(),
                },
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let candidate = prepared.candidate().ok_or("missing candidate")?;
        // Poll the real collection entry once: a preparation must retain a root,
        // never the admission read gate. This detects a held gate without a
        // timing bound or letting a broken preparation hang the test.
        {
            let collection = fs.objects().collection().ok_or("missing collector")?;
            let mut begin = Box::pin(collection.begin());
            assert!(futures::poll!(begin.as_mut()).is_ready());
        }
        fs.collect_local_garbage(None, &cancellation).await?;
        let retained = unbounded!(parent, generation(candidate))?;
        assert_eq!(unbounded!(retained, read("/child", 16))?.as_ref(), b"child");
        drop(retained);
        drop(prepared);
        assert!(fs.collect_local_garbage(None, &cancellation).await?.removed > 0);
        assert!(unbounded!(parent, generation(candidate)).is_err());
    }
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn prepared_join_flushes_normalized_history_before_candidate_publication()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let options = crate::LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    // Wait only after this scope has retired every workspace and join-plan
    // owner. A reusable JoinPlan now keeps its filesystem alive as well.
    let (parent_generation, released) = {
        let fs = Fs::local(options.clone()).await?;
        let parent = unbounded!(fs, create_workspace("parent"))?;
        parent.write_text("/base", "base").await?;
        let child = fork_workspace(
            &parent,
            "child",
            ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
        )
        .await?;
        child.write_text("/child", "child").await?;
        parent.write_text("/parent", "parent").await?;
        let plan = unbounded!(child.join_into(&parent), plan())?;
        let expected_target = plan.target_head();
        let prepared = plan
            .prepare(
                ApplyOptions {
                    if_target: expected_target,
                    idempotency_key: IdempotencyKey::new(),
                },
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let JoinOutcome::Applied(application) = prepared
            .publish(WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
        else {
            return Err("missing publication".into());
        };
        let parents = unbounded!(application.generation, parents())?;
        assert_eq!(parents.len(), 2);
        let parent_generation = parents[1];
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing release barrier")?;
        (parent_generation, released)
    };
    released.wait_async().await;
    let fs = Fs::local(options).await?;
    let parent = unbounded!(fs, open_workspace("parent"))?;
    let normalized_source = unbounded!(parent, generation(parent_generation))?;
    assert_eq!(
        unbounded!(normalized_source, read("/child", 16))?.as_ref(),
        b"child"
    );
    fs.collect_local_garbage(None, &cancellation).await?;
    Ok(())
}

async fn conflicting_pair<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    fs: &Fs<A, O>,
) -> Result<(Workspace<A, O>, Workspace<A, O>), Box<dyn Error>> {
    let parent = unbounded!(fs, create_workspace("parent"))?;
    parent.write_text("/conflict", "base").await?;
    let child = fork_workspace(
        &parent,
        "child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
    )
    .await?;
    parent.write_text("/conflict", "ours").await?;
    child.write_text("/conflict", "theirs").await?;
    child.write_text("/accepted", "accepted").await?;
    Ok((parent, child))
}

#[tokio::test]
async fn prepared_join_resolves_retained_conflicts_and_publishes_only_its_candidate()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let (parent, child) = conflicting_pair(&fs).await?;
    let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
    let (conflicts, truncated) = prepared.conflicts().ok_or("missing frontier")?;
    assert!(!truncated);
    assert!(!conflicts.is_empty());
    let resolutions = conflicts
        .iter()
        .cloned()
        .map(|key| {
            (
                key,
                crate::kernel::MergeConflictResolution::Select(
                    crate::kernel::MergeConflictSide::Theirs,
                ),
            )
        })
        .collect();
    // An incomplete frontier performs no object I/O and remains available.
    let cancellation = CancellationToken::new();
    let pending = prepared
        .resolve(
            &BTreeMap::new(),
            WorkBudget {
                backend_read_operations: 0,
                backend_write_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await?
        .value;
    assert!(pending.candidate().is_none());
    assert!(pending.conflicts().is_some());
    let resolved = unbounded!(pending, resolve(&resolutions))?;
    assert!(resolved.conflicts().is_none());
    let candidate = resolved.candidate().ok_or("missing resolved candidate")?;
    assert_eq!(unbounded!(parent, read("/conflict", 32))?.as_ref(), b"ours");
    let published = resolved
        .publish(
            WorkBudget {
                object_bytes_written: 0,
                page_writes: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await?
        .value;
    let JoinOutcome::Applied(application) = published else {
        return Err("missing publication".into());
    };
    assert_eq!(application.generation.id(), candidate);
    assert_eq!(
        unbounded!(parent, read("/conflict", 32))?.as_ref(),
        b"theirs"
    );
    assert_eq!(
        unbounded!(parent, read("/accepted", 32))?.as_ref(),
        b"accepted"
    );
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn prepared_join_conflict_frontier_survives_collection_without_holding_its_gate()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let (parent, child) = conflicting_pair(&fs).await?;
    let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
    let resolutions = prepared
        .conflicts()
        .ok_or("missing frontier")?
        .0
        .iter()
        .cloned()
        .map(|key| {
            (
                key,
                crate::kernel::MergeConflictResolution::Select(
                    crate::kernel::MergeConflictSide::Theirs,
                ),
            )
        })
        .collect();
    {
        let collection = fs.objects().collection().ok_or("missing collector")?;
        let mut begin = Box::pin(collection.begin());
        assert!(futures::poll!(begin.as_mut()).is_ready());
    }
    fs.collect_local_garbage(None, &CancellationToken::new())
        .await?;
    let resolved = unbounded!(prepared, resolve(&resolutions))?;
    assert!(resolved.candidate().is_some());
    let JoinOutcome::Applied(_) = unbounded!(resolved, publish())? else {
        return Err("missing publication".into());
    };
    assert_eq!(
        unbounded!(parent, read("/conflict", 32))?.as_ref(),
        b"theirs"
    );
    assert_eq!(
        unbounded!(parent, read("/accepted", 32))?.as_ref(),
        b"accepted"
    );
    Ok(())
}

#[test]
fn preparation_enum_keeps_large_variants_behind_exact_owned_boxes() {
    let ceiling = 128;
    assert!(std::mem::size_of::<crate::facade::WorkspaceJoinPreparation>() <= ceiling);
    // An enum containing this payload inline necessarily exceeds the ceiling.
    assert!(std::mem::size_of::<crate::facade::PreparedWorkspaceJoin>() > ceiling);
    assert!(
        std::mem::size_of::<
            crate::facade::RetentionUpdate<crate::MemoryAuthorityStore, crate::MemoryObjectStore>,
        >() <= ceiling
    );
}

#[tokio::test]
async fn retained_join_standalone_admits_existing_buffers_before_any_new_work()
-> Result<(), Box<dyn Error>> {
    for pending in [false, true] {
        let fs = Fs::memory();
        let (parent, child) = conflicting_pair(&fs).await?;
        let prepared = if pending {
            prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?
        } else {
            let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
            let resolutions = prepared
                .conflicts()
                .ok_or("missing frontier")?
                .0
                .iter()
                .cloned()
                .map(|key| {
                    (
                        key,
                        crate::kernel::MergeConflictResolution::Select(
                            crate::kernel::MergeConflictSide::Theirs,
                        ),
                    )
                })
                .collect();
            unbounded!(prepared, resolve(&resolutions))?
        };
        let result = prepared
            .resolve(
                &BTreeMap::new(),
                WorkBudget {
                    peak_allocation_bytes: 0,
                    backend_read_operations: 0,
                    backend_write_operations: 0,
                    ..WorkBudget::UNBOUNDED
                },
                &CancellationToken::new(),
            )
            .await;
        let Err(failure) = result else {
            return Err("retained backing escaped its new invocation bound".into());
        };
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert!(matches!(
            failure.error,
            WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                ..
            })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn retained_join_selecting_only_ours_preserves_the_exact_target_head()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("parent"))?;
    parent.write_text("/conflict", "base").await?;
    let child = fork_workspace(
        &parent,
        "child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
    )
    .await?;
    parent.write_text("/conflict", "ours").await?;
    child.write_text("/conflict", "theirs").await?;
    let head = unbounded!(parent, head())?.id();
    let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
    let resolutions = prepared
        .conflicts()
        .ok_or("missing frontier")?
        .0
        .iter()
        .cloned()
        .map(|key| {
            (
                key,
                crate::kernel::MergeConflictResolution::Select(
                    crate::kernel::MergeConflictSide::Ours,
                ),
            )
        })
        .collect();
    let resolved = unbounded!(prepared, resolve(&resolutions))?;
    assert!(resolved.candidate().is_none());
    assert!(resolved.conflicts().is_none());
    let checkpoint = unbounded!(resolved, checkpoint())?;
    assert_eq!(
        checkpoint.state,
        crate::facade::PreparedJoinCheckpointState::Unchanged
    );
    let mut forged = checkpoint;
    forged.state = crate::facade::PreparedJoinCheckpointState::Candidate;
    assert!(unbounded!(parent, resume_prepared_join(&forged)).is_err());
    drop(resolved);
    let resumed = unbounded!(parent, resume_prepared_join(&checkpoint))?;
    let JoinOutcome::NoChanges(generation) = unbounded!(resumed, publish())? else {
        return Err("ours created a new candidate".into());
    };
    assert_eq!(generation.id(), head);
    assert_eq!(unbounded!(parent, head())?.id(), head);
    unbounded!(parent, release_prepared_join(&checkpoint))?;
    Ok(())
}

#[tokio::test]
async fn unchanged_join_rechecks_the_exact_head_after_prepare_and_resume()
-> Result<(), Box<dyn Error>> {
    for resume in [false, true] {
        let fs = Fs::memory();
        let parent = unbounded!(fs, create_workspace("parent"))?;
        parent.write_text("/conflict", "base").await?;
        let child = fork_workspace(
            &parent,
            "child",
            ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
        )
        .await?;
        parent.write_text("/conflict", "ours").await?;
        child.write_text("/conflict", "theirs").await?;
        let prepared = prepare(&parent, &child, crate::PublicationPermit::Unrestricted).await?;
        let choices = prepared
            .conflicts()
            .ok_or("missing conflict")?
            .0
            .iter()
            .cloned()
            .map(|key| {
                (
                    key,
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Ours,
                    ),
                )
            })
            .collect();
        let prepared = unbounded!(prepared, resolve(&choices))?;
        let checkpoint = unbounded!(prepared, checkpoint())?;
        assert_eq!(
            checkpoint.state,
            crate::facade::PreparedJoinCheckpointState::Unchanged
        );
        let prepared = if resume {
            drop(prepared);
            unbounded!(parent, resume_prepared_join(&checkpoint))?
        } else {
            prepared
        };
        parent.write_text("/later", "later").await?;
        let current = unbounded!(parent, head())?.id();
        let JoinOutcome::StaleTarget(observed) = unbounded!(prepared, publish())? else {
            return Err("unchanged preparation hid a later target publication".into());
        };
        assert_eq!(observed.id(), current);
        assert_eq!(unbounded!(parent, head())?.id(), current);
        assert_eq!(unbounded!(parent, read("/later", 32))?.as_ref(), b"later");
        unbounded!(parent, release_prepared_join(&checkpoint))?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_pending_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    parent: &Workspace<A, O>,
    child: &Workspace<A, O>,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    let plan = unbounded!(child.join_into(parent), plan())?;
    let expected = plan.target_head();
    let prepared = plan
        .prepare(
            ApplyOptions {
                if_target: expected,
                idempotency_key: IdempotencyKey::new(),
            },
            crate::PublicationPermit::Unrestricted,
            original,
            cancellation,
        )
        .await?;
    let resolutions = prepared
        .conflicts()
        .ok_or(WorkspaceError::InvalidMergeResolution)?
        .0
        .iter()
        .cloned()
        .map(|key| {
            (
                key,
                crate::kernel::MergeConflictResolution::Select(
                    crate::kernel::MergeConflictSide::Theirs,
                ),
            )
        })
        .collect();
    let resolved = prepared
        .resolve(&resolutions, original, cancellation)
        .await?;
    assert!(resolved.conflicts().is_none());
    assert!(resolved.candidate().is_some());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn retained_join_prepare_and_resolve_keep_original_custody_at_every_allocation_cut()
-> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    {
        let fs = Fs::memory();
        let (parent, child) = conflicting_pair(&fs).await?;
        native_pending_path(&parent, &child, &original, &cancellation).await?;
    }
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    let required = original.receipt().allocation_operations;
    assert!(required > 1);
    for limit in 1..required {
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: limit,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        {
            let fs = Fs::memory();
            let (parent, child) = conflicting_pair(&fs).await?;
            assert!(
                native_pending_path(&parent, &child, &original, &cancellation)
                    .await
                    .is_err(),
                "cut {limit}"
            );
        }
        assert!(original.receipt().allocation_operations <= limit);
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes(),
            "cut {limit}"
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn retained_join_resolution_rejects_a_foreign_or_detached_work_owner()
-> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    for foreign_native in [false, true] {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let foreign = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        {
            let fs = Fs::memory();
            let (parent, child) = conflicting_pair(&fs).await?;
            let plan = unbounded!(child.join_into(&parent), plan())?;
            let expected = plan.target_head();
            let prepared = plan
                .prepare(
                    ApplyOptions {
                        if_target: expected,
                        idempotency_key: IdempotencyKey::new(),
                    },
                    crate::PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                )
                .await?;
            let resolutions = prepared
                .conflicts()
                .ok_or("missing frontier")?
                .0
                .iter()
                .cloned()
                .map(|key| {
                    (
                        key,
                        crate::kernel::MergeConflictResolution::Select(
                            crate::kernel::MergeConflictSide::Theirs,
                        ),
                    )
                })
                .collect();
            let before = foreign.receipt();
            if foreign_native {
                assert!(
                    prepared
                        .resolve(&resolutions, &foreign, &cancellation)
                        .await
                        .is_err()
                );
            } else {
                assert!(unbounded!(prepared, resolve(&resolutions)).is_err());
            }
            assert_eq!(foreign.receipt(), before);
        }
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(
            foreign.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_side_adapter_uses_one_frontier_and_the_original_publication_guard()
-> Result<(), Box<dyn Error>> {
    for history in [crate::JoinHistory::Merge, crate::JoinHistory::Rebase] {
        let mut canonical_reads = None;
        for adapter in [false, true] {
            let cancellation = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(error, _)| error)?;
            {
                let fs = Fs::memory();
                let (parent, child) = conflicting_pair(&fs).await?;
                let plan = unbounded!(child.join_into(&parent).history(history), plan())?;
                let options = ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                };
                // This observation is outside the measured invocation. Both
                // routes receive the same immutable frontier and side choices.
                let observed = plan
                    .prepare(
                        options,
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?
                    .value;
                let selections: BTreeMap<_, _> = observed
                    .conflicts()
                    .ok_or("missing conflicts")?
                    .0
                    .iter()
                    .cloned()
                    .map(|key| (key, ConflictSide::Theirs))
                    .collect();
                drop(observed);
                let guard_original = original.clone();
                let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let guard_calls = calls.clone();
                let mut guard = move |observed: &crate::NativeMaterializationWork| {
                    assert!(observed.shares_ledger(&guard_original));
                    guard_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    std::future::ready(Ok::<_, DrivenJoinError>(()))
                };
                let (outcome, allocation) = if adapter {
                    plan.apply_sides_with_permit_guarded_with_native_work(
                        options,
                        &selections,
                        crate::PublicationPermit::Unrestricted,
                        &original,
                        &cancellation,
                        &mut guard,
                    )
                    .await?
                } else {
                    let prepared = plan
                        .prepare(
                            options,
                            crate::PublicationPermit::Unrestricted,
                            &original,
                            &cancellation,
                        )
                        .await?;
                    let resolutions = selections
                        .keys()
                        .map(|key| {
                            (
                                key.clone(),
                                crate::kernel::MergeConflictResolution::Select(
                                    crate::kernel::MergeConflictSide::Theirs,
                                ),
                            )
                        })
                        .collect();
                    prepared
                        .resolve(&resolutions, &original, &cancellation)
                        .await?
                        .publish_with_native_work(&original, &cancellation, &mut guard)
                        .await?
                };
                assert!(matches!(outcome, JoinOutcome::Applied(_)));
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                let reads = original.receipt().backend_read_operations;
                if adapter {
                    assert_eq!(
                        Some(reads),
                        canonical_reads,
                        "side selection must not reread and replan its immutable inputs"
                    );
                } else {
                    canonical_reads = Some(reads);
                }
                assert_eq!(
                    unbounded!(parent, read("/conflict", 32))?.as_ref(),
                    b"theirs"
                );
                assert_eq!(
                    unbounded!(parent, read("/accepted", 32))?.as_ref(),
                    b"accepted"
                );
                drop((outcome, allocation));
            }
            assert_eq!(
                original.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn pending_join_checkpoint_retains_accepted_work_across_restart_and_collection()
-> Result<(), Box<dyn Error>> {
    for history in [JoinHistory::Merge, JoinHistory::Rebase] {
        let directory = tempfile::tempdir()?;
        let options = crate::LocalOptions::new(directory.path());
        let cancellation = CancellationToken::new();
        let (checkpoint, released) = {
            let fs = Fs::local(options.clone()).await?;
            let parent = unbounded!(fs, create_workspace("parent"))?;
            parent.write_text("/conflict", "base").await?;
            let child = fork_workspace(
                &parent,
                "child",
                ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
            )
            .await?;
            parent.write_text("/conflict", "ours").await?;
            parent.write_text("/ours", "ours").await?;
            child.write_text("/conflict", "theirs").await?;
            child.write_text("/accepted", "accepted").await?;
            let plan = unbounded!(child.join_into(&parent).history(history), plan())?;
            let prepared = unbounded!(
                plan,
                prepare(
                    ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: IdempotencyKey::new()
                    },
                    crate::PublicationPermit::Unrestricted
                )
            )?;
            assert!(prepared.conflicts().is_some());
            let checkpoint = unbounded!(prepared, checkpoint())?;
            assert!(checkpoint.state.program().is_some());
            assert_eq!(
                serde_json::from_slice::<crate::PreparedJoinCheckpoint>(&serde_json::to_vec(
                    &checkpoint
                )?)?,
                checkpoint
            );
            (
                checkpoint,
                fs.local_root_release_barrier()
                    .ok_or("missing release barrier")?,
            )
        };
        released.wait_async().await;
        let fs = Fs::local(options).await?;
        let parent = unbounded!(fs, open_workspace("parent"))?;
        fs.collect_local_garbage(None, &cancellation).await?;
        for field in 0..4 {
            let mut forged = checkpoint;
            match field {
                0 => {
                    forged.state = crate::facade::PreparedJoinCheckpointState::Pending(
                        crate::Digest::from_bytes([41; 32]),
                    );
                }
                1 => forged.join.maximum_changes += 1,
                2 => forged.state = crate::facade::PreparedJoinCheckpointState::Candidate,
                _ => forged.state = crate::facade::PreparedJoinCheckpointState::Unchanged,
            }
            assert!(unbounded!(parent, resume_prepared_join(&forged)).is_err());
        }
        let resumed = unbounded!(parent, resume_prepared_join(&checkpoint))?;
        let resolutions = resumed
            .conflicts()
            .ok_or("lost saved frontier")?
            .0
            .iter()
            .cloned()
            .map(|key| {
                (
                    key,
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Theirs,
                    ),
                )
            })
            .collect();
        let resolved = unbounded!(resumed, resolve(&resolutions))?;
        let JoinOutcome::Applied(_) = unbounded!(resolved, publish())? else {
            return Err("saved program did not publish".into());
        };
        assert_eq!(
            unbounded!(parent, read("/conflict", 32))?.as_ref(),
            b"theirs"
        );
        assert_eq!(unbounded!(parent, read("/ours", 32))?.as_ref(), b"ours");
        assert_eq!(
            unbounded!(parent, read("/accepted", 32))?.as_ref(),
            b"accepted"
        );
        unbounded!(parent, release_prepared_join(&checkpoint))?;
        fs.collect_local_garbage(None, &cancellation).await?;
        assert!(unbounded!(parent, resume_prepared_join(&checkpoint)).is_err());
    }
    Ok(())
}
