use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn authority(file: &FileRef) -> Result<(AuthorityIssuer, AuthorityVerifier, Scope)> {
    let issuer = AuthorityIssuer::new(
        "pinned",
        [2; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "pinned".into(),
        },
    );
    let scope = issuer.issue(
        "reader",
        &crate::Capabilities::new([file.volume().capability(VolumeOperation::Read, &mut None)?]),
        Some(AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    Ok((issuer, verifier, scope))
}

async fn read_all(
    reader: &Arc<
        FilesystemContentVerifier<
            acyclic_fs::MemoryAuthorityBackend,
            acyclic_fs::MemoryObjectBackend,
        >,
    >,
    file: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<()> {
    assert_eq!(
        reader.read(file, 4096, None, io)?.await?.as_ref(),
        b"payload"
    );
    assert_eq!(
        reader
            .read_range(
                file,
                acyclic_fs::ByteRange {
                    offset: 1,
                    length: 3
                },
                None,
                io
            )?
            .await?
            .as_ref(),
        b"ayl"
    );
    reader.verify(file, 4096, io)?.await?;
    Ok(())
}

#[test]
fn content_readers_recheck_shared_evidence_without_reauthenticating() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (host, file) = runtime.block_on(fixture(b"payload"))?;
    let (_issuer, verifier, scope) = authority(&file)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let reader = FilesystemContentVerifier::new(host, &verifier, &scope, 4096, Some(&io))?;
    let pinned = reader.reference_reader(&file, &io)?;
    let cloned = Arc::clone(&reader);
    let (result, signatures) = differential::observe(
        || (),
        |()| -> Result<()> {
            runtime.block_on(async {
                for reader in [&reader, &pinned, &cloned] {
                    read_all(reader, &file, None).await?;
                    read_all(reader, &file, Some(&io)).await?;
                    reader.require_native_work(&work)?;
                }
                Ok(())
            })
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "content reader reauthenticated immutable scope evidence"
    );
    let before = work.receipt().allocation_operations;
    cancellation.cancel();
    for reader in [&reader, &pinned, &cloned] {
        assert!(reader.require_native_work(&work).is_err());
        assert!(
            runtime
                .block_on(reader.read_file(&file, 4096, None, Some(&io)))
                .is_err()
        );
    }
    assert_eq!(work.receipt().allocation_operations, before);
    Ok(())
}

#[test]
fn revoked_content_readers_reject_pending_and_shared_entry_points() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (host, file) = runtime.block_on(fixture(b"payload"))?;
    let (issuer, verifier, scope) = authority(&file)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let reader = FilesystemContentVerifier::new(host, &verifier, &scope, 4096, Some(&io))?;
    let pinned = reader.reference_reader(&file, &io)?;
    let pending = reader.read(&file, 4096, None, None)?;
    issuer.revoke_workload(&scope)?;
    assert!(runtime.block_on(pending).is_err());
    for reader in [&reader, &pinned] {
        assert!(
            runtime
                .block_on(reader.read(&file, 4096, None, None)?)
                .is_err()
        );
        assert!(
            runtime
                .block_on(reader.read_range(
                    &file,
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: 1
                    },
                    None,
                    None
                )?)
                .is_err()
        );
        assert!(runtime.block_on(reader.verify(&file, 4096, None)?).is_err());
        assert!(reader.require_native_work(&work).is_err());
    }
    Ok(())
}
