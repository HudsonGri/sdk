//! The citation reader uses an actual project provider and the issued task scope.
use super::*;
use crate::{
    AgentId,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    file_citations::FileCitations,
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost, PreparationIo,
    },
    resources::ProviderRef,
};
use acyclic_markdown::directive::{Resolution, ResolutionError};

async fn resolved(source: &str, grants: bool, body: &[u8]) -> Result<FileCitations> {
    resolved_with_reader(source, grants, true, body).await
}

async fn resolved_with_reader(
    source: &str,
    grants: bool,
    reader_grants: bool,
    body: &[u8],
) -> Result<FileCitations> {
    let provider = ProviderRef::new(
        "citations".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let filesystem = acyclic_fs::Fs::memory();
    let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("citations".into()),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "citations",
        [71; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "citation-task".into(),
        },
    );
    let capabilities = Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let signed = issuer.issue(
        "owner",
        &capabilities,
        Some(AgentId::from_bytes([72; 16])),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let write = ContentGrant::verify(
        &verifier,
        &signed,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    host.put_content(
        &volume,
        &write,
        "file.rs",
        body,
        "text/plain",
        "file.rs",
        4_194_304,
        &crate::IdempotencyKey::new("fixture")?,
        None,
        None,
    )
    .await?;
    let token = acyclic_fs::CancellationToken::new();
    let workspace = filesystem
        .open_workspace(
            &volume.storage_name(&mut None)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &token,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .value;
    let mut transaction = workspace
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &token,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .value;
    transaction
        .create_symbolic_link(
            "/link",
            acyclic_stream::OwnedBytes::from_static(b"file.rs"),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &token,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    transaction
        .create_symbolic_link(
            "/outside",
            acyclic_stream::OwnedBytes::from_static(b"../../secret"),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &token,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &token,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let reader_scope = issuer.issue(
        "reader",
        &if reader_grants {
            capabilities.clone()
        } else {
            Capabilities::default()
        },
        Some(AgentId::from_bytes([72; 16])),
        None,
        &mut None,
    )?;
    let reader =
        FilesystemContentVerifier::new(host.clone(), &verifier, &reader_scope, 1_048_576, None)?;
    let writer = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &verifier,
        &signed,
        1_048_576,
        None,
    )?;
    let context = test_context(
        Bindings {
            scope: RuntimeScope::new(
                if grants {
                    Capabilities::new([volume.directory_read_capability("", &mut None)?])
                } else {
                    Capabilities::default()
                },
                Limits::default(),
            )?,
            content: Some(ContentBindings {
                reader,
                writer: Some(writer),
            }),
            ..Bindings::local()
        }
        .build(None)?,
    )?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_048_576, 8_388_608),
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let snapshot = crate::NativeFutureBundle::new(&work, || {
        crate::filesystem::file_citations::resolve(&context, source, &io)
    })?
    .await?;
    if body.len() > 4096
        && snapshot
            .entries
            .iter()
            .any(|entry| entry.read == Some(0..4096))
    {
        let cost = work.receipt();
        assert!(
            (cost.authority_bytes_read + cost.object_bytes_read) < body.len() as u64,
            "bounded prefix must not read the unrelated suffix: {cost:?}"
        );
        assert!(
            cost.bytes_hashed < body.len() as u64,
            "bounded prefix must not hash the unrelated suffix: {cost:?}"
        );
        assert_eq!(
            cost.backend_write_operations, 0,
            "reading must not publish project retention"
        );
        assert_eq!(
            cost.authority_records_appended, 0,
            "reading must not append project authority"
        );
    }
    let encoded = serde_json::to_vec(&snapshot).unwrap();
    drop(snapshot);
    let retained = io.owned_bytes()?;
    drop(io.into_native_custody(retained)?);
    FileCitations::decode(&encoded, encoded.len() as u64)
}

#[tokio::test]
async fn file_citations_project_reader_is_utf8_safe_and_range_proportional() -> Result<()> {
    let source = "::file[file.rs#L1]{chars=1:3}";
    let mut body = "αβγδ\n".as_bytes().to_vec();
    // The provider authenticates whole 1 MiB chunks. Span several chunks so
    // the backend-work assertions distinguish a prefix from a whole-file read.
    body.extend(std::iter::repeat_n(b'x', 3 * 1024 * 1024));
    let snapshot = resolved(source, true, &body).await?;
    let [entry] = snapshot.entries.as_slice() else {
        panic!("missing citation: {snapshot:?}")
    };
    assert!(snapshot.view.is_some());
    assert_eq!(entry.read, Some(0..4096));
    assert!(matches!(&entry.resolution, Resolution::Content(excerpt) if excerpt.content == "βγ"));
    assert_eq!(
        FileCitations::decode(&serde_json::to_vec(&snapshot).unwrap(), 1_048_576)?,
        snapshot
    );
    Ok(())
}

#[tokio::test]
async fn file_citations_denial_containment_and_range_errors_are_typed() -> Result<()> {
    for (source, grants, expected) in [
        ("::file[file.rs]", false, ResolutionError::Denied),
        ("::file[../file.rs]", true, ResolutionError::OutsideBoundary),
        ("::file[/file.rs]", true, ResolutionError::OutsideBoundary),
        ("::file[link]", true, ResolutionError::OutsideBoundary),
        ("::file[missing]", true, ResolutionError::Read),
        (
            "::file[outside/secret]",
            true,
            ResolutionError::OutsideBoundary,
        ),
        ("::file[outside]", true, ResolutionError::OutsideBoundary),
        ("::file[C:/file.rs]", true, ResolutionError::OutsideBoundary),
        (
            "::file[.system/private]",
            true,
            ResolutionError::OutsideBoundary,
        ),
        ("::file[file.rs#L9]", true, ResolutionError::LineRange),
        (
            "::file[file.rs]{chars=0:99}",
            true,
            ResolutionError::CharRange,
        ),
        (
            "::file[file.rs]{symbol=answer}",
            true,
            ResolutionError::UnsupportedSymbol,
        ),
    ] {
        let snapshot = resolved(source, grants, "αβ\n".as_bytes()).await?;
        let [entry] = snapshot.entries.as_slice() else {
            panic!("missing {source}: {snapshot:?}")
        };
        assert_eq!(entry.resolution, Resolution::Error(expected), "{source}");
        if matches!(
            expected,
            ResolutionError::Denied
                | ResolutionError::OutsideBoundary
                | ResolutionError::UnsupportedSymbol
        ) {
            assert_eq!(entry.read, None);
        }
    }
    let snapshot = resolved(&"::file[file.rs]\n".repeat(257), true, b"data").await?;
    assert_eq!(
        snapshot.error,
        Some(Resolution::Error(ResolutionError::TooMany))
    );
    assert!(snapshot.view.is_none());
    assert!(snapshot.entries.is_empty());
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "range reads must exclude unrelated file history")]
async fn file_citations_range_counter_detects_the_whole_file_read() {
    let mut body = "αβγδ\n".as_bytes().to_vec();
    body.extend(std::iter::repeat_n(b'x', 128 * 1024));
    crate::filesystem::file_citations::READ_WHOLE_FILE.set(true);
    let snapshot = resolved("::file[file.rs#L1]{chars=1:3}", true, &body).await;
    crate::filesystem::file_citations::READ_WHOLE_FILE.set(false);
    let snapshot = snapshot.unwrap();
    assert_eq!(
        snapshot.entries[0].read,
        Some(0..4096),
        "range reads must exclude unrelated file history"
    );
}

#[tokio::test]
async fn file_citations_provider_policy_can_deny_an_admitted_path() -> Result<()> {
    let snapshot =
        resolved_with_reader("::file[file.rs]", true, false, b"unread private body").await?;
    assert_eq!(
        snapshot.entries[0].resolution,
        Resolution::Error(ResolutionError::Denied)
    );
    assert!(snapshot.entries[0].read.is_none());
    assert!(snapshot.view.is_none());
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "bounded prefix must not read the unrelated suffix")]
async fn file_citations_backend_counter_detects_pinning_the_whole_project() {
    use futures::FutureExt as _;
    let mut body = "αβγδ\n".as_bytes().to_vec();
    body.extend(std::iter::repeat_n(b'x', 3 * 1024 * 1024));
    crate::filesystem::file_citations::PIN_PROJECT.set(true);
    let result =
        std::panic::AssertUnwindSafe(resolved("::file[file.rs#L1]{chars=1:3}", true, &body))
            .catch_unwind()
            .await;
    crate::filesystem::file_citations::PIN_PROJECT.set(false);
    match result {
        Ok(result) => {
            result.unwrap();
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
