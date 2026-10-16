#[tokio::test]
async fn restrictive_policy_blocks_wire_reads_and_both_credential_issuers()
-> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
        .with_credential_issuer(Arc::new(TestIssuer));
    let created = service.create_workspace(Request::new(wire::CreateWorkspaceRequest {
        name: "restricted".into(), profile: wire::FilesystemProfile::Portable as i32,
        operation: operation(1),
    })).await?.into_inner().workspace.ok_or("missing workspace")?;
    let reference = created.workspace.ok_or("missing reference")?;
    let owner = service.filesystem.open_workspace("restricted", WorkBudget::UNBOUNDED,
        &CancellationToken::new()).await?.value;
    crate::access::fixtures::publish_policy(&owner,
        &crate::access::fixtures::denied(crate::access::Action::Read, "secrets/**")).await?;
    assert_eq!(service.workspace(Some(reference.clone())).await.err().unwrap().code(),
        tonic::Code::PermissionDenied);
    for kind in [CredentialKind::Mount, CredentialKind::S3] {
        let error = service.issue_credential(Request::new(wire::CredentialRequest {
            workspace: Some(reference.clone()), generation: None, writable: true,
            expires_after_seconds: 60, operation: operation(2),
        }), kind).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    Ok(())
}
