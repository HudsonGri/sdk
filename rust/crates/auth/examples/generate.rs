//! Generate the shared auth protobuf and semantic TypeScript projections.
use acyclic_auth::contract::*;
use std::{fs, path::PathBuf};
use ts_rs::TS;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args_os().nth(1).ok_or("expected output root")?);
    render(&root.join("rust/crates/auth/proto"))?;
    let mut typescript =
        String::from("// Generated from acyclic-auth contract-derive declarations.\n");
    macro_rules! export_types {
        ($($ty:ty),* $(,)?) => {$(
            typescript.push_str("export ");
            typescript.push_str(&<$ty>::decl(&ts_rs::Config::default()));
            typescript.push('\n');
        )*};
    }
    export_types!(
        PrincipalKind,
        ResourcePattern,
        ResourceGrant,
        ExecutionGrant,
        BillingGrant,
        SocketPortRange,
        SocketHostRule,
        WebsocketConnectGrant,
        Grant,
        GrantSet,
        Attribute,
        Principal,
        KeyRange,
        AttributeMatch,
        Filter,
        Pin,
        Limits,
        Holder,
        Caveat,
        Block,
        SignedBlock,
        Capability,
        AdmittedCapability,
        GrantKind,
        PinValue,
        CaveatKind,
        TipProof,
        SubjectId,
        SubjectLineage,
        SubjectReference,
        SubjectReferenceKind,
        LineageAppend,
        Validity,
        Birth,
        BirthCertificate,
        CertifiedKey,
        KeyCertificate,
        CertificateChain,
        Revocation,
        KeyringSnapshot,
        SignedKeyringSnapshot,
        AuditRecord,
        RecordAttributes,
        AuditDecision,
        SecretsOperation,
        SecretKind,
        SecretsGrant,
        SecretPutRequest,
        SecretListRequest,
        SecretDeleteRequest,
        SecretMutationReply,
        PlainSecretVariable,
        ProtectedSecretVariable,
        SecretVariableKind,
        SecretVariable,
        SecretListReply
    );
    let path = root.join("typescript/packages/auth/src/contract.ts");
    fs::create_dir_all(path.parent().ok_or("missing output parent")?)?;
    let typescript = typescript
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(path, typescript)?;
    let vectors = root.join("typescript/packages/auth/generated/conformance");
    fs::create_dir_all(&vectors)?;
    fs::write(
        vectors.join("secrets-v1.json"),
        acyclic_auth::secrets::conformance::SUITE,
    )?;
    Ok(())
}
