//! Identity secrets contract. Protected values exist only in put requests.
use super::*;
use serde::{Deserialize, Serialize};

/// Exact operation, independent of every product action vocabulary.
#[acyclic_contract_derive::enumeration(error = Error, unknown = Error::UnknownEnum)]
#[derive(Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum SecretsOperation {
    /// Never admitted.
    Unspecified = 0,
    /// Create or replace with a generation fence.
    Put = 1,
    /// List metadata and plain values only.
    List = 2,
    /// Delete with a generation fence.
    Delete = 3,
}

/// Declared visibility, or a deletion receipt.
#[acyclic_contract_derive::enumeration(error = Error, unknown = Error::UnknownEnum)]
#[derive(Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum SecretKind {
    /// Never admitted.
    Unspecified = 0,
    /// List may return the value.
    Plain = 1,
    /// List returns metadata only.
    Protected = 2,
    /// Only returned by delete.
    Deleted = 3,
}

/// One secrets permission; an absent name covers the entire exact scope.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_grant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SecretsGrant {
    /// Exactly one operation.
    #[wire(tag = 1, enumeration)]
    pub operation: SecretsOperation,
    /// Exact scope, never a prefix or an account selector.
    #[wire(tag = 2, string)]
    pub scope: String,
    /// Null means every name in the scope.
    #[wire(tag = 3, string)]
    pub name: Option<String>,
}

/// Create or replace; values must never be included in diagnostics.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_put, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretPutRequest {
    /// Exact owner-local scope.
    #[wire(tag = 1, string)]
    pub scope: String,
    /// Exact variable name.
    #[wire(tag = 2, string)]
    pub name: String,
    /// Plain or protected; deleted is not a write kind.
    #[wire(tag = 3, enumeration)]
    pub kind: SecretKind,
    /// Write-only for protected rows.
    #[wire(tag = 4, string)]
    pub value: String,
    /// Generation observed before the mutation, including zero for an empty scope.
    #[wire(tag = 5, uint64)]
    pub expected_generation: u64,
}

/// List one scope. There is deliberately no account selector or reveal flag.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_list, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SecretListRequest {
    /// Exact owner-local scope.
    #[wire(tag = 1, string)]
    pub scope: String,
}

/// Generation-fenced deletion.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_delete, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretDeleteRequest {
    /// Exact owner-local scope.
    #[wire(tag = 1, string)]
    pub scope: String,
    /// Exact variable name.
    #[wire(tag = 2, string)]
    pub name: String,
    /// Generation observed before the mutation.
    #[wire(tag = 3, uint64)]
    pub expected_generation: u64,
}

/// Mutation metadata only. It has no value field, even for plain puts.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SecretMutationReply {
    /// Scope generation after this mutation.
    #[wire(tag = 1, uint64)]
    pub generation: u64,
    /// Mutated variable name.
    #[wire(tag = 2, string)]
    pub name: String,
    /// Variable version after this mutation.
    #[wire(tag = 3, uint64)]
    pub version: u64,
    /// Declared put kind, or deleted.
    #[wire(tag = 4, enumeration)]
    pub kind: SecretKind,
}

/// One declared plain row.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct PlainSecretVariable {
    /// Variable name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Variable version.
    #[wire(tag = 2, uint64)]
    pub version: u64,
    /// The only response type that can contain a value.
    #[wire(tag = 3, string)]
    pub value: String,
}

/// Protected metadata cannot represent a value.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct ProtectedSecretVariable {
    /// Variable name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Variable version.
    #[wire(tag = 2, uint64)]
    pub version: u64,
}

/// Discriminated visibility; protected and plain use different payload types.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SecretVariableKind {
    /// Explicit plain value.
    #[wire(tag = 1, message)]
    Plain(PlainSecretVariable),
    /// Protected metadata only.
    #[wire(tag = 2, message)]
    Protected(ProtectedSecretVariable),
}

/// List row. JSON is the tagged visibility payload, protobuf a required oneof.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, TS)]
#[ts(type = "SecretVariableKind")]
pub struct SecretVariable {
    /// Exactly one visibility payload.
    #[wire(oneof = "1, 2")]
    pub kind: SecretVariableKind,
}

/// One generation-pinned list; no get, reveal or export operation exists.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SecretListReply {
    /// Generation of the listed scope.
    #[wire(tag = 1, uint64)]
    pub generation: u64,
    /// Plain values and protected metadata, in server order.
    #[wire(tag = 2, message)]
    pub variables: Vec<SecretVariable>,
}

macro_rules! redacted_debug {
    ($($ty:ty),* $(,)?) => {$(
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(concat!(stringify!($ty), "([redacted])"))
            }
        }
    )*};
}
redacted_debug!(
    SecretPutRequest,
    SecretListRequest,
    SecretDeleteRequest,
    SecretMutationReply,
    PlainSecretVariable,
    ProtectedSecretVariable,
    SecretVariable,
    SecretListReply
);

impl Serialize for SecretVariable {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.kind.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for SecretVariable {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        SecretVariableKind::deserialize(deserializer).map(|kind| Self { kind })
    }
}
impl zeroize::Zeroize for SecretPutRequest {
    fn zeroize(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.value);
    }
}
