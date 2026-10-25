//! CLI input only. The shared auth client owns the contract and all HTTP logic.
use super::{Error, Value, private_file};
use acyclic_auth::{contract::*, credential::CredentialProvider, secrets::{Client, HttpTransport}};
use std::{collections::HashSet, io::Read as _, path::PathBuf, sync::Arc};
use zeroize::{Zeroize as _, Zeroizing};

struct Options {
    operation: SecretsOperation,
    endpoint: String,
    bearer_file: PathBuf,
    scope: String,
    name: Option<String>,
    kind: Option<SecretKind>,
    generation: Option<u64>,
    value_file: Option<PathBuf>,
}

fn usage() -> Error {
    Error::new("usage_error", "secrets: use put|list|delete --scope S --bearer-file FILE; mutations require --name N --expected-generation G; put requires --kind plain|protected and reads stdin or --value-file FILE")
}

impl Options {
    fn parse(arguments: &[String]) -> Result<Self, Error> {
        let mut arguments = arguments.iter();
        let operation = match arguments.next().map(String::as_str) {
            Some("put") => SecretsOperation::Put,
            Some("list") => SecretsOperation::List,
            Some("delete") => SecretsOperation::Delete,
            _ => return Err(usage()),
        };
        let mut endpoint = std::env::var("ACYCLIC_API_URL").ok();
        let (mut bearer_file, mut scope, mut name, mut kind, mut generation, mut value_file) =
            (None, None, None, None, None, None);
        let mut seen = HashSet::new();
        while let Some(flag) = arguments.next() {
            if !seen.insert(flag) { return Err(usage()); }
            if flag == "--json" { continue; }
            if !matches!(flag.as_str(), "--endpoint" | "--bearer-file" | "--scope" | "--name" | "--kind" | "--expected-generation" | "--value-file") {
                return Err(usage());
            }
            let value = arguments.next().ok_or_else(usage)?;
            match flag.as_str() {
                "--endpoint" => endpoint = Some(value.clone()),
                "--bearer-file" => bearer_file = Some(PathBuf::from(value)),
                "--scope" => scope = Some(value.clone()),
                "--name" if operation != SecretsOperation::List => name = Some(value.clone()),
                "--kind" if operation == SecretsOperation::Put => kind = Some(match value.as_str() {
                    "plain" => SecretKind::Plain,
                    "protected" => SecretKind::Protected,
                    _ => return Err(usage()),
                }),
                "--expected-generation" if operation != SecretsOperation::List => generation = Some(value.parse().map_err(|_| usage())?),
                "--value-file" if operation == SecretsOperation::Put => value_file = Some(PathBuf::from(value)),
                _ => return Err(usage()),
            }
        }
        let scope = scope.filter(|scope| !scope.is_empty()).ok_or_else(usage)?;
        if operation != SecretsOperation::List && (name.as_ref().is_none_or(String::is_empty) || generation.is_none())
            || operation == SecretsOperation::Put && kind.is_none() { return Err(usage()); }
        Ok(Self { operation, endpoint: endpoint.ok_or_else(usage)?, bearer_file: bearer_file.ok_or_else(usage)?,
            scope, name, kind, generation, value_file })
    }
}

fn input_error() -> Error {
    Error::new("secrets_input_error", "secrets: cannot read UTF-8 input; provide --bearer-file with an owner-only bearer and put values on stdin or in --value-file")
}

pub(super) async fn execute(arguments: &[String]) -> Result<Value, Error> {
    let options = Options::parse(arguments)?;
    let transport = HttpTransport::new(&options.endpoint).map_err(client_error)?;
    let bytes = private_file::read(&options.bearer_file, acyclic_auth::credential::MAX_CREDENTIAL_BYTES as u64)
        .map_err(|_| input_error())?;
    let token = std::str::from_utf8(&bytes.0).map_err(|_| input_error())?.trim_end_matches(['\r', '\n']);
    if !acyclic_auth::credential::valid_credential(token) { return Err(input_error()); }
    let token = Arc::new(Zeroizing::new(token.to_owned()));
    let client = Client::new(transport, CredentialProvider::new(move || {
        let token = token.clone();
        async move { Ok(token.to_string()) }
    }));
    match options.operation {
        SecretsOperation::List => {
            let result = client.list(&SecretListRequest { scope: options.scope }).await.map_err(client_error)?;
            serde_json::to_value(result).map_err(|_| client_error(acyclic_auth::secrets::Error::InvalidResponse))
        }
        SecretsOperation::Put => {
            let mut value = String::new();
            let read = match options.value_file {
                Some(file) => std::fs::File::open(file).and_then(|mut file| file.read_to_string(&mut value)),
                None => std::io::stdin().read_to_string(&mut value),
            };
            if read.is_err() { value.zeroize(); return Err(input_error()); }
            let request = Zeroizing::new(SecretPutRequest { scope: options.scope, name: options.name.ok_or_else(usage)?,
                kind: options.kind.ok_or_else(usage)?, value, expected_generation: options.generation.ok_or_else(usage)? });
            let result = client.put(&request).await;
            serde_json::to_value(result.map_err(client_error)?).map_err(|_| client_error(acyclic_auth::secrets::Error::InvalidResponse))
        }
        SecretsOperation::Delete => {
            let result = client.delete(&SecretDeleteRequest { scope: options.scope, name: options.name.ok_or_else(usage)?,
                expected_generation: options.generation.ok_or_else(usage)? }).await.map_err(client_error)?;
            serde_json::to_value(result).map_err(|_| client_error(acyclic_auth::secrets::Error::InvalidResponse))
        }
        SecretsOperation::Unspecified => Err(usage()),
    }
}

fn client_error(error: acyclic_auth::secrets::Error) -> Error {
    Error::new("secrets_error", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_cli_rejects_argv_values_account_selection_and_unknown_operations_without_echoing() -> Result<(), Box<dyn std::error::Error>> {
        let planted = "PLANTED_SECRET_DO_NOT_LOG";
        for args in [
            vec!["put", "--value", planted], vec!["put", "--account", planted],
            vec!["get", planted], vec!["reveal", planted], vec!["export", planted],
            vec!["put", "--kind", planted], vec!["put", "--expected-generation", planted],
        ] {
            let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            let Err(error) = Options::parse(&args) else { return Err("forbidden CLI shape admitted".into()); };
            let diagnostic = format!("{error:?} {error} {}", error.json());
            assert!(!diagnostic.contains(planted), "CLI diagnostic exposed the planted value");
            assert!(format!("unexpected argument: {planted}").contains(planted), "negative control must detect the former echo path");
        }
        Ok(())
    }
}
