use super::*;
use proptest::prelude::*;
use serde_json::{Value, json};

proptest! {
    // Native import and its codec preserve the exact schema selection, including
    // Unicode, empty selections and invalid catalog names (validated at install).
    #[test]
    fn exposure_survives_native_declaration_round_trip(
        names in proptest::collection::vec(".{0,16}", 0..5), eager in any::<bool>(), http in any::<bool>(),
    ) {
        let exposure = if eager { McpSchemaExposure::Eager {} } else { McpSchemaExposure::Selected { names } };
        let mut value = if http { json!({"type":"http", "url":"https://example.test/mcp"}) }
            else { json!({"command":"fixture"}) };
        value["schema_exposure"] = serde_json::to_value(&exposure).unwrap();
        let bytes = serde_json::to_vec(&value).unwrap();
        super::fuzz::check(&bytes);
        for dialect in [Dialect::Claude, Dialect::Codex] {
            let parsed: ImportedServer = serde_json::from_slice(&bytes).unwrap();
            let server = parsed.normalize(dialect).unwrap();
            prop_assert_eq!(server.schema_exposure, exposure.clone());
        }
    }
}

#[test]
fn defaults_are_eager_and_malformed_exposure_is_rejected() {
    let parsed: ImportedServer = serde_json::from_value(json!({"command":"fixture"})).unwrap();
    assert_eq!(
        parsed.normalize(Dialect::Claude).unwrap().schema_exposure,
        McpSchemaExposure::Eager {}
    );
    for policy in [
        json!({"kind":"selected"}),
        json!({"kind":"selected","names":"read"}),
        json!({"kind":"eager","names":[]}),
        json!({"kind":"hidden"}),
        Value::Bool(false),
        Value::Null,
    ] {
        assert!(
            serde_json::from_value::<ImportedServer>(
                json!({"command":"fixture", "schema_exposure":policy.clone()})
            )
            .is_err(),
            "accepted malformed exposure: {policy}"
        );
    }
}

proptest! {
    #[test]
    fn eager_exposure_rejects_every_extra_field(key in "extension_[a-z]{1,16}", value in ".{0,32}") {
        let mut policy = json!({"kind":"eager"});
        policy[&key] = value.into();
        prop_assert!(serde_json::from_value::<McpSchemaExposure>(policy.clone()).is_err());

        // Exact old representation: Serde ignores fields beside an internally
        // tagged unit variant even when deny_unknown_fields is present.
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum OldExposure { Eager }
        prop_assert!(serde_json::from_value::<OldExposure>(policy).is_ok());
    }
}

#[test]
fn fuzz_corpus_exercises_the_native_declaration_parser() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../harness-fuzz/corpus/mcp_configuration");
    let mut count = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        super::fuzz::check(&std::fs::read(entry.unwrap().path()).unwrap());
        count += 1;
    }
    assert!(count >= 4);
}
