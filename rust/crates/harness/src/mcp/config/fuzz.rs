use super::*;

/// Checks the actual native declaration codec without opening a connection.
pub fn check(bytes: &[u8]) {
    let Ok(declaration) = serde_json::from_slice::<ImportedServer>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&declaration).expect("serializable MCP declaration");
    let restored: ImportedServer =
        serde_json::from_slice(&encoded).expect("MCP declaration round trip");
    assert!(
        declaration == restored,
        "MCP declaration round trip changed fields"
    );
    for dialect in [Dialect::Claude, Dialect::Codex] {
        let expected = declaration.schema_exposure.clone();
        if let Ok(normalized) = declaration.clone().normalize(dialect) {
            assert_eq!(normalized.schema_exposure, expected);
        }
    }
}
