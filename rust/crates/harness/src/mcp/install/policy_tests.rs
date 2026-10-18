use super::*;
use crate::mcp::{McpSchemaExposure, config::Dialect};
use proptest::prelude::*;
use serde_json::json;
use std::cell::Cell;

thread_local! { static IGNORE_EXPOSURE: Cell<bool> = const { Cell::new(false) }; }

pub(super) fn ignore_exposure() -> bool {
    IGNORE_EXPOSURE.with(Cell::get)
}

fn installation(exposure: McpSchemaExposure) -> Result<(ToolRegistry, Capabilities)> {
    let server = super::tests::server(
        json!({"command":"fixture", "schema_exposure":exposure}),
        Dialect::Codex,
    )?;
    register(
        "fixture".into(),
        &server,
        Connected {
            transport: Arc::new(super::tests::Remote::default()),
            tools: super::tests::advertised(&["a", "b", "c", "d"]),
            binding: [5; 32],
        },
        McpClientLimits::default(),
        &|| Ok(()),
    )
}

fn check_visibility(installed: &(ToolRegistry, Capabilities), expected: &[String]) -> Result<()> {
    let names = installed
        .0
        .definitions(None)?
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    if names != expected {
        return Err(Error::Invalid("native MCP schema selection differs".into()));
    }
    let grants = installed.1.iter().collect::<Vec<_>>();
    if grants
        != [
            "mcp:call:fixture",
            "tool:call:mcp.fixture.a",
            "tool:call:mcp.fixture.b",
            "tool:call:mcp.fixture.c",
            "tool:call:mcp.fixture.d",
        ]
    {
        return Err(Error::Invalid(
            "schema visibility changed native MCP authority".into(),
        ));
    }
    Ok(())
}

proptest! {
    // Selection changes advertised schemas, never which installed revisions the
    // host grants authority to call. Exercise every subset of the same catalog.
    #[test]
    fn native_schema_selection_is_independent_of_tool_authority(mask in 0_u8..16, eager in any::<bool>()) {
        let selected = ["a", "b", "c", "d"].into_iter().enumerate()
            .filter(|(index, _)| eager || mask & (1 << index) != 0)
            .map(|(_, name)| name.to_owned()).collect::<Vec<_>>();
        let expected = selected.iter().map(|name| format!("mcp.fixture.{name}")).collect::<Vec<_>>();
        let exposure = if eager { McpSchemaExposure::Eager } else { McpSchemaExposure::Selected { names: selected } };
        let installed = installation(exposure).unwrap();
        prop_assert!(check_visibility(&installed, &expected).is_ok());
    }
}

#[test]
fn ignoring_the_configured_selection_breaks_the_same_oracle() -> Result<()> {
    let exposure = McpSchemaExposure::Selected { names: vec![] };
    check_visibility(&installation(exposure.clone())?, &[])?;
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            IGNORE_EXPOSURE.with(|fault| fault.set(false));
        }
    }
    let _reset = Reset;
    IGNORE_EXPOSURE.with(|fault| fault.set(true));
    assert!(
        check_visibility(&installation(exposure)?, &[]).is_err(),
        "old eager registration passed selected-schema expectations"
    );
    Ok(())
}
