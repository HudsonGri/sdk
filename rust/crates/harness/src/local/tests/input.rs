use super::*;
use crate::{
    client::CommandResolution,
    conversation::ContentResidencyVerifier,
    model::{ModelContent, ModelContentPart},
};

#[tokio::test]
async fn composer_commands_are_typed_and_unknown_commands_never_admit() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let operation = OperationId::new();
    assert!(matches!(
        harness
            .resolve_input(operation, "/shell echo hello")
            .await?,
        CommandResolution::Tool {
            tool: crate::client::CommandTool::Bash,
            ..
        }
    ));
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/missing-input-command")
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(provider.requests().is_empty());
    // The previous construction demonstrably admits the slash spelling as a
    // model prompt. The production resolver must choose a different effect.
    let legacy = crate::client::prepare_local_prompt(operation, "/shell echo hello")?;
    assert!(matches!(
        serde_json::from_slice::<CommandResolution>(&legacy.bytes)
            .map_err(super::super::storage)?,
        CommandResolution::Prompt { .. }
    ));
    harness.disconnect().await
}

#[tokio::test]
async fn composer_templates_and_mentions_retain_the_original_cut_through_dispatch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir_all(directory.path().join(".claude/commands"))
        .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join(".claude/commands/marker.md"),
        "Inspect $ARGUMENTS @calc.py",
    )
    .map_err(super::super::storage)?;
    std::fs::write(directory.path().join("calc.py"), "value = 42\n")
        .map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let operation = OperationId::new();
    let resolved = harness.resolve_input(operation, "/marker foo").await?;
    let CommandResolution::Prompt { text, mentions, .. } = &resolved else {
        panic!("not a prompt")
    };
    assert_eq!(text, "Inspect foo @calc.py");
    assert_eq!(mentions.len(), 1);
    let attached = mentions[0].file.clone();
    assert_eq!(
        harness
            .client_reader()?
            .read(&attached, 1024, None)?
            .await?
            .as_ref(),
        b"value = 42\n"
    );
    std::fs::write(directory.path().join("calc.py"), "value = 99\n")
        .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join(".claude/commands/marker.md"),
        "changed",
    )
    .map_err(super::super::storage)?;
    assert_eq!(
        harness.resolve_input(operation, "/marker foo").await?,
        resolved
    );
    assert!(matches!(
        harness.resolve_input(operation, "/marker changed").await,
        Err(Error::Conflict(_))
    ));
    harness.dispatch_command(operation, None, resolved).await?;
    let (_, model) = harness
        .recover_turn_inputs(operation)
        .await?
        .expect("model input");
    let bytes = harness
        .client_reader()?
        .read(&model.payload, harness.limits.file_bytes, None)?
        .await?;
    let staged: crate::filesystem::ModelTaskCommand =
        serde_json::from_slice(&bytes).map_err(super::super::storage)?;
    let ModelContent::Parts(parts) = staged.input else {
        panic!("mention was dropped")
    };
    assert!(matches!(&parts[0], ModelContentPart::Text { text } if text == "Inspect foo @calc.py"));
    assert!(matches!(&parts[1], ModelContentPart::File { file, .. } if file == &attached));
    assert_eq!(
        harness
            .resume(crate::runtime::task_id_for_admission(operation))
            .await?
            .text,
        "done"
    );
    assert_eq!(provider.requests().len(), 1);
    harness.disconnect().await
}

#[tokio::test]
async fn composer_duplicate_and_unsafe_sources_fail_before_model_work() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    for path in [".claude/commands", ".codex/prompts"] {
        std::fs::create_dir_all(directory.path().join(path)).map_err(super::super::storage)?;
        std::fs::write(directory.path().join(path).join("duplicate.md"), "body")
            .map_err(super::super::storage)?;
    }
    let provider = text();
    let harness = LocalHarness::open(checkout_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/duplicate")
            .await,
        Err(Error::Conflict(_))
    ));
    for input in [
        "inspect @../outside",
        "inspect @/.system/secret",
        "inspect @.system/secret",
        "inspect @\"unterminated",
    ] {
        assert!(
            matches!(
                harness.resolve_input(OperationId::new(), input).await,
                Err(Error::Invalid(_))
            ),
            "{input}"
        );
    }
    assert!(provider.requests().is_empty());
    harness.disconnect().await
}
