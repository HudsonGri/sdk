use super::*;
use crate::{
    client::{CommandFileRoot, PromptFiles, run_local_command},
    conversation::verified_content_bytes,
    model::{FileProjectionPolicy, ModelContentPart},
    tui::{InputEffect, PromptInput},
};

#[tokio::test]
async fn host_image_files_reach_model_and_reopen_with_exact_pins() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let owner = harness.client_owner();
    let session = OperationId::new();
    let mut commands = crate::client::local_ui(&harness, &session.to_string(), &owner).await?;
    let reader = harness.client_reader()?;
    let publisher = harness.client_publisher()?;
    let prepared = crate::tui::prepare_image(2, 1, vec![255; 8]).unwrap();
    let crate::tui::ClipboardContent::Image { jpeg, .. } = prepared else {
        unreachable!()
    };
    let source = directory.path().join("selected 图像.jpg");
    std::fs::write(&source, &jpeg).map_err(super::super::storage)?;
    let mut input = PromptInput::default();
    crate::client::host_tui_input(
        &mut commands,
        reader.as_ref(),
        publisher.as_ref(),
        &mut input,
        InputEffect::Submit {
            text: format!("/attach {}", source.display()),
            edit: None,
            queued: false,
        },
        20,
        0,
        0,
    )
    .await?;
    let files = commands
        .state(CommandFileRoot::Session, "attachments.json")?
        .unwrap();
    let pins: Vec<crate::conversation::FileRef> =
        serde_json::from_value(files).map_err(super::super::storage)?;
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0].volume().class(), VolumeClass::AgentPrivate);
    assert_eq!(
        verified_content_bytes(reader.as_ref(), &pins[0], 4096, None)
            .await?
            .0
            .as_ref(),
        jpeg.as_slice()
    );
    let mut prompt_files = PromptFiles::default();
    prompt_files.refresh(&commands, &mut input)?;
    assert_eq!(input.images, 1);
    std::fs::write(&source, b"changed host file").map_err(super::super::storage)?;
    drop(commands);
    let mut commands = crate::client::local_ui(&harness, &session.to_string(), &owner).await?;
    let mut input = PromptInput::default();
    PromptFiles::default().refresh(&commands, &mut input)?;
    assert_eq!(
        input.images, 1,
        "a reopened UI folds the private draft file"
    );
    commands
        .submit("describe the attached image", reader.as_ref())
        .await?;
    let request = commands.next_request().await?;
    assert!(
        commands
            .state(CommandFileRoot::Session, "attachments.json")?
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    let resolution: crate::client::CommandResolution =
        serde_json::from_slice(&request.bytes).map_err(super::super::storage)?;
    let crate::client::CommandResolution::Prompt { mentions, .. } = resolution else {
        panic!("expected prompt")
    };
    assert_eq!(mentions[0].file, pins[0]);
    run_local_command(&mut commands, &harness, &request).await?;
    let requests = provider.requests();
    let images: Vec<_> = requests[0]
        .messages
        .iter()
        .flat_map(|m| m.content.parts())
        .filter_map(|p| match p {
            ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::Native(_),
            } => Some(file),
            _ => None,
        })
        .collect();
    assert_eq!(images, vec![&pins[0]]);
    drop(commands);
    drop(harness);
    let reopened = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let reader = reopened.client_reader()?;
    assert_eq!(
        verified_content_bytes(reader.as_ref(), &pins[0], 4096, None)
            .await?
            .0
            .as_ref(),
        jpeg.as_slice()
    );
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn host_image_rejections_preserve_the_followed_draft() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, text())).without_default_tools(),
    )
    .await?;
    let mut commands = crate::client::local_ui(
        &harness,
        &OperationId::new().to_string(),
        &harness.client_owner(),
    )
    .await?;
    let reader = harness.client_reader()?;
    let publisher = harness.client_publisher()?;
    let mut input = PromptInput::default();
    for (name, bytes) in [
        ("empty.png", Vec::new()),
        ("text.png", b"not an image".to_vec()),
        ("oversized.png", vec![0; 4 * 1024 * 1024 + 1]),
    ] {
        let path = directory.path().join(name);
        std::fs::write(&path, bytes).map_err(super::super::storage)?;
        crate::client::host_tui_input(
            &mut commands,
            reader.as_ref(),
            publisher.as_ref(),
            &mut input,
            InputEffect::Submit {
                text: format!("/attach {}", path.display()),
                edit: None,
                queued: false,
            },
            20,
            0,
            0,
        )
        .await?;
        assert!(
            commands
                .state(CommandFileRoot::Session, "attachments.json")?
                .is_none()
        );
        assert!(
            !commands
                .state(CommandFileRoot::Session, "notifications.json")?
                .unwrap()
                .as_array()
                .unwrap()
                .is_empty(),
            "{name}"
        );
        assert!(commands.take_request().is_none());
    }
    let crate::tui::ClipboardContent::Image { jpeg, .. } =
        crate::tui::prepare_image(1, 1, vec![255; 4]).unwrap()
    else {
        unreachable!()
    };
    let path = directory.path().join("valid.jpg");
    std::fs::write(&path, jpeg).map_err(super::super::storage)?;
    crate::client::host_tui_input(
        &mut commands,
        reader.as_ref(),
        publisher.as_ref(),
        &mut input,
        InputEffect::Submit {
            text: format!("/attach {}", path.display()),
            edit: None,
            queued: false,
        },
        20,
        0,
        0,
    )
    .await?;
    assert!(
        commands
            .state(CommandFileRoot::Session, "notifications.json")?
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty(),
        "successful host actions clear stale errors"
    );
    Ok(())
}
