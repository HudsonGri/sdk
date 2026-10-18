//! A scripted model, durable citation file and ordinary client/TUI fold.
use super::*;
use crate::tui::{
    ClientRenderer, PromptInput, Viewport,
    ratatui::{Terminal, backend::TestBackend},
};
use crate::{
    client::{self, CommandSession, TaskFileClient},
    task_volume::{Component, FileKey, Query, TaskFile},
    wire,
};

#[tokio::test]
async fn file_citations_model_output_reopens_as_the_same_excerpt() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(super::super::storage)?;
    let workspace = fs
        .open_workspace(
            &project.storage_name(&mut None)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|error| super::super::storage(error.error))?
        .value;
    workspace
        .write_text("/file.rs", "first line\nlet cited_value = 42;\nlast line")
        .await
        .map_err(super::super::storage)?;
    let emitted = "See :file[file.rs]\n\n::file[file.rs#L2]\n\n";
    let provider = scripted([ScriptedTurn::Text(emitted.into())]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let session = uuid::Uuid::new_v4();
    let session_owner = harness.client_owner();
    let mut ui = client::local_ui(&harness, &session.to_string(), &session_owner).await?;
    ui.submit("cite the second line", harness.client_reader()?.as_ref())
        .await?;
    let mut commands = CommandSession::open_local(&harness, session, 1_048_576, 128).await?;
    let task_id = commands.active_task().expect("admitted prompt task");
    let request = commands.next_request().await?;
    let output = client::run_local_command(&mut commands, &harness, &request)
        .await?
        .expect("command outcome");
    assert_eq!(output["text"], emitted);
    let task = Component::new(task_id.to_string()).unwrap();
    let snapshot = harness
        .task_files()
        .snapshot(&Query::Task(task.clone()), 64)
        .await
        .map_err(super::super::storage)?;
    let citation = snapshot
        .files
        .iter()
        .find(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::Citations(0),
                    ..
                }
            )
        })
        .expect("canonical completion must publish the citation sidecar");
    let resolved = crate::file_citations::FileCitations::decode(&citation.bytes, 1_048_576)?;
    assert!(resolved.entries.iter().any(|entry| matches!(&entry.resolution, acyclic_markdown::directive::Resolution::Content(excerpt) if excerpt.content == "let cited_value = 42;")));
    workspace
        .write_text("/file.rs", "changed after the response")
        .await
        .map_err(super::super::storage)?;
    let authority = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task_id.to_string(),
    };
    let client =
        TaskFileClient::observe(&harness.task_files().inner, &authority, 128, 1_048_576).await?;
    assert_excerpt(&client, &authority)?;
    drop(client);
    // Reopening the command session exercises the same retained archive query
    // and task selection as GraphCoder, after the source file has changed.
    drop(commands);
    let commands = CommandSession::open_local(&harness, session, 1_048_576, 128).await?;
    let mut client = TaskFileClient::observe(
        harness.task_files().as_ref(),
        &session_owner,
        128,
        1_048_576,
    )
    .await?;
    client
        .follow_commands(harness.task_files().as_ref(), &commands, &session_owner)
        .await?;
    client.project_commands(&commands, &session_owner)?;
    assert_excerpt(&client, &session_owner)?;
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

fn assert_excerpt(client: &TaskFileClient, authority: &wire::Authority) -> Result<()> {
    let mut renderer = ClientRenderer::default();
    renderer.update(client.views(), authority);
    let mut terminal = Terminal::new(TestBackend::new(100, 24)).map_err(super::super::storage)?;
    terminal
        .draw(|frame| {
            renderer.screen().render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                0,
            );
        })
        .map_err(super::super::storage)?;
    let picture: String = terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect();
    assert!(picture.contains("let cited_value = 42;"), "{picture}");
    assert!(picture.contains("file.rs:2-2"), "{picture}");
    assert!(!picture.contains("::file["), "{picture}");
    assert!(!picture.contains("changed after the response"));
    Ok(())
}
