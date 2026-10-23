//! Inspection never opens or recovers a publication window.
use super::*;

pub(super) async fn run(
    tools: &CheckoutTools,
    workspace: &LocalWorkspace,
    view: crate::tools::git::View<'_>,
    source: Option<&workspace::MountedVolume>,
    reserved: &[String],
    arguments: &[String],
    context: &ToolContext,
) -> Result<ToolResult> {
    let owner = context
        .task()
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
    let selected = crate::tools::git::Workspace {
        filesystem: &workspace.filesystem,
        store: &workspace.store,
        view,
        root: source.map_or(workspace.path.as_path(), |source| {
            source.destination.as_path()
        }),
        repository: if source.is_some() {
            view.workspace().id()
        } else {
            workspace.view().workspace().id()
        },
        lease: None,
        checkout: None,
        reserved,
        author: "LocalHarness",
        authored_at_seconds: i64::try_from(tools.clock.now_unix_millis() / 1000)
            .unwrap_or(i64::MAX),
    };
    let verify = || async { owner.verify_owner(None)?.await };
    let result = if source.is_some() {
        crate::tools::git::run_with_guard(selected, arguments, verify).await
    } else {
        crate::tools::git::inspect_with_guard(
            selected,
            arguments,
            acyclic_fs::NativeGitView {
                options: &tools.capture_options,
                policy: &tools.capture_policy,
                materialized: workspace.mounted.is_some(),
            },
            verify,
        )
        .await
    };
    owner.verify_owner(None)?.await?;
    match result {
        Ok(output) => {
            let command = output
                .command
                .ok_or_else(|| Error::Conflict("read-only git produced no result".into()))?;
            Ok(ToolResult {
                value: serde_json::json!({"result": command}),
            })
        }
        Err(
            error @ (Error::Conflict(_)
            | Error::Invalid(_)
            | Error::NotFound(_)
            | Error::Unsupported(_)),
        ) => Ok(crate::tools::rejected_input(error)),
        Err(error) => Err(error),
    }
}
