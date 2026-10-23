//! Measure the shipping shell path, including its native custody boundary.
use super::*;

#[tokio::test]
async fn repeated_shell_calls_reuse_the_agent_interpreter() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?;
    let task = Arc::new(context.task().clone());
    let root = Arc::new(
        CheckoutTools::new(
            Arc::clone(workspace),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            Arc::clone(&harness.task_files),
        )
        .await?,
    );
    let selecting = ToolContext::new(Arc::clone(&task), OperationId::new(), "shell-select", None)?;
    let tools = root.select(&selecting).await?.unwrap_or(root);
    #[cfg(unix)]
    let command = "printf '%s\\n' \"${BASHPID}\"";
    #[cfg(windows)]
    let command = "[Console]::WriteLine($PID)";
    let mut pids = Vec::new();
    let mut micros = Vec::new();
    for index in 0..4 {
        let operation_id = OperationId::new();
        let call_id = format!("shell-startup-{index}");
        let invocation = crate::tool::ToolInvocation {
            operation_id,
            call_id: call_id.clone(),
            name: crate::tools::shell::PLATFORM.name.into(),
            arguments: serde_json::json!({ "command": command }),
        };
        let context = ToolContext::new(Arc::clone(&task), operation_id, &call_id, None)?;
        let started = std::time::Instant::now();
        let result = tools.execute(invocation, Some(context)).await?;
        micros.push(started.elapsed().as_micros());
        assert_eq!(result.value["success"], true, "{result:?}");
        let pid: u32 = result.value["stdout"]
            .as_str()
            .ok_or_else(|| Error::Invalid("shell PID was not text".into()))?
            .trim()
            .parse()
            .map_err(storage)?;
        pids.push(pid);
    }
    eprintln!("SHELL_STARTUP calls=4 pids={pids:?} elapsed_us={micros:?}");
    harness.disconnect_unfinished().await?;
    assert!(
        pids.first()
            .is_some_and(|first| pids.iter().all(|pid| pid == first)),
        "repeated calls must retain the agent interpreter instead of starting another shell"
    );
    Ok(())
}
