//! GraphCoder's runtime and shared invocation entrypoint.
use crate::{
    args::{Args, InputFormat},
    view,
};
use acyclic_fs::in_heap;
use acyclic_harness::{
    OperationId, client,
    local::{LocalHarness, LocalHarnessConfig},
};
use acyclic_harness_providers::{Credentials, Registry};
use std::sync::Arc;

/// Polls one invocation to completion on the calling thread.
///
/// `main` calls this on the process main thread, whose stack the platform
/// fixes (1 MiB on Windows): the whole turn runs within it.
pub(crate) fn serve(
    args: Args,
    credentials: Option<Credentials>,
    state_directory: Option<std::path::PathBuf>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(in_heap(|| run(args, credentials, state_directory)));
    runtime.shutdown_background();
    result
}

async fn run(
    args: Args,
    credentials: Option<Credentials>,
    state_directory: Option<std::path::PathBuf>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let operation = args
        .operation
        .as_deref()
        .map(OperationId::parse)
        .transpose()?;
    let credentials = match credentials {
        Some(credentials) => credentials,
        None => load_credentials(args.credentials.as_deref())?,
    };
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    let provider = Arc::new(registry.resolve(&args.model)?);
    let mut config = LocalHarnessConfig::new(&args.workspace, (provider.model().clone(), provider));
    if let Some(directory) = state_directory {
        config = config.state_directory(directory);
    }
    if let Some(root) = &args.settings_root {
        config = config.settings(
            acyclic_harness::native_settings::SettingsConfig::new(&args.workspace, root)
                .codex_home(root.join(".codex")),
        );
    }
    let local = Arc::new(in_heap(|| LocalHarness::open(config)).await?);
    let operation =
        in_heap(|| client::local_identity(&local, args.prompt.as_deref(), operation)).await?;
    let command = args
        .prompt
        .as_deref()
        .map(|prompt| client::prepare_local_prompt(operation, prompt))
        .transpose()?;
    if args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
        && !acyclic_harness::tui::term::Environment::capture().is_dumb()
    {
        let result = in_heap(|| {
            client::with_local_commands(local.clone(), operation, || {
                view::run(local.clone(), operation, args.inline)
            })
        })
        .await;
        in_heap(|| client::disconnect_local(local)).await?;
        result?;
        return Ok(0);
    }
    let (mut connection, target) = in_heap(|| client::connect_local(local, operation)).await?;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let outcome = if let Some(command) = command {
        in_heap(|| client::run_headless(&mut connection, command, target, &mut output)).await
    } else {
        in_heap(|| client::run_jsonl(&mut connection, target, client::stdin_jsonl(), &mut output))
            .await
    };
    Ok(in_heap(|| connection.finish(outcome)).await?.exit_code())
}

#[inline(never)]
fn load_credentials(path: Option<&std::path::Path>) -> acyclic_harness::Result<Credentials> {
    Ok(path
        .map(Credentials::from_file)
        .transpose()?
        .unwrap_or_default())
}
