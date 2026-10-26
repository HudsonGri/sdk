//! Argv, the harness client, and shared TUI composition.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

#[path = "graphcoder/args.rs"]
mod args;
#[path = "graphcoder/view.rs"]
mod view;

use acyclic_harness::{
    OperationId, client,
    local::{LocalHarness, LocalHarnessConfig},
};
use acyclic_harness_providers::{Credentials, Registry};
use args::{Args, InputFormat};
use clap::Parser;
use futures::{FutureExt as _, future::LocalBoxFuture};
use std::{future::Future, process::ExitCode, sync::Arc};

// Construct each phase on its own call frame. Boxing an already constructed
// future in the awaiting frame still retains its construction scratch space
// throughout nested polls, and devirtualization can inline its poll there too.
#[inline(never)]
fn phase<'a, F: Future + 'a>(build: impl FnOnce() -> F) -> LocalBoxFuture<'a, F::Output> {
    build().boxed_local()
}

fn main() -> ExitCode {
    let args = Args::parse();
    let result = acyclic_native_runtime::coordination_runtime()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| {
            // Keep Tokio's entry frame independent of the size of the CLI future.
            let result = runtime.block_on(phase(|| run(args)));
            runtime.shutdown_background();
            result
        });
    match result {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            if let Some(cause) = error.downcast_ref::<client::LocalAgentError>() {
                match client::write_agent_unknown(cause, &mut std::io::stderr().lock()) {
                    Ok(true) => return ExitCode::from(client::HeadlessOutcome::Indeterminate.exit_code()),
                    Ok(false) => {},
                    Err(output) => eprintln!("graphcoder: cannot write unresolved commit: {output}"),
                }
            }
            eprintln!("graphcoder: {error}");
            ExitCode::FAILURE
        }
    }
}

/// The local owner exactly as the shipping binary composes it from argv and
/// resolved credentials: the provider registry, its catalog and the
/// workspace's native settings.
async fn open(
    args: &Args,
    credentials: Credentials,
) -> Result<Arc<LocalHarness>, Box<dyn std::error::Error>> {
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    let home = args
        .settings_root
        .clone()
        .or_else(dirs::home_dir)
        .ok_or("user settings directory is unavailable")?;
    let mut settings =
        acyclic_harness::native_settings::SettingsConfig::new(&args.workspace, &home);
    if args.settings_root.is_some() {
        settings = settings.codex_home(home.join(".codex"));
    }
    let selection = match &args.model {
        Some(selection) => selection.clone(),
        None => settings
            .configured_model()?
            .unwrap_or_else(|| registry.default_model().into()),
    };
    let provider = Arc::new(registry.resolve(&selection)?);
    let mut config = LocalHarnessConfig::new(&args.workspace, (provider.model().clone(), provider))
        .working_directory(&std::env::current_dir()?)?;
    if let Some(directory) = &args.directory {
        config = config.active_directory(directory);
    }
    let search = acyclic_harness::search::SearchConfig::from_file(&settings.search_config_path())?;
    let search = if args.settings_root.is_some() {
        search.without_native_sources()
    } else {
        search
    };
    if let Some(provider) = search.provider()? {
        config = config.search_provider(provider);
    }
    config = config.settings(settings).web_tools();
    Ok(Arc::new(phase(|| LocalHarness::open(config)).await?))
}

// Own native phase futures separately from the CLI orchestration frame.
async fn run(args: Args) -> Result<u8, Box<dyn std::error::Error>> {
    if args.host_workspace {
        let directory = args
            .host_state_directory
            .as_ref()
            .ok_or("host state directory absent")?;
        let local = Arc::new(
            phase(|| {
                LocalHarness::open(
                    LocalHarnessConfig::native_host(&args.workspace).state_directory(directory),
                )
            })
            .await?,
        );
        phase(|| {
            acyclic_harness::local::host_files::serve_until(local, async {
                workspace_shutdown()
                    .await
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
            })
        })
        .await?;
        return Ok(0);
    }
    let dumb = acyclic_harness::tui::term::Environment::capture().is_dumb();
    if dumb && args.resume {
        return Err("--resume requires an interactive terminal".into());
    }
    let operation = args
        .operation
        .as_deref()
        .map(OperationId::parse)
        .transpose()?;
    let credentials = args
        .credentials
        .as_deref()
        .map(Credentials::from_file)
        .transpose()?
        .unwrap_or_default();
    let local = phase(|| open(&args, credentials)).await?;
    if args.prompt.is_some()
        || args.input_format == InputFormat::Jsonl
        || dumb
    {
        let mut agent = phase(|| client::LocalAgentClient::connect_recent(local, 4 * 1024 * 1024)).await?;
        let mut output = std::io::stdout().lock();
        let outcome = if let Some(prompt) = &args.prompt {
            let id = acyclic_harness::agent::IntentId::try_from(
                operation.unwrap_or_else(OperationId::new).to_string(),
            )?;
            phase(|| client::run_agent_prompt(&mut agent, &id, prompt, &mut output)).await?
        } else {
            phase(|| client::run_agent_jsonl(&mut agent, client::stdin_jsonl(), &mut output)).await?
        };
        return Ok(outcome.exit_code());
    }
    Ok(phase(|| view::run(local, args.inline, args.resume)).await?.exit_code())
}

// Drop the workspace owner while the runtime is still available so its native
// mounts retire before process exit. The library does not install signal handlers.
async fn workspace_shutdown() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
#[path = "graphcoder/tests.rs"]
mod tests;
