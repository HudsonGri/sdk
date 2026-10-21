//! The phases of opening a standalone local harness: its tools, then its
//! durable state and storage.
use super::*;

/// The tools, settings and compaction policy a standalone harness runs with.
pub(super) struct Tooling {
    pub(super) tools: ToolRegistry,
    pub(super) compaction: crate::context::CompactionPolicy,
    #[cfg(feature = "native-execution")]
    pub(super) native_mounts: bool,
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) settings: Arc<crate::native_settings::SettingsConfig>,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) capabilities: Capabilities,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) diagnostics: Vec<crate::mcp::McpServerFailure>,
}

impl Tooling {
    /// Registers the configured tools, installs MCP servers, selects
    /// compaction and starts the native services the tools need.
    pub(super) async fn assemble(config: &mut LocalHarnessConfig) -> Result<Self> {
        let mut tools = ToolRegistry::default();
        if config.default_tools {
            crate::tools::bind_defaults(
                &mut tools,
                &mut Vec::new(),
                None,
                true,
                cfg!(feature = "native-execution"),
            )?;
        }
        for tool in std::mem::take(&mut config.tools) {
            tools.register((tool, None), &mut None, None)?;
        }
        #[cfg(feature = "search-http")]
        if config.web_tools {
            web::WebExecutor::bind(&mut tools, config.search_provider.take())?;
        }
        #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        let settings =
            settings::configuration(&config.project, config.settings.take(), config.hermetic)?;
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let mcp = crate::native_settings::NativeSettings::load_mcp(Arc::clone(&settings)).await?;
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let mcp_workspace =
            std::fs::canonicalize(config.project.root()).map_err(Error::process_io)?;
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let installation = mcp
            .install(tools, &mcp_workspace, config.mcp_limits)
            .await?;
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let tools = installation.tools;
        let compaction = compaction::select(
            config.compaction.take(),
            &config.model.0,
            config.model.1.as_ref(),
            &tools,
            config.limits,
        )
        .await?;
        #[cfg(feature = "native-execution")]
        let native_mounts = tools.get("bash", None, None)?.is_some();
        acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
        #[cfg(feature = "native-execution")]
        acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
        #[cfg(feature = "native-execution")]
        if native_mounts {
            workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
            acyclic_fs::initialize_native_mount_services().map_err(storage)?;
        }
        Ok(Self {
            tools,
            compaction,
            #[cfg(feature = "native-execution")]
            native_mounts,
            #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            settings,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            capabilities: installation.capabilities,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            diagnostics: installation.diagnostics,
        })
    }
}

/// The durable state a standalone harness opened for its project.
pub(super) struct Storage {
    pub(super) owner: owner::Owner,
    pub(super) files: Arc<LocalFilesystem>,
    pub(super) task_volume: VolumeRef,
    pub(super) scratch: VolumeRef,
    pub(super) journal: VolumeRef,
    #[cfg(feature = "native-execution")]
    pub(super) workspace: Option<Arc<workspace::LocalWorkspace>>,
    pub(super) stream: Arc<LocalStream>,
    pub(super) issuer: AuthorityIssuer,
    pub(super) verifier: AuthorityVerifier,
}

impl Storage {
    /// Selects and claims the project's state, then opens its filesystem,
    /// private volumes, native view and task stream.
    pub(super) async fn open(
        config: &LocalHarnessConfig,
        state_location: config::StateLocation,
        tooling: &Tooling,
    ) -> Result<Self> {
        let (directory, checkout) = select_state(config, state_location)?;
        let owner = owner::Owner::open(&directory, &config.project, checkout.as_ref())?;
        let options = acyclic_fs::LocalOptions::new(match &config.project {
            LocalProject::Workspace(_) => directory.join("filesystem"),
            LocalProject::Volume { directory, .. } => directory.clone(),
        });
        let state_owner = config.test_state.clone();
        let filesystem = match &state_owner {
            Some(owner) if matches!(&config.project, LocalProject::Workspace(_)) => {
                acyclic_fs::Fs::local_with_ownership_anchor(options, owner.clone()).await
            }
            _ => acyclic_fs::Fs::local(options).await,
        }
        .map_err(storage)?;
        let provider_ref = owner.project.provider();
        let files = FilesystemHost::new(filesystem.clone(), provider_ref, None)?;
        let [task_volume, scratch, journal] = agent_volumes(&files, &owner).await?;
        #[cfg(feature = "native-execution")]
        let attach = workspace::Attach {
            volume: &owner.project,
            scratch: &scratch,
            directory: &directory,
            operation: workspace::attach_operation(owner.task),
            native_mounts: tooling.native_mounts,
        };
        #[cfg(feature = "native-execution")]
        let workspace = project_workspace(
            &config.project,
            &filesystem,
            &attach,
            checkout,
            state_owner.clone(),
        )
        .await?;
        #[cfg(not(feature = "native-execution"))]
        let _ = (tooling, checkout);
        #[cfg(feature = "native-execution")]
        let files = files
            .with_project_family(workspace.view().workspace().id(), workspace.store.clone())?;
        // A configured project is opened, never silently replaced by an empty volume.
        files
            .resolve(
                &crate::filesystem::workspace_ref(
                    provider_ref.clone(),
                    &owner.project.storage_name(&mut None)?,
                )?,
                None,
            )
            .await?;
        let stream = Arc::new(
            LocalStream::open_with_clock_and_anchor(
                directory.join("stream"),
                Default::default(),
                config.clock.clone(),
                state_owner,
            )
            .await
            .map_err(storage)?,
        );
        let authority = Authority {
            kind: AggregateKind::Task,
            id: owner.task.to_string(),
        };
        let issuer = AuthorityIssuer::new("acyclic.local", owner.key, authority);
        let verifier = issuer.verifier(&mut None)?;
        Ok(Self {
            owner,
            files,
            task_volume,
            scratch,
            journal,
            #[cfg(feature = "native-execution")]
            workspace: Some(Arc::new(workspace)),
            stream,
            issuer,
            verifier,
        })
    }
}

/// Selects the control directory and captures the checkout. A caller's
/// overlapping control directory is refused before capture can publish the
/// checkout's incarnation token.
fn select_state(
    config: &LocalHarnessConfig,
    state_location: config::StateLocation,
) -> Result<(PathBuf, Option<owner::Checkout>)> {
    #[cfg(not(feature = "native-execution"))]
    if matches!(config.project, LocalProject::Workspace(_)) {
        return Err(Error::Unsupported(
            "checkout workspaces require native-execution".into(),
        ));
    }
    if let LocalProject::Workspace(path) = &config.project
        && !path.is_dir()
    {
        return Err(Error::Invalid("local workspace is not a directory".into()));
    }
    let selected_directory = match (&state_location, &config.project) {
        (config::StateLocation::Directory(directory), LocalProject::Workspace(path)) => {
            Some(owner::separated_directory(directory, Some(path))?)
        }
        (config::StateLocation::ProjectRoot(root), LocalProject::Workspace(path)) => {
            owner::separated_directory(root, Some(path))?;
            None
        }
        _ => None,
    };
    let checkout = match &config.project {
        LocalProject::Workspace(path) => Some(owner::Checkout::capture(path)?),
        LocalProject::Volume { .. } => None,
    };
    let state_directory = state_location.resolve(&config.project, checkout.as_ref())?;
    let directory = match selected_directory {
        Some(directory) => directory,
        None => owner::separated_directory(
            &state_directory,
            checkout.as_ref().map(|checkout| checkout.path.as_path()),
        )?,
    };
    #[cfg(test)]
    let checkout = match (&config.project, owner::RECAPTURE.get()) {
        (LocalProject::Workspace(path), true) => Some(owner::Checkout::capture(path)?),
        _ => checkout,
    };
    Ok((directory, checkout))
}

/// Creates the agent's private task, scratch and journal volumes.
async fn agent_volumes(files: &LocalFilesystem, owner: &owner::Owner) -> Result<[VolumeRef; 3]> {
    let volume = |kind: &str| {
        VolumeRef::new(
            owner.project.provider().clone(),
            format!("{kind}-{}", owner.agent),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner.agent),
            &mut None,
        )
    };
    let volumes = [volume("tasks")?, volume("scratch")?, volume("journal")?];
    for volume in &volumes {
        files.create_volume(volume).await?;
    }
    Ok(volumes)
}

/// Opens the project's lazy native view: the captured checkout, or the
/// configured volume's private materialization.
#[cfg(feature = "native-execution")]
async fn project_workspace(
    project: &LocalProject,
    filesystem: &acyclic_fs::LocalFs,
    attach: &workspace::Attach<'_>,
    checkout: Option<owner::Checkout>,
    state_owner: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<workspace::LocalWorkspace> {
    // This standalone composition owns its private companion namespace.
    // Retain the existing owned WAL instead of reopening/checkpointing it
    // for every transaction. Embedded bind() retains shared-store semantics.
    let store =
        acyclic_fs::LocalCoreStateStore::open_shared_owner(attach.directory.join("workspace"))
            .map_err(storage)?
            .with_ownership_anchor(state_owner);
    match project {
        LocalProject::Workspace(_) => {
            let root = checkout
                .ok_or_else(|| Error::Invalid("checkout capture is absent".into()))?
                .root;
            workspace::open(filesystem, attach, root, false, store).await
        }
        LocalProject::Volume { .. } => workspace::open_volume(filesystem, attach, store).await,
    }
}
