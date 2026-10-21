use super::*;
use crate::conversation::ContentGrant;

impl LocalHarness {
    /// Registers request-file tool execution in an existing composition.
    pub fn register_commands(
        tasks: &mut TaskRegistry,
        machines: &mut MachineRegistry,
    ) -> Result<()> {
        let direct = Arc::new(command_machine::DirectTool::new());
        tasks.register(direct.definition()?)?;
        machines.register(direct, &mut None)
    }

    /// Binds command files to an existing runtime and its authenticated volume.
    pub async fn bind(
        mut config: LocalHarnessConfig,
        runtime: LocalTaskRuntime,
        files: Arc<LocalFilesystem>,
        storage: (acyclic_fs::LocalFs, Arc<LocalStream>),
        authority: (&AuthorityVerifier, &Scope),
        checkout: Option<&Path>,
    ) -> Result<Self> {
        config.limits.validate(&mut None)?;
        #[cfg(feature = "native-execution")]
        if checkout.is_some() {
            config = config.prepare_state()?;
        }
        #[cfg(feature = "native-execution")]
        let state_owner = config.test_state.clone();
        #[cfg(feature = "native-execution")]
        let checkout_state = checkout
            .map(|checkout| {
                owner::separated_directory(&config.resolved_state_directory()?, Some(checkout))
            })
            .transpose()?;
        let (verifier, signed) = authority;
        #[cfg(feature = "search-http")]
        config.validate_web_binding(runtime.tool_registry(), verifier, signed)?;
        let LocalProject::Volume { volume, .. } = &config.project else {
            return Err(Error::Invalid(
                "existing runtime requires its bound volume".into(),
            ));
        };
        verify_grants(verifier, signed, volume, config.limits.file_bytes)?;
        #[cfg(feature = "hooks")]
        let settings =
            settings::configuration(&config.project, config.settings.take(), config.hermetic)?;
        config.model.0.validate()?;
        let compaction = compaction::select(
            config.compaction.take(),
            &config.model.0,
            config.model.1.as_ref(),
            runtime.tool_registry(),
            config.limits,
        )
        .await?;
        let (filesystem, stream) = storage;
        let (runtime, task_files) = bind_task_files(
            runtime,
            stream,
            (volume, verifier, signed),
            config.limits.file_bytes,
        )
        .await?;
        #[cfg(feature = "native-execution")]
        let workspace = match checkout.zip(checkout_state) {
            Some(checkout) => {
                Some(open_bound_checkout(&filesystem, volume, checkout, state_owner).await?)
            }
            None => None,
        };
        #[cfg(not(feature = "native-execution"))]
        {
            let _ = filesystem;
            if checkout.is_some() {
                return Err(Error::Unsupported(
                    "checkout commands require native-execution".into(),
                ));
            }
        }
        #[cfg(feature = "native-execution")]
        let runtime = match &workspace {
            Some(workspace) => runtime.with_workspace_tools(Arc::new(
                checkout_tools::CheckoutTools::new(
                    Arc::clone(workspace),
                    volume,
                    config.clock,
                    Arc::clone(&task_files),
                )
                .await?,
            ))?,
            None => runtime,
        };
        let (model, provider) = config.model;
        let local = Self {
            files,
            runtime: Arc::new(runtime),
            task_files,
            project: volume.clone(),
            scratch: volume.clone(),
            journal: volume.clone(),
            verifier: verifier.clone(),
            issuer: None,
            signed: signed.clone(),
            model,
            provider,
            context_manifest: config.context_manifest,
            #[cfg(feature = "hooks")]
            settings,
            #[cfg(test)]
            context_read_observer: None,
            limits: config.limits,
            compaction,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: Vec::new(),
            goal_continuations: config.goal_continuations,
            admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            publication: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            drivers: super::orphans::Drivers::new(),
            #[cfg(feature = "native-execution")]
            workspace,
            #[cfg(feature = "native-execution")]
            checkout_context: checkout.is_some(),
        };
        Ok(local)
    }

    /// The same task runtime used by request-file consumers and SDK queries.
    pub fn task_runtime(&self) -> &Arc<LocalTaskRuntime> {
        &self.runtime
    }
}

/// Refuses a bound volume the caller's scope cannot both read and write.
fn verify_grants(
    verifier: &AuthorityVerifier,
    signed: &Scope,
    volume: &VolumeRef,
    file_bytes: u64,
) -> Result<()> {
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        ContentGrant::verify(verifier, signed, volume, operation, file_bytes, &mut None)?;
    }
    Ok(())
}

type BoundTaskFiles = Arc<crate::task_volume::TaskFiles<LocalStream>>;

/// Opens the bound volume's task files and binds them to `runtime`.
async fn bind_task_files(
    runtime: LocalTaskRuntime,
    stream: Arc<LocalStream>,
    (volume, verifier, signed): (&VolumeRef, &AuthorityVerifier, &Scope),
    file_bytes: u64,
) -> Result<(LocalTaskRuntime, BoundTaskFiles)> {
    let task_files = Arc::new(
        crate::task_volume::TaskFiles::open(stream, volume, verifier, signed, file_bytes).await?,
    );
    let erased_files = Arc::new(task_files.reopen_erased().await?);
    let mut runtime = runtime.with_task_files(erased_files)?;
    runtime.bind_task_files(Some(task_files.clone()));
    Ok((runtime, task_files))
}

/// Opens the checkout bound to `volume` from the root it holds now; the
/// attach operation is derived from the volume so a rebind replays it.
#[cfg(feature = "native-execution")]
async fn open_bound_checkout(
    filesystem: &acyclic_fs::LocalFs,
    volume: &VolumeRef,
    (checkout, directory): (&Path, std::path::PathBuf),
    state_owner: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<Arc<workspace::LocalWorkspace>> {
    let root = acyclic_fs::hold_root(&std::fs::canonicalize(checkout).map_err(storage_error)?)
        .map_err(storage_error)?;
    let digest = blake3::derive_key(
        "acyclic.local.bound-checkout/v1",
        &serde_json::to_vec(volume).map_err(storage_error)?,
    );
    let operation =
        acyclic_fs::OperationId::from_bytes(digest[..16].try_into().map_err(storage_error)?);
    let store = acyclic_fs::LocalCoreStateStore::new(directory.join("workspace"))
        .with_ownership_anchor(state_owner);
    let attach = workspace::Attach {
        volume,
        scratch: volume,
        directory: &directory,
        operation,
        native_mounts: false,
    };
    Ok(Arc::new(
        workspace::open(filesystem, &attach, root, true, store).await?,
    ))
}

#[cfg(feature = "native-execution")]
fn storage_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
