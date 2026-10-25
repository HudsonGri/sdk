//! Share immutable closed-volume bytes, never live providers or effect ledgers.
use super::super::admission::snapshot::Snapshot;
use super::*;

#[derive(Debug, PartialEq)]
pub(in crate::local) struct VolumeInitialized;

pub(in crate::local) struct Bootstrap {
    source: tempfile::TempDir,
    filesystem: Snapshot,
    state: Snapshot,
    project: VolumeRef,
    pub agent: Option<crate::AgentId>,
}

impl Bootstrap {
    pub async fn new() -> Result<Self> {
        let source = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(source.path()).await?;
        let config =
            volume_config(source.path(), project.clone(), scripted([]))?.without_default_tools();
        let state = config
            .state_directory
            .as_ref()
            .expect("explicit fixture state");
        assert!(state.starts_with(source.path()));
        assert_eq!(&config.resolved_state_directory()?, state);
        assert!(super::super::owner::user_state_root().is_err());
        let harness = LocalHarness::open(config).await?;
        let agent = harness.signed.agent();
        harness.close().await?;
        Self::capture(source, project, agent)
    }

    /// Consume and close the only live host before copying its durable bytes.
    pub async fn checkpoint(
        &self,
        directory: tempfile::TempDir,
        harness: LocalHarness,
    ) -> Result<Self> {
        assert_eq!(harness.project(), &self.project);
        assert_eq!(harness.signed.agent(), self.agent);
        harness.close().await?;
        Self::capture(directory, self.project.clone(), self.agent)
    }

    fn capture(
        source: tempfile::TempDir,
        project: VolumeRef,
        agent: Option<crate::AgentId>,
    ) -> Result<Self> {
        Ok(Self {
            filesystem: Snapshot::capture(&source.path().join("filesystem"))?,
            state: Snapshot::capture(
                &source
                    .path()
                    .join("state")
                    .join(project.storage_name(&mut None)?),
            )?,
            source,
            project,
            agent,
        })
    }

    pub fn restore(&self) -> Result<tempfile::TempDir> {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        assert_ne!(directory.path(), self.source.path());
        self.filesystem
            .restore(&directory.path().join("filesystem"))?;
        let parent = directory.path().join("state");
        std::fs::create_dir(&parent).map_err(super::super::storage)?;
        self.state
            .restore(&parent.join(self.project.storage_name(&mut None)?))?;
        Ok(directory)
    }

    pub fn config(
        &self,
        directory: &Path,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<LocalHarnessConfig> {
        Ok(volume_config(directory, self.project.clone(), provider)?.without_default_tools())
    }
}
