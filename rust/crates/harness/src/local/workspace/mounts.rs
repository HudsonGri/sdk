//! Task routes retain the provider's authenticated policy view.
use super::*;

pub(super) async fn owner(
    root: &Path,
    filesystem: &acyclic_fs::LocalFs,
    workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    volume: &VolumeRef,
    volumes: impl IntoIterator<Item = (String, VolumeRef)>,
    authority: (&AuthorityIssuer, AgentId),
) -> Result<Mounts> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let options = crate::filesystem::owner_mount_options(
        workspace,
        authority.0,
        Some(authority.1),
        true,
        &work,
    )
    .await?;
    let access = Arc::clone(options.require_access_view()?);
    let head = access.current().map_err(storage)?.head_position();
    let primary = crate::workspace::VolumeView {
        name: "project".into(),
        agent: authority.1.to_string(),
        volume: volume.clone(),
        generation: head,
        head,
        access,
        source: None,
    };
    let mut others = Vec::new();
    for (name, volume) in volumes {
        let workspace = filesystem
            .open_workspace(&volume.storage_name(&mut None)?, &work, &cancellation)
            .await
            .map_err(storage)?;
        let options = crate::filesystem::owner_mount_options(
            &workspace,
            authority.0,
            Some(authority.1),
            true,
            &work,
        )
        .await?
        .publication(acyclic_fs::MountPublication::Manual);
        let access = Arc::clone(options.require_access_view()?);
        let head = access.current().map_err(storage)?.head_position();
        let source = Some(Arc::new(MountedVolume {
            workspace: crate::tools::git::OwnedView::Volume(workspace),
            destination: root.join(".volumes").join(&name),
            options,
            native: StateArc::new(AsyncMutex::new(None)),
        }));
        others.push(crate::workspace::VolumeView {
            name,
            agent: authority.1.to_string(),
            volume,
            generation: head,
            head,
            access,
            source,
        });
    }
    crate::workspace::compose(
        root.into(),
        crate::filesystem::access::access_now(),
        primary,
        others,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::access::Action;
    use proptest::prelude::*;

    fn owner<T>(
        root: &Path,
        volume: &VolumeRef,
        source: T,
        volumes: impl IntoIterator<Item = (String, VolumeRef, T)>,
    ) -> Result<crate::workspace::MountTable<T>> {
        use acyclic_fs::access::*;
        let principal = Principal {
            kind: PrincipalKind::Agent,
            id: "owner".into(),
            lineage: vec!["owner".into()],
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        };
        let policy: Policy = serde_json::from_slice(INITIAL_POLICY)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: Action::ALL.into_iter().collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        };
        let view = |name, volume, source| -> Result<_> {
            Ok(crate::workspace::VolumeView {
                name,
                agent: "owner".into(),
                volume,
                generation: 0,
                head: 0,
                source,
                access: crate::workspace::fixtures::access(
                    &policy,
                    &principal,
                    &capability,
                    Resource {
                        config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Ephemeral),
                        lineage: &principal.lineage,
                        volume: "",
                        generation: 0,
                        head: 0,
                        path: "",
                    },
                    Context {
                        now: 0,
                        approvals: 0,
                    },
                )?,
            })
        };
        let primary = view("project".into(), volume.clone(), source)?;
        let others = volumes
            .into_iter()
            .map(|(name, volume, source)| view(name, volume, source))
            .collect::<Result<Vec<_>>>()?;
        crate::workspace::compose(root.into(), 0, primary, others)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Replacing a provider binding cannot change the namespace, access
        // decisions or volume identity compiled at composition time.
        #[test]
        fn binding_keeps_the_composed_authority(sources in any::<[u8; 2]>(), entry in "[a-z]{1,8}(/[a-z]{1,8}){0,3}") {
            let provider = crate::resources::ProviderRef::new(
                "binding".into(), "filesystem".into(), "1".into(), &mut None,
            ).unwrap();
            let volume = |name: &str| VolumeRef::new(provider.clone(), name.into(),
                VolumeClass::Project, VolumeOwner::Project("owner".into()), &mut None).unwrap();
            let root = std::env::current_dir().unwrap();
            let mut mounts = owner(&root, &volume("project"), sources[0],
                [("data".into(), volume("data"), sources[1])]).unwrap();
            let before: Vec<_> = mounts.mounts().map(|(path, mount)|
                (path.to_owned(), mount.view.volume.clone(), mount.mode, mount.view.source)).collect();
            for (_, mount) in mounts.mounts_mut() {
                mount.view.source = mount.view.source.wrapping_add(1);
            }
            for (path, volume, mode, source) in before {
                let (mounted, relative) = mounts.resolve(Path::new(&path)).unwrap();
                let qualifier = if path.is_empty() { "project" } else { path.strip_prefix(".volumes/").unwrap() };
                let (selected_path, selected) = mounts.named(Some(qualifier)).unwrap();
                prop_assert_eq!(selected_path, path.as_str());
                prop_assert_eq!(&selected.view.volume, &volume);
                prop_assert!(relative.is_empty());
                prop_assert_eq!(&mounted.view.volume, &volume);
                prop_assert_eq!(mounted.mode, mode);
                prop_assert_eq!(mounted.view.source, source.wrapping_add(1));
                prop_assert!(mounted.check(Action::Read, "file", 0).is_ok());
                prop_assert!(mounted.check(Action::Write, "file", 0).is_ok());
            }
            for (path, mount) in mounts.mounts() {
                let task_path = if path.is_empty() { entry.clone() } else { format!("{path}/{entry}") };
                let (routed, relative) = mounts.file(None, &task_path).unwrap();
                prop_assert_eq!(&routed.view.volume, &mount.view.volume);
                prop_assert_eq!(&relative, &entry);
                let (routed_again, same_path) = mounts.file(Some(&routed.view.volume), &relative).unwrap();
                prop_assert_eq!(&routed_again.view.volume, &mount.view.volume);
                prop_assert_eq!(same_path, relative);
            }
            prop_assert!(mounts.file(Some(&volume("project")), ".volumes/data/entry").is_err());
            prop_assert!(mounts.file(None, "../entry").is_err());
            prop_assert!(mounts.named(Some("journal")).is_err());
            prop_assert!(mounts.named(Some("../data")).is_err());
            prop_assert_eq!(&mounts.named(None).unwrap().1.view.volume, &volume("project"));
        }
    }
}
