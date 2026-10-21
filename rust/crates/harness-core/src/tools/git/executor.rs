use super::*;

impl<F, Fut> GitFilesystemExecutor for WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    type Error = Error;

    async fn validate(&self) -> Result<(), Self::Error> {
        (self.before_effect)().await?;
        let Some(lease) = &self.lease else {
            return Ok(());
        };
        let snapshot = OperationWindowCoordinator::new(self.fs.operation_window_store())
            .snapshot(lease.workspace_id)
            .await
            .map_err(Error::from)?;
        let active = match snapshot.phase {
            acyclic_fs::OperationWindowPhase::Active { leases, .. } => {
                leases.get(&lease.lease_id).is_some_and(|active| {
                    active.expires_at_millis == lease.expires_at_millis
                        && active.expires_at_millis > now_millis()
                })
            }
            acyclic_fs::OperationWindowPhase::Idle
            | acyclic_fs::OperationWindowPhase::Reconciling { .. } => false,
        };
        if active {
            Ok(())
        } else {
            Err(Self::error("Git compatibility command lease expired"))
        }
    }

    async fn validate_merge_resolution(
        &self,
        operation: OperationId,
        action: &GitFilesystemAction,
        tree: GitTreeRef,
    ) -> Result<GitFilesystemResult, Self::Error> {
        self.validate().await?;
        let GitFilesystemAction::Join { tracked_paths, .. } = action else {
            return Err(Error::Invalid("merge continuation requires a join".into()));
        };
        let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
        let pending = acyclic_fs::in_heap(|| repository.pending_transition())
            .await
            .map_err(super::errors::compat_error)?
            .filter(|pending| pending.id.operation_id() == operation)
            .ok_or_else(|| Self::error("Git merge transition changed"))?;
        let acyclic_fs::GitPendingMutation::Join {
            conflicts: Some(paths),
            ..
        } = &pending.mutation
        else {
            return Err(Self::error(
                "Git merge has unresolved entries; conflicts have not been inspected",
            ));
        };
        let current = acyclic_fs::in_heap(|| self.generation(tree)).await?;
        let Some(tracked_paths) = acyclic_fs::in_heap(|| {
            acyclic_fs::git_merge_resolution(&current, paths, tracked_paths)
        })
        .await?
        else {
            return Err(Self::error(
                "Git merge still contains unresolved conflict markers",
            ));
        };
        self.validate().await?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(tree),
            tracked_paths: Some(tracked_paths),
        })
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        match action {
            GitFilesystemAction::CaptureCommit {
                workspace_tree,
                head_tree,
                head_workspace_tree,
                tracked_paths,
                ..
            } => {
                if matches!(workspace_tree, GitTreeRef::Lazy(_)) {
                    return Ok(GitFilesystemResult::Captured {
                        tree: *workspace_tree,
                        tracked_paths: tracked_paths.clone(),
                        proof: None,
                    });
                }
                let workspace_generation =
                    acyclic_fs::in_heap(|| self.generation(*workspace_tree)).await?;
                let cancellation = acyclic_fs::CancellationToken::new();
                if self.current.id() != workspace_generation.workspace_id()
                    || acyclic_fs::in_heap(|| {
                        self.current
                            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    })
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(Error::from)?
                    .id()
                        != workspace_generation.id()
                {
                    return Err(Self::error("Git commit workspace changed before capture"));
                }
                // Capture is the deepest action: it runs in a heap future of
                // its own, so the other actions' frames never carry it.
                let captured = acyclic_fs::in_heap(|| async {
                    match (head_workspace_tree.as_ref(), head_tree) {
                        (Some(previous_live), Some(previous_capture)) => {
                            let previous_live =
                                acyclic_fs::in_heap(|| self.generation(*previous_live)).await?;
                            let previous_capture =
                                acyclic_fs::in_heap(|| self.generation(*previous_capture)).await?;
                            acyclic_fs::in_heap(|| {
                                capture_git_compatible_generation_incremental(
                                    &self.current,
                                    &previous_live,
                                    &previous_capture,
                                    &self.ignore,
                                    tracked_paths,
                                    operation_id,
                                )
                            })
                            .await
                            .map_err(Error::from)
                        }
                        _ => acyclic_fs::in_heap(|| {
                            capture_git_compatible_generation(
                                &self.current,
                                &self.ignore,
                                tracked_paths,
                                operation_id,
                            )
                        })
                        .await
                        .map_err(Error::from),
                    }
                })
                .await?;
                let graph = WorkspaceGraph::new(self.store.clone());
                let proof =
                    acyclic_fs::in_heap(|| captured.authenticated_proof(operation_id, &graph))
                        .await
                        .map_err(Error::from)?;
                Ok(GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(
                        captured.generation.workspace_id(),
                        captured.generation.id(),
                    ),
                    tracked_paths: captured.tracked_paths,
                    proof,
                })
            }
            GitFilesystemAction::ForkBranch {
                branch,
                source_tree,
                switch,
                ..
            } => {
                let cancellation = acyclic_fs::CancellationToken::new();
                acyclic_fs::in_heap(|| async {
                    let source_matches = match source_tree {
                        GitTreeRef::Exact(source) => {
                            self.current.id() == source.workspace_id
                                && acyclic_fs::in_heap(|| {
                                    self.current
                                        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                                })
                                .await
                                .map(|receipt| receipt.value)
                                .map_err(|failure| failure.error)
                                .map_err(Error::from)?
                                .id()
                                    == source.generation
                        }
                        GitTreeRef::Lazy(source) => {
                            acyclic_fs::in_heap(|| self.lazy_current.tree(false, self.permit))
                                .await
                                .map_err(Error::from)?
                                == GitTreeRef::Lazy(*source)
                        }
                    };
                    if !source_matches {
                        return Err(Self::error("Git branch source workspace changed"));
                    }
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(&self.repository_id.into_bytes());
                    hasher.update(branch.as_bytes());
                    let destination = format!(
                        "git-branch-{}",
                        hex::encode(&hasher.finalize().as_bytes()[..12])
                    );
                    let fork_generation = acyclic_fs::in_heap(|| {
                        self.lazy_current
                            .workspace()
                            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    })
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(Error::from)?
                    .id();
                    let workspace = acyclic_fs::in_heap(|| {
                        self.lazy_current
                            .fork(&destination, operation_id, self.permit)
                    })
                    .await
                    .map_err(Error::from)?;
                    let graph = WorkspaceGraph::new(self.store.clone());
                    acyclic_fs::in_heap(|| {
                        graph.register_existing_child(
                            self.lazy_current.workspace(),
                            workspace.workspace(),
                            fork_generation,
                        )
                    })
                    .await
                    .map_err(Error::from)?;
                    let workspace_id = workspace.workspace().id();
                    if *switch {
                        *self.switched.lock().map_err(|_| {
                            Self::error("Git workspace switch lock is unavailable")
                        })? = Some(workspace);
                    }
                    Ok(GitFilesystemResult::Forked { workspace_id })
                })
                .await
            }
            GitFilesystemAction::SwitchWorkspace { workspace_id } => {
                acyclic_fs::in_heap(|| async {
                    let workspace = acyclic_fs::in_heap(|| self.workspace(*workspace_id)).await?;
                    let lazy_workspace = self
                        .lazy_current
                        .related(workspace)
                        .await
                        .map_err(Error::from)?;
                    let tree = lazy_workspace.view().tree(false, self.permit).await?;
                    *self
                        .switched
                        .lock()
                        .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                        Some(lazy_workspace);
                    Ok(GitFilesystemResult::Applied {
                        tree: Some(tree),
                        tracked_paths: None,
                    })
                })
                .await
            }
            GitFilesystemAction::Diff {
                from,
                to,
                tracked_paths,
            } => {
                acyclic_fs::in_heap(|| async {
                    let to = acyclic_fs::in_heap(|| self.generation(*to)).await?;
                    let value = if let Some(from) = from {
                        let from = acyclic_fs::in_heap(|| self.generation(*from)).await?;
                        let cancellation = CancellationToken::new();
                        let diff = acyclic_fs::in_heap(|| {
                            git_compatible_diff_counts(
                                &from,
                                &to,
                                tracked_paths,
                                100_000,
                                &cancellation,
                            )
                        })
                        .await
                        .map_err(Error::from)?;
                        json!({
                            "from": hex::encode(from.id().digest().as_bytes()),
                            "to": hex::encode(to.id().digest().as_bytes()),
                            "fileChanges": diff.file_changes,
                            "bindingChanges": diff.binding_changes
                        })
                    } else {
                        json!({
                            "from": null,
                            "to": hex::encode(to.id().digest().as_bytes()),
                            "unborn": true
                        })
                    };
                    Ok(GitFilesystemResult::Data {
                        kind: "diff".to_owned(),
                        value,
                    })
                })
                .await
            }
            GitFilesystemAction::RestoreGeneration { tree, paths } => {
                acyclic_fs::in_heap(|| async {
                    if let Some(paths) = paths {
                        let paths = paths.iter().cloned().collect::<Vec<_>>();
                        return acyclic_fs::in_heap(|| {
                            self.restore_paths(operation_id, *tree, &paths)
                        })
                        .await;
                    }
                    let source = acyclic_fs::in_heap(|| self.generation(*tree)).await?;
                    if source.workspace_id() != self.current.id() {
                        return Err(Self::error(
                            "an exact workspace restore cannot use a foreign generation",
                        ));
                    }
                    let cancellation = CancellationToken::new();
                    let current = acyclic_fs::in_heap(|| {
                        self.current.head(WorkBudget::UNBOUNDED, &cancellation)
                    })
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(Error::from)?;
                    let generation = match acyclic_fs::in_heap(|| {
                        self.current.restore_generation(
                            &source,
                            current.id(),
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                            self.permit,
                        )
                    })
                    .await
                    .map_err(Error::from)?
                    {
                        WorkspaceRestore::Restored(generation)
                        | WorkspaceRestore::AlreadyRestored(generation)
                        | WorkspaceRestore::Current(generation) => generation,
                        WorkspaceRestore::Stale(_) => {
                            return Err(Self::error(
                                "Git restore raced with another workspace writer",
                            ));
                        }
                        WorkspaceRestore::Fenced => {
                            return Err(Self::error("Git restore was fenced"));
                        }
                        WorkspaceRestore::IdempotencyConflict => {
                            return Err(Self::error("Git restore retry identity was reused"));
                        }
                    };
                    Ok(GitFilesystemResult::Applied {
                        tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                        tracked_paths: None,
                    })
                })
                .await
            }
            GitFilesystemAction::RestorePaths { tree, paths } => {
                acyclic_fs::in_heap(|| async {
                    self.restore_paths(operation_id, *tree, paths).await
                })
                .await
            }
            GitFilesystemAction::Join {
                source_workspace,
                rebase,
                tracked_paths,
                ..
            } => {
                acyclic_fs::in_heap(|| async {
                    self.join(operation_id, *source_workspace, *rebase, tracked_paths)
                        .await
                })
                .await
            }
            GitFilesystemAction::ApplyCommit {
                base,
                source,
                paths,
                tracked_paths,
                ..
            } => {
                acyclic_fs::in_heap(|| async {
                    let base = match base {
                        Some(base) => Some(acyclic_fs::in_heap(|| self.generation(*base)).await?),
                        None => None,
                    };
                    let source = match source {
                        Some(source) => {
                            Some(acyclic_fs::in_heap(|| self.generation(*source)).await?)
                        }
                        None => None,
                    };
                    // The head read and the path application each run in a
                    // heap frame of their own, so this arm's frame holds
                    // neither future inline.
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let current = acyclic_fs::in_heap(|| {
                        self.current
                            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    })
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(Error::from)?;
                    let paths = paths.iter().cloned().collect::<Vec<_>>();
                    let outcome = acyclic_fs::in_heap(|| {
                        self.current.apply_paths_from(
                            base.as_ref(),
                            source.as_ref(),
                            &paths,
                            current.id(),
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                            self.permit,
                        )
                    })
                    .await
                    .map_err(Error::from)?;
                    let generation = match outcome {
                        WorkspacePathApply::Applied(generation)
                        | WorkspacePathApply::AlreadyApplied(generation)
                        | WorkspacePathApply::NoChanges(generation) => generation,
                        WorkspacePathApply::Conflicted(conflicts) => {
                            return Err(Self::error(
                                serde_json::to_string(&json!({
                                    "kind": "conflicts",
                                    "conflicts": conflicts
                                        .iter()
                                        .map(|conflict| json!({
                                            "path": conflict.path,
                                            "kind": format!("{:?}", conflict.kind),
                                        }))
                                        .collect::<Vec<_>>()
                                }))
                                .map_err(json_error)?,
                            ));
                        }
                        WorkspacePathApply::Stale(_) => {
                            return Err(Self::error(
                                "Git commit application raced with another workspace writer",
                            ));
                        }
                        WorkspacePathApply::Fenced => {
                            return Err(Self::error("Git commit application was fenced"));
                        }
                        WorkspacePathApply::IdempotencyConflict => {
                            return Err(Self::error(
                                "Git commit application retry identity was reused",
                            ));
                        }
                    };
                    let resulting_tracked =
                        acyclic_fs::in_heap(|| existing_tracked_paths(&generation, tracked_paths))
                            .await?;
                    Ok(GitFilesystemResult::Applied {
                        tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                        tracked_paths: Some(resulting_tracked),
                    })
                })
                .await
            }
            GitFilesystemAction::Archive { tree } => {
                acyclic_fs::in_heap(|| async {
                    let generation = acyclic_fs::in_heap(|| self.generation(*tree)).await?;
                    let entries = acyclic_fs::in_heap(|| walk_git_tree(&generation, None, 100_000))
                        .await
                        .map_err(Error::from)?;
                    Ok(GitFilesystemResult::Data {
                        kind: "archive".to_owned(),
                        value: json!({
                            "format": "acyclic-fs-manifest-v1",
                            "generation": hex::encode(generation.id().digest().as_bytes()),
                            "entries": entries
                        }),
                    })
                })
                .await
            }
            GitFilesystemAction::Grep {
                pattern,
                path,
                tree,
            } => {
                acyclic_fs::in_heap(|| async {
                    let generation = acyclic_fs::in_heap(|| self.generation(*tree)).await?;
                    let result = grep_git_generation(
                        &generation,
                        pattern,
                        path.as_deref(),
                        100_000,
                        16 * 1024 * 1024,
                        100_000,
                    )
                    .await
                    .map_err(Error::from)?;
                    Ok(GitFilesystemResult::Data {
                        kind: "grep".to_owned(),
                        value: serde_json::to_value(result).map_err(json_error)?,
                    })
                })
                .await
            }
            GitFilesystemAction::Blame { path, commits } => {
                acyclic_fs::in_heap(|| async {
                    let mut history = Vec::with_capacity(commits.len());
                    for commit in commits {
                        let generation =
                            acyclic_fs::in_heap(|| self.generation(commit.tree)).await?;
                        history.push((commit.clone(), generation));
                    }
                    let lines = blame_git_generations(&history, path, 16 * 1024 * 1024)
                        .await
                        .map_err(Error::from)?;
                    Ok(GitFilesystemResult::Data {
                        kind: "blame".to_owned(),
                        value: serde_json::to_value(lines).map_err(json_error)?,
                    })
                })
                .await
            }
            GitFilesystemAction::Clean {
                dry_run,
                tree,
                tracked_paths,
            } => {
                acyclic_fs::in_heap(|| async {
                    let generation = acyclic_fs::in_heap(|| self.generation(*tree)).await?;
                    if generation.workspace_id() != self.current.id() {
                        return Err(Self::error(
                            "Git clean generation is not the live workspace",
                        ));
                    }
                    let entries = acyclic_fs::in_heap(|| walk_git_tree(&generation, None, 100_000))
                        .await
                        .map_err(Error::from)?;
                    let candidates: Vec<_> = entries
                        .into_iter()
                        .filter(|entry| {
                            entry.kind != "directory"
                                && !tracked_paths.contains(&entry.path)
                                && self.ignore.eligible(&entry.path, false, false)
                        })
                        .map(|entry| entry.path)
                        .collect();
                    if *dry_run || candidates.is_empty() {
                        return Ok(GitFilesystemResult::Data {
                            kind: "clean".to_owned(),
                            value: json!({ "paths": candidates, "dryRun": dry_run }),
                        });
                    }
                    let mut transaction = self
                        .current
                        .begin_transaction(
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .map_err(Error::from)?;
                    for path in &candidates {
                        transaction
                            .remove(
                                &format!("/{path}"),
                                acyclic_fs::WorkBudget::UNBOUNDED,
                                &acyclic_fs::CancellationToken::new(),
                            )
                            .await
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error)
                            .map_err(Error::from)?;
                    }
                    committed(
                    transaction
                        .commit(
                            None,
                            self.permit,
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                            |_| async {
                                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                                    acyclic_fs::OperationReceipt {
                                        value: (),
                                        work: acyclic_fs::WorkCounters::default(),
                                    },
                                )
                            },
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .map_err(Error::from)?,
                )
                })
                .await
            }
            GitFilesystemAction::ApplyPatch { patch } => committed(
                apply_git_patch(
                    &self.current,
                    patch,
                    IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    self.permit,
                )
                .await
                .map_err(Error::from)?,
            ),
            GitFilesystemAction::CheckIgnore { paths, tree } => {
                acyclic_fs::in_heap(|| async {
                    let generation = acyclic_fs::in_heap(|| self.generation(*tree)).await?;
                    if generation.workspace_id() != self.current.id()
                        || self
                            .current
                            .head(
                                acyclic_fs::WorkBudget::UNBOUNDED,
                                &acyclic_fs::CancellationToken::new(),
                            )
                            .await
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error)
                            .map_err(Error::from)?
                            .id()
                            != generation.id()
                    {
                        return Err(Self::error(
                            "Git check-ignore live generation changed before inspection",
                        ));
                    }
                    let ignored = paths
                        .iter()
                        .filter(|path| !self.ignore.eligible(path, false, false))
                        .cloned()
                        .collect::<Vec<_>>();
                    Ok(GitFilesystemResult::Data {
                        kind: "check-ignore".to_owned(),
                        value: json!({ "paths": ignored }),
                    })
                })
                .await
            }
            unsupported => Err(Self::error(format!(
                "Git filesystem action is not yet available in the Codex adapter: {unsupported:?}"
            ))),
        }
    }
}
