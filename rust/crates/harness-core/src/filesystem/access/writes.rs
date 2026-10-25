use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn authorize_content_write(
        &self,
        workspace: &Workspace<A, O>,
        grant: &ContentGrant,
        paths: &[&str],
        replay: bool,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<(
        GenerationRef,
        acyclic_fs::access::PolicyFence,
        Option<(Generation<A, O>, Vec<bool>)>,
    )> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        grant.recheck(Some(io))?;
        let scope = grant.policy_scope();
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let view = crate::filesystem::access::access_view(
            workspace,
            scope,
            None,
            capability_payload(label, VolumeOperation::Write, io)?,
            ceiling,
            io,
        )
        .await?;
        let fence = view
            .0
            .publication_fence()
            .ok_or_else(|| io.invalid("missing write policy fence"))?;
        // Refuse before probing existence when neither mutation can succeed.
        for path in paths {
            if !view.0.decide(Action::Create, path, access_now()).allowed {
                require(&view.0, Action::Write, path, io)?;
            }
        }
        if replay {
            return Ok((self.generation_ref_id(view.2, Some(io))?, fence, None));
        }
        let generation = io.sdk(workspace.generation(view.2, io, io.cancellation()).await)?;
        let mut present = crate::model::vec_with_capacity(paths.len(), Some(io))?;
        for path in paths {
            present.push(Self::authorize_target(&view.0, &generation, path, io).await?);
        }
        Ok((
            self.generation_ref_id(view.2, Some(io))?,
            fence,
            Some((generation, present)),
        ))
    }

    /// Checks one target and every missing parent; returns whether the target exists.
    async fn authorize_target(
        view: &PathTrie,
        generation: &Generation<A, O>,
        path: &str,
        io: &PreparationIo,
    ) -> Result<bool> {
        let absolute = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let hold = io.guard_owned(absolute.capacity() as u64)?;
        let absolute = (absolute, hold);
        // Check the target and every missing parent. Existing parents need no
        // disclosure grant: create-only inboxes must remain usable without List.
        let mut candidate = path;
        let mut target_present = false;
        loop {
            let prefix = absolute
                .0
                .get(..candidate.len() + 1)
                .ok_or_else(|| io.invalid("invalid parent path boundary"))?;
            let present = match io.sdk(generation.stat(prefix, io, io.cancellation()).await) {
                Ok(_) => true,
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => false,
                Err(error) => return Err(error),
            };
            if candidate == path {
                target_present = present;
            }
            if candidate == path || !present {
                let action = if present {
                    Action::Write
                } else {
                    Action::Create
                };
                require(view, action, candidate, io)?;
            }
            let Some((parent, _)) = candidate.rsplit_once('/') else {
                break;
            };
            candidate = parent;
        }
        Ok(target_present)
    }
}
