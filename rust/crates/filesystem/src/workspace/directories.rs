use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Creates missing directory prefixes without replacing existing objects.
    pub async fn create_dir_all<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.create_dirs(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn create_dirs(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let depth = PortablePath::validate(path, self.checkout.volume_config().limits).map_err(
            |error| WorkspaceError::from(crate::kernel::NamespacePathError::Path(error)),
        )?;
        if depth == 0 {
            return Ok(());
        }
        let config = self.checkout.volume_config();
        if config.profile == crate::model::FilesystemProfile::Windows
            || config.case_sensitivity != crate::model::CaseSensitivity::Sensitive
            || depth <= 1
        {
            return self
                .create_dirs_individually(path, accounting, cancellation)
                .await;
        }
        let mut parsed = Some(self.mutation_path(path, accounting, cancellation)?);
        let result = async {
            let lookup = self
                .checkout
                .directory_prefix_with_accounting(
                    &parsed.as_ref().ok_or(FsError::InvalidDiff)?.value,
                    accounting,
                    cancellation,
                )
                .await?;
            if let Some(record) = lookup.record {
                return if record.kind == FileKind::Directory {
                    Ok(())
                } else {
                    Err(WorkspaceError::NotDirectory)
                };
            }
            // Retain the full parsed path for the final mutation instead of
            // validating and allocating its names again after the lookup.
            for prefix in prefixes(path)
                .take(depth - 1)
                .skip(usize::from(lookup.resolved_components))
            {
                let prefix = self.mutation_path(prefix, accounting, cancellation)?;
                self.create_directory_path(prefix, accounting, cancellation)
                    .await?;
            }
            self.create_directory_path(
                parsed.take().ok_or(FsError::InvalidDiff)?,
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        if let Some(parsed) = parsed {
            parsed.retire(accounting)?;
        }
        result
    }

    async fn create_directory_path(
        &mut self,
        path: crate::kernel::Owned<NamespacePath>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_mutation(
            &mut operations,
            crate::kernel::Owned::new(
                AuthoredMutation::CreateDirectory {
                    path: path.value,
                    metadata: FileMetadata::default(),
                },
                path.allocation,
                path.bytes,
            ),
            accounting,
        )?;
        self.apply_authored(operations, accounting, cancellation)
            .await
    }

    pub(crate) async fn create_dirs_individually(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        for end in path
            .match_indices('/')
            .skip(1)
            .map(|(index, _)| index)
            .chain(std::iter::once(path.len()))
        {
            let prefix = path.get(..end).ok_or(crate::FsError::InvalidDiff)?;
            let path = self.mutation_path(prefix, accounting, cancellation)?;
            match self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
                .await?
                .0
                .record
            {
                Some(record) if record.kind == FileKind::Directory => path.retire(accounting)?,
                Some(_) => {
                    path.retire(accounting)?;
                    return Err(WorkspaceError::NotDirectory);
                }
                None => {
                    self.create_directory_path(path, accounting, cancellation)
                        .await?
                }
            }
        }
        Ok(())
    }
}

fn prefixes(path: &str) -> impl Iterator<Item = &str> {
    path.match_indices('/')
        .skip(1)
        .map(|(index, _)| path.get(..index).expect("separator is a UTF-8 boundary"))
        .chain(std::iter::once(path))
}
