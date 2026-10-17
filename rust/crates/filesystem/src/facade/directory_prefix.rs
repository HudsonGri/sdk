use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    // One walk captures every prefix record that separate no-follow lookups
    // would observe. A non-directory prefix is returned before descending.
    pub(crate) async fn directory_prefix_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::PathLookup, FsError> {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            self.synchronize_live_with_accounting(accounting, cancellation)
                .await?;
            let observing = self.tracks_observations();
            let candidate = if !observing || self.has_pending_mutations() {
                Some(
                    crate::kernel::resolve_path_with_accounting(
                        &self.volume.fs.inner.objects,
                        &self.root,
                        path,
                        self.volume.config,
                        accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                        false,
                        crate::kernel::CaptureRecords::Prefixes,
                    )
                    .await?
                    .0
                    .lookup,
                )
            } else {
                None
            };
            if !observing {
                return candidate.ok_or(FsError::InvalidDiff);
            }
            let (mut observed, mut allocation) = crate::kernel::resolve_path_with_accounting(
                &self.volume.fs.inner.objects,
                &self.base_root,
                path,
                self.volume.config,
                accounting,
                cancellation,
                Some(&self.dependencies.data_custody),
                true,
                crate::kernel::CaptureRecords::Prefixes,
            )
            .await?;
            self.dependencies.extend_observations(
                std::mem::take(&mut observed.dependencies),
                &mut allocation,
                self.volume.config.limits.maximum_checkout_dependencies,
                accounting,
                cancellation,
            )?;
            Ok(candidate.unwrap_or(observed.lookup))
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|mut lookup| {
            lookup.work = accounting.spent();
            lookup
        })
    }
}
