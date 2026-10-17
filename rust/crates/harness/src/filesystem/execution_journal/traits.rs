use super::*;

impl<P, A, O> ToolApprovalVerifier for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_tool_approval_with_io(id, operation, digest, None))
    }
}

impl<P, A, O> ExecutionJournal for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn recover_model_files<'a>(
        &'a self,
        operation: OperationId,
        step: u32,
        purpose: crate::executor::ModelPurpose,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if purpose == crate::executor::ModelPurpose::Response {
                self.recover_published_model_delta(operation, step, io)
                    .await?;
            }
            Ok(())
        })
    }

    fn publish_model_outcome<'a>(
        &'a self,
        operation: OperationId,
        step: u32,
        outcome: std::result::Result<&'a serde_json::Value, (usize, &'a Error)>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.publish_model_outcome_file(operation, step, outcome, io)
                .await
        })
    }

    fn stage_model_event<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        turn: (u32, crate::executor::ModelPurpose, usize),
        event: &'a ModelEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.require_operation(operation, io)?;
            if self.task_files.is_some()
                && turn.1 == crate::executor::ModelPurpose::Response
                && matches!(
                    event,
                    ModelEvent::Content { .. } | ModelEvent::Reasoning { .. }
                )
            {
                self.stage_model_delta(operation, turn.0, turn.2, event, io)
                    .await
            } else {
                let reference =
                    crate::executor::stage_event_json(self, operation, key, event, io).await?;
                if self.task_files.is_some()
                    && turn.1 == crate::executor::ModelPurpose::Response
                    && let ModelEvent::ToolCall(call) = event
                {
                    self.publish_tool_arguments(operation, turn.0, turn.2, call, io)
                        .await?;
                }
                Ok(reference)
            }
        })
    }

    fn model_file_turn<'a>(
        &'a self,
        operation: OperationId,
        step: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, u64>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let Some(files) = &self.task_files else {
                return Ok(u64::from(step));
            };
            self.require_operation(operation, io)?;
            let (owner, _) = self.owner.as_ref().ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "model turn has no task owner",
                )
            })?;
            let task = crate::task_volume::Component::new(owner.task_id().to_string())?;
            files
                .inner
                .published_turn(&task, Self::model_publication(operation, step, io)?)
                .await?
                .ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "model turn has no request file",
                    )
                })
        })
    }

    fn canonical_authority(&self) -> Option<&crate::core::Authority> {
        Some(self.verifier.audience())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining().map_err(Error::from)?;
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn staged_ref_with_native_work<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        work.remaining().map_err(Error::from)?;
        let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "original staged-payload observation requires a task-owned journal",
        })?;
        owner.require_volume_grant(
            &self.volume,
            VolumeOperation::Read,
            &mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)?,
            ),
        )?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = {
                owner
                    .verify_read_access(&mut Some(
                        &mut crate::filesystem::PreparationIo::from_native_work(work)
                            .map(crate::store::HistoryWork::new)?,
                    ))
                    .await?;
                let file = self
                    .staged_ref_with_io(operation_id, idempotency_key, Some(&io))
                    .await?;
                owner
                    .verify_read_access(&mut Some(
                        &mut crate::filesystem::PreparationIo::from_native_work(work)
                            .map(crate::store::HistoryWork::new)?,
                    ))
                    .await?;
                file
            };
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if io.is_none() {
                let span = obs_span!(
                    "acyclic.harness.journal.replay",
                    rev = after,
                    items = crate::obs::Empty
                );
                return traced(
                    span,
                    Box::pin(async move {
                        let records = self.records(operation, after, maximum, io).await?;
                        crate::obs::obs_record!("items" = records.len());
                        Ok(records)
                    }),
                )
                .await;
            }
            let records = self.records(operation, after, maximum, io).await?;
            io.ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "bounded branch",
                )
            })?
            .remaining()?;
            Ok(records)
        })
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if io.is_none() {
                let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
                return traced(
                    span,
                    Box::pin(async move {
                        let bytes = self.load_content(reference, io).await?;
                        crate::obs::obs_record!("bytes" = bytes.len());
                        Ok(bytes)
                    }),
                )
                .await;
            }
            self.load_content(reference, io).await
        })
    }

    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            return crate::conversation::BoundedContentFuture::new(io, || async move {
                let local = PreparationIo::from_native_work(work)?;
                let result = self
                    .append_validated(operation, None, key, event, Some(&local))?
                    .await;
                let retained = local.owned_bytes()?;
                drop(local.into_native_custody(retained)?);
                result.map(|_| ())
            });
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let span = obs_span!("acyclic.harness.journal.append");
            traced(
                span,
                self.append_validated(operation, None, key, event, io)?,
            )
            .await
            .map(|_| ())
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        expected_tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            return crate::conversation::BoundedContentFuture::new(io, || async move {
                let local = PreparationIo::from_native_work(work)?;
                let result = self
                    .append_validated(operation, Some(expected_tail), key, event, Some(&local))?
                    .await;
                let retained = local.owned_bytes()?;
                drop(local.into_native_custody(retained)?);
                result
            });
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let span = obs_span!(
                "acyclic.harness.journal.append_if_tail",
                rev = expected_tail
            );
            traced(
                span,
                self.append_validated(operation, Some(expected_tail), key, event, io)?,
            )
            .await
        })
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = match io.and_then(PreparationIo::original_native_work) {
            Some(work) => {
                work.remaining()?;
                let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native staging requires an original dispatch owner",
                })?;
                owner.require_volume_grant(
                    &self.volume,
                    VolumeOperation::Write,
                    &mut Some(
                        &mut io
                            .ok_or_else(|| {
                                crate::executor::stock_boundary(
                                    io,
                                    crate::wire::ErrorCode::Invalid,
                                    "native ledger",
                                )
                            })?
                            .history_io(),
                    ),
                )?;
                Some((owner, work))
            }
            None => None,
        };
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some((owner, work)) = native {
                let local = PreparationIo::from_native_work(work)?;
                owner
                    .verify(true, &mut Some(&mut local.history_io()))
                    .await?;
                let file = self
                    .stage_with_io(operation, key, bytes, media, Some(&local))
                    .await?;
                owner
                    .verify(true, &mut Some(&mut local.history_io()))
                    .await?;
                let retained = file.logical_owned_bytes()?;
                io.ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "native ledger",
                    )
                })?
                .absorb_native_custody(local.into_native_custody(retained)?)?;
                return Ok(file);
            }
            let span = obs_span!("acyclic.harness.journal.stage", bytes = bytes.len());
            traced(
                span,
                Box::pin(self.stage_with_io(operation, key, bytes, media, io)),
            )
            .await
        })
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_input_file_with_io(reference, io)
        })
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        context: Option<&'a crate::runtime::TaskContext>,
        summary_files: &'a [&'a FileRef],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_model_content_with_io::<true>(content, io, context, summary_files)
        })
    }

    fn authenticate_summary<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.authenticate_summary_with_io(file, io)
        })
    }

    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a SelectedModelContext,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_selected_context_with_io(operation, selected, io)
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let temporary = PreparationIo::temporary(io)?;
            let active = temporary.as_ref().or(io);
            self.open_interaction_with_io(id, interaction, active).await
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let temporary = PreparationIo::temporary(io)?;
            let active = temporary.as_ref().or(io);
            let outcome = self.interaction_outcome_with_io(id, active).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(temporary) = temporary {
                let retained = temporary.owned_bytes()?;
                io.ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "native parent IO",
                    )
                })?
                .absorb_native_custody(temporary.into_native_custody(retained)?)?;
            }
            Ok(outcome)
        })
    }
}
