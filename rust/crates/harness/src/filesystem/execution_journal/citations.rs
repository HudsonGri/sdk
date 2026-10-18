//! One immutable citation sidecar after a response completes.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn publish_citations<'a>(
        &'a self,
        step: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let (Some(context), Some(files), Some((owner, operation))) =
                (&self.input_context, &self.task_files, &self.owner)
            else {
                return Ok(());
            };
            owner.require_task_context_binding(context, io)?;
            let key = FileKey::Task {
                task: Component::new(owner.task_id().to_string())?,
                file: TaskFile::Citations(u64::from(step)),
            };
            // An accepted sidecar is the result, including on replay after project edits.
            if !files
                .inner
                .snapshot(&Query::File(key.clone()), 1)
                .await?
                .files
                .is_empty()
            {
                return Ok(());
            }
            let local;
            let io = match io {
                Some(io) => io,
                None => {
                    local = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &local
                }
            };
            let before = io.owned_bytes()?;
            let result = async {
            let mut source = String::new();
            let mut after = 0;
            let mut through = None;
            let mut output_offset = 0u64;
            loop {
                let before_page = io.owned_bytes()?;
                let page = self.replay_page(*operation, after, EXECUTION_REPLAY_PAGE_RECORDS, true, Some(io)).await?;
                let page_hold = io.guard_owned(io.owned_since(before_page)?)?;
                if page.is_empty() { break; }
                let mut completed = false;
                for (record, model) in &page {
                    after = record.sequence;
                    if matches!(record.event, ExecutionEvent::Model { step: found, purpose: crate::executor::ModelPurpose::Response, .. } if found < step)
                        && let Some(ModelEvent::Content { delta }) = model
                    {
                        output_offset = output_offset.checked_add(delta.len() as u64).ok_or_else(|| io.invalid("citation output offset overflows"))?;
                    }
                    if matches!(record.event, ExecutionEvent::Model { step: found, purpose: crate::executor::ModelPurpose::Response, .. } if found == step) {
                        if let Some(ModelEvent::Content { delta }) = model {
                            crate::executor::stock_string_push(&mut source, delta, self.maximum_payload_bytes, "citation message exceeds its bound", Some(io))?;
                            through = record.idempotency_key.rsplit(':').next().and_then(|part| part.parse().ok());
                        }
                        if matches!(model, Some(ModelEvent::Completed { .. })) { completed = true; break; }
                    }
                }
                drop(page);
                drop(page_hold);
                if completed { break; }
            }
            let mut citations = super::super::file_citations::resolve(context, &source, io).await?;
            if citations.entries.is_empty() && citations.error.is_none() { return Ok(()); }
            citations.through_sequence = through;
            citations.output_offset = output_offset;
            let encoded_length = crate::executor::with_history_io(Some(io), |work| crate::executor::json::json_byte_length(&citations, u64::MAX, work))?;
            if encoded_length > self.maximum_payload_bytes {
                citations = crate::file_citations::FileCitations {
                    source_bytes: source.len() as u64,
                    output_offset,
                    through_sequence: through,
                    error: Some(acyclic_markdown::directive::Resolution::Error(acyclic_markdown::directive::ResolutionError::TooLarge)),
                    ..Default::default()
                };
            }
            let bytes = io.encode(&citations, self.maximum_payload_bytes, false)?;
            let value = FileValue { requester: None, publication: None, key, channel: None, bytes };
            loop {
                let mut observer = io.history_io();
                let condition = owner.condition(crate::distributed::JournalWrite::Fresh, &mut Some(&mut observer)).await?;
                let head = files.inner.head().await?;
                match files.inner.create_if(head, &value, vec![condition], owner.admission().run_limits.deadline_epoch_ms, io.original_native_work()).await {
                    Ok(_) => break,
                    // The first fenced publication captures the project cut.
                    // A duplicate completion must retain that immutable winner.
                    Err(crate::task_volume::Error::ImmutableConflict(key)) if key == value.key => break,
                    Err(crate::task_volume::Error::Conflict(_)) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        }.await;
            PreparationIo::release_since(Some(io), Some(before))?;
            result
        })
    }
}
