//! All clients submit approval decisions to the same exact-intent files.
use super::*;
impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(super) async fn submit_approval(&mut self, input: &str) -> Result<Option<OperationId>> {
        // Presentation and cancellation commands remain usable while a tool
        // awaits a decision; only ordinary input is an approval response.
        if input.trim_start().starts_with('/') {
            return Ok(None);
        }
        let Some(task) = self.active_task().or(self.selected_task()?) else {
            return Ok(None);
        };
        let snapshot = self
            .volume
            .snapshot(
                &files::Query::Approvals(files::Component::new(task.to_string())?),
                self.maximum_files,
            )
            .await?;
        for file in &snapshot.files {
            if !matches!(
                file.key,
                files::FileKey::Task {
                    file: files::TaskFile::ApprovalRequest(_),
                    ..
                }
            ) {
                continue;
            }
            if file.bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid(
                    "approval request exceeds client file bound".into(),
                ));
            }
            let request: files::FileApproval =
                serde_json::from_slice(&file.bytes).map_err(codec)?;
            if request.task != task || request.key()? != file.key {
                return Err(Error::Invalid(
                    "approval request path differs from its task".into(),
                ));
            }
            let mut resolved = false;
            for decision in &snapshot.files {
                resolved |= request.decision(&decision.key, &decision.bytes)?.is_some();
            }
            if resolved {
                continue;
            }
            let approved = match input.trim().to_ascii_lowercase().as_str() {
                "y" | "yes" => true,
                "n" | "no" => false,
                _ => return Err(Error::Invalid("approval requires yes or no".into())),
            };
            self.volume
                .decide_approval(&request, files::Component::new("user")?, approved)
                .await?;
            return Ok(Some(request.binding()?.operation_id));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{MemoryStream, StreamPath};

    #[tokio::test]
    async fn clients_publish_exact_decisions_without_submitting_a_prompt() -> Result<()> {
        for approved in [false, true] {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("approval-client").unwrap(),
                )
                .await?,
            );
            let mut client =
                CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 65536, 32).await?;
            let task = TaskId::new();
            client
                .write_state(
                    CommandFileRoot::Session,
                    "layout.json",
                    &json!({"selected_task":task}),
                )
                .await?;
            let operation_id = OperationId::new();
            let request = files::FileApproval {
                task,
                id: crate::InteractionId::new(),
                request: crate::interaction::Interaction::Approval {
                    prompt: "approve action".into(),
                    operation_id,
                    action_digest: [7; 32],
                },
            };
            volume
                .append(
                    volume.head().await?,
                    &files::FileValue::new(
                        request.key()?,
                        serde_json::to_vec(&request).map_err(codec)?,
                    ),
                )
                .await?;
            assert!(client.submit_approval("perhaps").await.is_err());
            assert_eq!(client.submit_approval("/quit").await?, None);
            let query = files::Query::Approvals(component(task.to_string())?);
            // Negative control: unrecognized input and presentation commands
            // cannot silently approve the pending action.
            assert_eq!(volume.snapshot(&query, 4).await?.files.len(), 1);
            assert_eq!(
                client
                    .submit_approval(if approved { "y" } else { "n" })
                    .await?,
                Some(operation_id)
            );
            let saved = volume.snapshot(&query, 4).await?;
            assert_eq!(saved.files.len(), 2);
            assert!(saved.files.iter().any(|file| request.decision(&file.key, &file.bytes).unwrap() == Some(approved)));
            assert!(
                volume
                    .decide_approval(&request, component("user")?, !approved)
                    .await
                    .is_err()
            );
            assert!(
                client.pending.is_empty(),
                "approval response became a model prompt"
            );
        }
        Ok(())
    }
}
