use super::*;
use crate::task_volume::ChildFile;
use serde::Deserialize;

/// Envelope carried as the text of an ordinary parent `send` message.
/// Its sender comes from the immutable inbox file's authenticated requester.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParentDecision {
    approval_decision: FileValue,
}

impl LocalHarness {
    pub(super) async fn service_parent_approval(
        &self,
        task: TaskId,
        parent: TaskId,
        request: &FileValue,
        inbox: Option<&(Arc<crate::task_volume::TaskFiles<LocalStream>>, Component)>,
    ) -> Result<bool> {
        let (files, reference) = inbox.ok_or_else(|| {
            Error::Unauthorized("parent approval requires the retained child inbox".into())
        })?;
        let mirror = FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::ApprovalRequest(request_id(request)?.clone()),
            },
            request.bytes.clone(),
        );
        crate::task_volume::publish_durable(&files.inner, &mirror, None).await?;
        let attention_key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::ApprovalAttention,
        };
        let attention = serde_json::json!({"task":task,"request":request_id(request)?,
            "request_sha256":fingerprint(request)?,"expected_version":1,
            "subject":serde_json::from_slice::<serde_json::Value>(&request.bytes).map_err(storage)?});
        crate::task_volume::publish_durable(
            &files.inner,
            &FileValue::new(
                attention_key.clone(),
                serde_json::to_vec(&attention).map_err(storage)?,
            ),
            None,
        )
        .await?;
        let snapshot = files
            .snapshot(&Query::Child(reference.clone()), self.limits.attachments)
            .await?;
        for message in snapshot.files {
            if !matches!(
                message.key,
                FileKey::Child {
                    file: ChildFile::Inbox(_),
                    ..
                }
            ) {
                continue;
            }
            let Ok(envelope) = serde_json::from_slice::<ParentDecision>(&message.bytes) else {
                continue;
            };
            if message.requester != Some(parent) {
                return Err(Error::Unauthorized(
                    "approval inbox sender is not the retained parent".into(),
                ));
            }
            let FileKey::Task {
                task: target,
                file:
                    TaskFile::ApprovalDecision {
                        request: target_request,
                        ..
                    },
            } = &envelope.approval_decision.key
            else {
                return Err(Error::Unauthorized(
                    "parent submitted another file kind".into(),
                ));
            };
            if target.as_str() != task.to_string() || target_request != request_id(request)? {
                continue;
            }
            self.admit_approval_file(
                task,
                &envelope.approval_decision,
                &format!("parent:{parent}"),
            )
            .await?;
            crate::task_volume::publish_durable(
                &files.inner,
                &FileValue::new(attention_key, b"null".to_vec()),
                None,
            )
            .await?;
            return Ok(true);
        }
        Ok(false)
    }
}
