//! Ordinary approver services consume owner-published files outside effect execution.
use super::*;
use crate::{
    approvals::{ApprovalReviewFile, Approver, RuleVerdict},
    interaction::{ApprovalDecisionFile, Interaction},
    runtime::{TaskStateProvider, ToolPolicy},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile},
};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::collections::BTreeMap;
mod llm;
mod owner;
mod parent;
pub(super) use owner::bind_owner;

pub(in crate::local) struct Inbox {
    pub files: Arc<crate::task_volume::TaskFiles<LocalStream>>,
    pub reference: Component,
    pub parent_done: acyclic_fs::CancellationToken,
}

#[derive(Clone, Debug)]
enum Stage {
    Automatic {
        principal: String,
        verdict: RuleVerdict,
        reason: Option<String>,
    },
    Human(String),
    Parent(TaskId),
}

fn request_id(file: &FileValue) -> Result<&Component> {
    match &file.key {
        FileKey::Task {
            file: TaskFile::ApprovalRequest(request),
            ..
        } => Ok(request),
        _ => Err(Error::Invalid("not an approval request".into())),
    }
}
fn review_key(request: &FileValue, principal: &str) -> Result<FileKey> {
    let FileKey::Task { task, .. } = &request.key else {
        return Err(Error::Invalid("not a task file".into()));
    };
    Ok(FileKey::Task {
        task: task.clone(),
        file: TaskFile::ApprovalReview {
            request: request_id(request)?.clone(),
            principal: Component::new(ApprovalDecisionFile::principal_file(principal))?,
        },
    })
}
fn fingerprint(request: &FileValue) -> Result<String> {
    let descriptor = crate::conversation::FileDescriptor::from_bytes(
        &request.bytes,
        "application/json",
        &mut None,
    )?;
    Ok(ApprovalDecisionFile::new("binding", descriptor.sha256(), 1, false, None)?.request_sha256)
}
fn pending(files: &BTreeMap<FileKey, FileValue>) -> Vec<&FileValue> {
    files
        .values()
        .filter(|file| {
            let FileKey::Task {
                file: TaskFile::ApprovalRequest(request),
                ..
            } = &file.key
            else {
                return false;
            };
            !files.keys().any(|key| {
                matches!(key, FileKey::Task {
            file: TaskFile::ApprovalDecision { request: decided, .. }, ..
        } if decided == request)
            })
        })
        .collect()
}

fn retained_review(
    file: Option<&FileValue>,
    expected: &ApprovalReviewFile,
) -> Result<Option<ApprovalReviewFile>> {
    file.map(|file| {
        let review: ApprovalReviewFile = serde_json::from_slice(&file.bytes).map_err(storage)?;
        if review.principal != expected.principal
            || review.request_sha256 != expected.request_sha256
            || review.policy_digest != expected.policy_digest
            || review.stage != expected.stage
        {
            return Err(Error::Conflict("approval review binding changed".into()));
        }
        Ok(review)
    })
    .transpose()
}

async fn publish_review(
    files: &crate::task_volume::TaskFiles<LocalStream>,
    key: FileKey,
    review: &ApprovalReviewFile,
) -> Result<()> {
    crate::task_volume::publish_durable(
        &files.inner,
        &FileValue::new(key, serde_json::to_vec(review).map_err(storage)?),
        None,
    )
    .await?;
    Ok(())
}

impl LocalHarness {
    /// Whether a suspended task has reached an external approver. Automatic
    /// stages must finish before a headless client can safely retire its driver.
    pub async fn awaiting_external_approval(&self, task: TaskId) -> Result<bool> {
        let suspension = match self
            .runtime
            .task_host()
            .workflow_suspension(task, &mut None)
            .await
        {
            Ok(suspension) => suspension,
            Err(Error::NotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        if suspension.is_none() {
            return Ok(false);
        }
        let task_files = self.runtime.approval_files(task).await?;
        let snapshot = task_files
            .snapshot(
                &Query::Approvals(Component::new(task.to_string())?),
                self.limits.attachments,
            )
            .await?;
        let files: BTreeMap<_, _> = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        for request in pending(&files) {
            let principal = match self
                .approval_stage(task, request, &task_files, &files, false)
                .await?
            {
                Some(Stage::Human(principal)) => principal,
                Some(Stage::Parent(parent)) => format!("parent:{parent}"),
                _ => continue,
            };
            // The waiting review is published after execution has suspended.
            if files.contains_key(&review_key(request, &principal)?) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn approval_digest(&self) -> Result<[u8; 32]> {
        let identity = self.approval_policy.bind()?.identity();
        if self.runtime.approval_policy_identity().as_ref() != Some(&identity) {
            return Err(Error::Conflict(
                "local approval policy differs from its runtime".into(),
            ));
        }
        Ok(identity.digest)
    }

    async fn approval_stage(
        &self,
        task: TaskId,
        request: &FileValue,
        task_files: &crate::task_volume::TaskFiles<LocalStream>,
        files: &BTreeMap<FileKey, FileValue>,
        evaluate: bool,
    ) -> Result<Option<Stage>> {
        let digest = self.approval_digest()?;
        let hash = fingerprint(request)?;
        let admission = self.runtime.task_host().observe_admission(task).await?;
        let parent = admission.parent;
        for (stage, approver) in self.approval_policy.approvers.iter().enumerate() {
            let principal = match approver {
                Approver::Rules { principal, .. }
                | Approver::Llm { principal, .. }
                | Approver::Human { principal } => principal.clone(),
                Approver::Parent => match parent {
                    Some(parent) => format!("parent:{parent}"),
                    None => continue,
                },
            };
            let key = review_key(request, &principal)?;
            let expected = ApprovalReviewFile {
                principal: principal.clone(),
                request_sha256: hash.clone(),
                policy_digest: digest,
                stage,
                verdict: RuleVerdict::Ask,
                reason: None,
            };
            let review = retained_review(files.get(&key), &expected)?;
            if matches!(approver, Approver::Human { .. } | Approver::Parent) {
                if evaluate && review.is_none() {
                    let waiting = ApprovalReviewFile {
                        reason: Some("awaiting authenticated decision".into()),
                        ..expected
                    };
                    publish_review(task_files, key, &waiting).await?;
                }
                return Ok(Some(match approver {
                    Approver::Human { .. } => Stage::Human(principal),
                    _ => {
                        Stage::Parent(parent.ok_or_else(|| {
                            Error::Unauthorized("parent admission is absent".into())
                        })?)
                    }
                }));
            }
            let review = if let Some(review) = review {
                review
            } else {
                if !evaluate {
                    return Ok(None);
                }
                self.automatic_review(task_files, request, approver, expected, admission.limits)
                    .await?
            };
            if review.verdict != RuleVerdict::Ask {
                return Ok(Some(Stage::Automatic {
                    principal,
                    verdict: review.verdict,
                    reason: review.reason,
                }));
            }
        }
        if evaluate {
            let review = ApprovalReviewFile {
                principal: "policy:exhausted".into(),
                request_sha256: hash,
                policy_digest: digest,
                stage: self.approval_policy.approvers.len(),
                verdict: RuleVerdict::Deny,
                reason: Some("approval chain exhausted without permission".into()),
            };
            publish_review(task_files, review_key(request, &review.principal)?, &review).await?;
        }
        Ok(Some(Stage::Automatic {
            principal: "policy:exhausted".into(),
            verdict: RuleVerdict::Deny,
            reason: Some("approval chain exhausted without permission".into()),
        }))
    }

    async fn automatic_review(
        &self,
        task_files: &crate::task_volume::TaskFiles<LocalStream>,
        request: &FileValue,
        approver: &Approver,
        expected: ApprovalReviewFile,
        limits: Limits,
    ) -> Result<ApprovalReviewFile> {
        let (verdict, reason) = match approver {
            Approver::Rules {
                rules, fallback, ..
            } => {
                let interaction: Interaction =
                    serde_json::from_slice(&request.bytes).map_err(storage)?;
                let verdict = match &interaction {
                    Interaction::Approval { subject, .. } => {
                        crate::approvals::ApprovalRule::evaluate_subject(rules, *fallback, subject)
                    }
                    _ => RuleVerdict::Ask,
                };
                (verdict, Some("configured approval rules".into()))
            }
            Approver::Llm { model, prompt, .. } => {
                self.review_model(
                    task_files,
                    request,
                    &expected.principal,
                    model,
                    prompt,
                    limits,
                )
                .await?
            }
            Approver::Human { .. } | Approver::Parent => {
                return Err(Error::Invalid(
                    "external approver requires a signed decision".into(),
                ));
            }
        };
        let review = ApprovalReviewFile {
            verdict,
            reason,
            ..expected
        };
        publish_review(task_files, review_key(request, &review.principal)?, &review).await?;
        Ok(review)
    }

    async fn settle_approval(
        &self,
        task: TaskId,
        request: &FileValue,
        principal: &str,
        approved: bool,
        reason: Option<String>,
    ) -> Result<()> {
        let descriptor = crate::conversation::FileDescriptor::from_bytes(
            &request.bytes,
            "application/json",
            &mut None,
        )?;
        let decision =
            ApprovalDecisionFile::new(principal, descriptor.sha256(), 1, approved, reason)?;
        let FileKey::Task {
            task: component, ..
        } = &request.key
        else {
            return Err(Error::Invalid("not a task file".into()));
        };
        let file = FileValue::new(
            FileKey::Task {
                task: component.clone(),
                file: TaskFile::ApprovalDecision {
                    request: request_id(request)?.clone(),
                    principal: Component::new(ApprovalDecisionFile::principal_file(principal))?,
                },
            },
            serde_json::to_vec(&decision).map_err(storage)?,
        );
        self.admit_approval_file(task, &file, principal).await
    }

    async fn admit_approval_file(
        &self,
        task: TaskId,
        file: &FileValue,
        principal: &str,
    ) -> Result<()> {
        let decision = ApprovalDecisionFile::decode(&file.bytes, self.limits.file_bytes)?;
        if decision.principal != principal {
            return Err(Error::Unauthorized(
                "decision principal differs from the active approver".into(),
            ));
        }
        self.runtime.submit_reviewed_approval(task, file).await
    }

    /// Authenticated human connection submits a decision file. File contents
    /// cannot select a rule, model or parent principal.
    pub async fn submit_approval_decision(&self, file: &FileValue) -> Result<()> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, .. },
        } = &file.key
        else {
            return Err(Error::Unauthorized(
                "only an approval decision can be submitted".into(),
            ));
        };
        let task = TaskId::from_bytes(
            uuid::Uuid::parse_str(task.as_str())
                .map_err(storage)?
                .into_bytes(),
        );
        let task_files = self.runtime.approval_files(task).await?;
        let observed = task_files
            .snapshot(
                &Query::Approvals(Component::new(task.to_string())?),
                self.limits.attachments,
            )
            .await?;
        let files: BTreeMap<_, _> = observed
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let key = FileKey::Task {
            task: Component::new(task.to_string())?,
            file: TaskFile::ApprovalRequest(request.clone()),
        };
        let request = files
            .get(&key)
            .ok_or_else(|| Error::NotFound("approval request is absent".into()))?;
        let Some(Stage::Human(principal)) = self
            .approval_stage(task, request, &task_files, &files, false)
            .await?
        else {
            return Err(Error::Unauthorized(
                "human is not the current approval stage".into(),
            ));
        };
        self.admit_approval_file(task, file, &principal).await
    }

    async fn service_pending_approvals(
        &self,
        task: TaskId,
        task_files: &crate::task_volume::TaskFiles<LocalStream>,
        files: &BTreeMap<FileKey, FileValue>,
        inbox: Option<&Inbox>,
    ) -> Result<(bool, bool)> {
        let mut settled = false;
        let mut parent_waiting = false;
        for request in pending(files) {
            match self
                .approval_stage(task, request, task_files, files, true)
                .await?
            {
                Some(Stage::Automatic {
                    principal,
                    verdict,
                    reason,
                }) => {
                    self.settle_approval(
                        task,
                        request,
                        &principal,
                        verdict == RuleVerdict::Allow,
                        reason,
                    )
                    .await?;
                    settled = true;
                }
                Some(Stage::Human(_)) => {}
                Some(Stage::Parent(parent)) => {
                    // Parent messages are routed by the child controller's
                    // authenticated file inbox, never by the human client.
                    let decided = self
                        .service_parent_approval(task, parent, request, inbox)
                        .await?;
                    settled |= decided;
                    parent_waiting |= !decided;
                }
                None => {
                    return Err(Error::Conflict(
                        "automatic approval stage was not evaluated".into(),
                    ));
                }
            }
        }
        Ok((settled, parent_waiting))
    }

    pub(super) fn service_approvals<'a>(
        &'a self,
        task: TaskId,
        commands: &'a dyn crate::filesystem::TaskCommandHost,
        inbox: Option<&'a Inbox>,
    ) -> BoxProviderFuture<'a, Result<bool>> {
        Box::pin(async move {
            let task_files = self.runtime.approval_files(task).await?;
            let query = Query::Approvals(Component::new(task.to_string())?);
            loop {
                let (cancelled, mut coordinator_changes) =
                    self.runtime.task_host().cancellation_follow(task).await?;
                if cancelled {
                    return Ok(false);
                }
                let mut inbox_changes = if let Some(Inbox {
                    files, reference, ..
                }) = inbox
                {
                    #[cfg(feature = "native-execution")]
                    if super::children::commands::state(
                        files,
                        reference,
                        usize::try_from(self.limits.render_bytes).map_err(storage)?,
                    )
                    .await?
                        != crate::task_volume::ChildState::Running
                    {
                        return Ok(false);
                    }
                    Some(
                        files
                            .observe(
                                Query::Child(reference.clone()),
                                None,
                                self.limits.attachments,
                            )
                            .await?
                            .changes,
                    )
                } else {
                    None
                };
                let observed = task_files
                    .observe(query.clone(), None, self.limits.attachments)
                    .await?;
                let mut changes = observed.changes;
                let files: BTreeMap<_, _> = observed
                    .snapshot
                    .ok_or_else(|| Error::Storage("approval snapshot absent".into()))?
                    .files
                    .into_iter()
                    .map(|file| (file.key.clone(), file))
                    .collect();
                let pending = pending(&files);
                if pending.is_empty() {
                    return Ok(false);
                }
                let (settled, parent_waiting) = tokio::select! {
                    result = self.service_pending_approvals(task, &task_files, &files, inbox) => result?,
                    result = self.approval_service_stopped(task, commands, &mut coordinator_changes) => {
                        result?;
                        return Ok(false);
                    }
                };
                if settled {
                    return Ok(true);
                }
                let cancelled = async {
                    match commands.invocation_cancellation() {
                        Some(token) => token.cancelled().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    () = async {
                        match inbox {
                            Some(inbox) => inbox.parent_done.cancelled().await,
                            None => std::future::pending().await,
                        }
                    }, if parent_waiting => {}
                    result = coordinator_changes.next() => { result.ok_or_else(|| Error::Storage("coordinator follow ended".into()))??; }
                    result = changes.next() => { result.ok_or_else(|| Error::Storage("approval follow ended".into()))??; }
                    result = async {
                        match &mut inbox_changes { Some(changes) => changes.next().await,
                            None => std::future::pending().await }
                    } => { result.ok_or_else(|| Error::Storage("parent inbox follow ended".into()))??; }
                    () = cancelled => return Ok(false),
                }
            }
        })
    }

    async fn approval_service_stopped(
        &self,
        task: TaskId,
        commands: &dyn crate::filesystem::TaskCommandHost,
        changes: &mut acyclic_stream::BoxProviderStream<'static, Result<()>>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                result = changes.next() => {
                    result.ok_or_else(|| Error::Storage("coordinator follow ended".into()))??;
                    let (stopped, next) = self.runtime.task_host().cancellation_follow(task).await?;
                    if stopped { return Ok(()); }
                    *changes = next;
                }
                () = async {
                    match commands.invocation_cancellation() {
                        Some(token) => token.cancelled().await,
                        None => std::future::pending().await,
                    }
                } => return Ok(()),
            }
        }
    }
}
