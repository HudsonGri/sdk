use super::*;
use proptest::prelude::*;

fn request() -> Result<FileValue> {
    Ok(FileValue::new(
        FileKey::Task {
            task: Component::new("task")?,
            file: TaskFile::ApprovalRequest(Component::new("request")?),
        },
        br#"{"tool":"write","arguments":{"path":"approved"}}"#.to_vec(),
    ))
}

fn rules(principal: &str) -> Approver {
    Approver::Rules {
        principal: principal.into(),
        rules: vec![],
        fallback: RuleVerdict::Ask,
    }
}

fn retain(
    files: &mut BTreeMap<FileKey, FileValue>,
    request: &FileValue,
    review: &ApprovalReviewFile,
) -> Result<()> {
    let key = review_key(request, &review.principal)?;
    let bytes = serde_json::to_vec(review).map_err(|error| Error::Storage(error.to_string()))?;
    files.insert(key.clone(), FileValue::new(key, bytes));
    Ok(())
}

#[test]
fn shared_chain_escalates_in_order_and_never_trusts_human_review_files() -> Result<()> {
    let policy = ApprovalPolicy {
        approvers: vec![rules("rules"), Approver::Llm { principal: "model".into(), model: "reviewer".into(), prompt: "Review the exact request".into() }, Approver::Human { principal: "human".into() }],
    };
    let digest = policy.bind()?.identity().digest;
    let request = request()?;
    let mut files = BTreeMap::new();
    for stage in 0..3 {
        let NextStage::Review { mut expected, .. } = next_stage(&policy, digest, None, &request, &files, 0)? else {
            panic!("chain decided before its active approver");
        };
        assert_eq!(expected.stage, stage);
        expected.verdict = if stage == 2 { RuleVerdict::Allow } else { RuleVerdict::Ask };
        retain(&mut files, &request, &expected)?;
    }
    assert!(matches!(next_stage(&policy, digest, None, &request, &files, 0)?, NextStage::Review {
        approver: Approver::Human { .. }, retained: Some(ApprovalReviewFile { verdict: RuleVerdict::Allow, .. }), ..
    }), "a review file cannot replace an authenticated human decision");
    let NextStage::Decided(exhausted) = next_stage(&policy, digest, None, &request, &files, 3)? else {
        panic!("exhausted chain did not decide");
    };
    assert_eq!(exhausted.verdict, RuleVerdict::Deny);
    Ok(())
}

#[test]
fn shared_chain_binds_parent_to_the_admitted_parent_task() -> Result<()> {
    let policy = ApprovalPolicy { approvers: vec![Approver::Parent] };
    let digest = policy.bind()?.identity().digest;
    let request = request()?;
    let files = BTreeMap::new();
    let parent = TaskId::new();
    let NextStage::Review { expected, .. } = next_stage(&policy, digest, Some(parent), &request, &files, 0)? else {
        panic!("admitted parent did not receive its review");
    };
    assert_eq!(expected.principal, format!("parent:{parent}"));
    assert!(matches!(next_stage(&policy, digest, None, &request, &files, 0)?, NextStage::Decided(ApprovalReviewFile { verdict: RuleVerdict::Deny, .. })));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn shared_chain_rejects_every_changed_review_binding(field in 0_u8..4) {
        let policy = ApprovalPolicy { approvers: vec![rules("rules")] };
        let digest = policy.bind()?.identity().digest;
        let request = request()?;
        let mut files = BTreeMap::new();
        let NextStage::Review { mut expected, .. } = next_stage(&policy, digest, None, &request, &files, 0)? else {
            prop_assert!(false, "missing first review");
            return Ok(());
        };
        expected.verdict = RuleVerdict::Allow;
        retain(&mut files, &request, &expected)?;
        prop_assert!(matches!(next_stage(&policy, digest, None, &request, &files, 0)?, NextStage::Decided(_)));
        let key = review_key(&request, &expected.principal)?;
        match field {
            0 => expected.principal.push('x'),
            1 => expected.request_sha256.push('0'),
            2 => expected.policy_digest = [255; 32],
            _ => expected.stage += 1,
        }
        let bytes = serde_json::to_vec(&expected)?;
        files.insert(key.clone(), FileValue::new(key, bytes));
        prop_assert!(matches!(next_stage(&policy, digest, None, &request, &files, 0), Err(Error::Conflict(_))));
    }
}
