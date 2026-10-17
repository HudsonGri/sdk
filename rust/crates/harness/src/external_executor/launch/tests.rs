use super::*;
use proptest::prelude::*;
use serde_json::json;

fn request(label: &str, prompt: &str, id: uuid::Uuid, literal: &str) -> ExternalRequest {
    super::super::request(
        ExternalHost::Pi,
        "tool_call",
        json!({
            "sessionId":"native-parent", "toolCallId":"spawn", "toolName":"acyclic_spawn",
            "input":{"id":label,"prompt":prompt,"executor":{
                "session":{"host":"pi","session_id":id.to_string(),"caller":null},
                "executable":std::env::current_exe().unwrap(),
                "arguments":[{"kind":"text","value":literal},{"kind":"session_file"},
                    {"kind":"session_id"},{"kind":"prompt"}],
                "session_bytes":json!({"type":"session","version":3,"id":id,
                    "timestamp":"2026-10-17T00:00:00Z","cwd":"/project"}).to_string(),
                "timeout_ms":1000
            }}
        }),
    )
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // The retained declaration preserves literal argv and native session identity.
    // A swapped SDK header cannot redirect the admitted child.
    #[test]
    fn declarations_round_trip_without_interpolation_or_identity_substitution(
        label in "[a-z]{1,20}", prompt in "[^\\x00]{1,80}",
        literal in "[^\\x00]{0,80}", id in any::<[u8;16]>(), parent in any::<[u8;16]>(),
    ) {
        let id=uuid::Uuid::from_bytes(id);
        let request=request(&label,&prompt,id,&literal);
        let spawn=Spawn::from_request(&request)?;
        let launch=Launch {parent:TaskId::from_bytes(parent),request};
        let bytes=volume::encode(&launch).unwrap();
        prop_assert_eq!(Launch::decode(&bytes)?,(launch,spawn));
        let (launch,spawn)=Launch::decode(&bytes)?;
        let path=std::env::temp_dir().join("literal session.jsonl");
        prop_assert_eq!(spawn.arguments(&path)?,vec![literal,path.to_str().unwrap().into(),id.to_string(),prompt]);
        let fork=spawn.fork(&launch.request);
        prop_assert_eq!(spawn.attachment(&launch.request,"/project")?.task_id(),fork.child_attachment("/project")?.task_id());
        let mut changed=launch.request;
        let ExternalAction::RunTool {arguments,..}=&mut changed.action else {unreachable!()};
        arguments["executor"]["session"]["session_id"]=json!(uuid::Uuid::from_bytes(id.into_bytes().map(|b|b^1)));
        prop_assert!(Spawn::from_request(&changed).is_err());
        arguments_insert_forbidden(&mut changed);
        prop_assert!(Spawn::from_request(&changed).is_err());
    }
}

fn arguments_insert_forbidden(request: &mut ExternalRequest) {
    let ExternalAction::RunTool { arguments, .. } = &mut request.action else {
        unreachable!()
    };
    arguments["executor"]["environment"] = json!({"credential":"never serialized"});
}

#[test]
fn native_decoder_rejects_unknown_fields_and_duplicate_headers() {
    let mut request = request("child", "prompt", uuid::Uuid::nil(), "$(literal)");
    assert!(Spawn::from_request(&request).is_ok());
    arguments_insert_forbidden(&mut request);
    assert!(Spawn::from_request(&request).is_err());
    let mut request = self::request("child", "prompt", uuid::Uuid::nil(), "literal");
    let ExternalAction::RunTool { arguments, .. } = &mut request.action else {
        unreachable!()
    };
    arguments["executor"]["session_bytes"] = json!(
        r#"{"type":"session","type":"session","version":3,"id":"00000000-0000-0000-0000-000000000000","timestamp":"now","cwd":"/project"}"#
    );
    assert!(Spawn::from_request(&request).is_err());
}
