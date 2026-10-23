//! The shipped CLI consumes policy data and follows the real approval files.
use super::*;
use acyclic_harness::{
    interaction::ApprovalDecisionFile,
    task_volume::{Component, FileValue},
};
use std::io::{BufRead as _, BufReader};
mod recovery;

#[derive(Clone, Copy, Debug)]
enum Case {
    Default,
    RuleAllow,
    RuleDeny,
    LlmAllow,
    LlmDeny,
    HumanAllow,
    HumanDeny,
    ForgedPrincipal,
    ForgedSubject,
    Chain,
    ParentAllow,
    ParentDeny,
    ParentForged,
}
impl Case {
    fn parent(self) -> bool {
        matches!(
            self,
            Self::ParentAllow | Self::ParentDeny | Self::ParentForged
        )
    }
    fn human(self) -> bool {
        matches!(
            self,
            Self::HumanAllow
                | Self::HumanDeny
                | Self::ForgedPrincipal
                | Self::ForgedSubject
                | Self::Chain
        )
    }
    fn allowed(self) -> bool {
        matches!(
            self,
            Self::Default
                | Self::RuleAllow
                | Self::LlmAllow
                | Self::HumanAllow
                | Self::Chain
                | Self::ParentAllow
        )
    }
    fn forged(self) -> bool {
        matches!(
            self,
            Self::ForgedPrincipal | Self::ForgedSubject | Self::ParentForged
        )
    }
}
fn rule(fallback: &str) -> Value {
    json!({"kind":"rules","principal":"rules","rules":[],"fallback":fallback})
}
fn policy(case: Case, address: std::net::SocketAddr) -> Value {
    let human = json!({"kind":"human","principal":"human"});
    let llm = json!({"kind":"llm","principal":"reviewer","model":"graphcoder-qual/reviewer","prompt":"REVIEW_FIXTURE"});
    let stages = match case {
        Case::RuleAllow | Case::RuleDeny => vec![json!({"kind":"rules","principal":"rules",
            "rules":[{"tool":"url_fetch","arguments":{"url":if matches!(case,Case::RuleDeny) {
                format!("http://{address}/changed-subject")
            } else { format!("http://{address}/effect") }},"verdict":"allow"}],"fallback":"deny"})],
        Case::Default => vec![rule("allow")],
        Case::LlmAllow | Case::LlmDeny => vec![llm],
        Case::Chain => vec![rule("ask"), llm, human],
        Case::ParentAllow | Case::ParentDeny | Case::ParentForged => vec![
            json!({"kind":"rules","principal":"rules","rules":[{"tool":"url_fetch","arguments":null,"verdict":"ask"}],"fallback":"allow"}),
            json!({"kind":"parent"}),
            human,
        ],
        _ => vec![human],
    };
    json!({"approvers":stages})
}
fn response_text(text: &str) -> String {
    let event = json!({"type":"response.output_text.delta","delta":text});
    format!(
        "data: {event}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\"}}}}\n\n"
    )
}
fn call(name: &str, arguments: Value, sequence: usize) -> String {
    let event = json!({"type":"response.output_item.done","output_index":0,
        "item":{"type":"function_call","call_id":format!("approval-{sequence}"),"name":name,"arguments":arguments.to_string()}});
    format!(
        "data: {event}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\"}}}}\n\n"
    )
}
fn tool_result(body: &Value) -> Value {
    let item = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|item| item["type"] == "function_call_output")
        .unwrap();
    serde_json::from_str(item["output"].as_str().unwrap()).unwrap()
}
fn decision(request: &FileValue, principal: &str, approved: bool) -> FileValue {
    let FileKey::Task {
        task,
        file: TaskFile::ApprovalRequest(id),
    } = &request.key
    else {
        panic!("request key")
    };
    let hash = acyclic_harness::conversation::FileDescriptor::from_bytes(
        &request.bytes,
        "application/json",
        &mut None,
    )
    .unwrap();
    FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::ApprovalDecision {
                request: id.clone(),
                principal: Component::new(ApprovalDecisionFile::principal_file(principal)).unwrap(),
            },
        },
        serde_json::to_vec(
            &ApprovalDecisionFile::new(principal, hash.sha256(), 1, approved, None).unwrap(),
        )
        .unwrap(),
    )
}

fn executable() -> std::path::PathBuf {
    let compiled = Path::new(build_metadata::EXECUTABLE);
    if compiled.is_file() {
        return compiled.to_owned();
    }
    // Cross-built integration fixtures are exported beside the executable
    // identified by Cargo JSON, preserving Cargo's binary filename.
    let exported = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join(compiled.file_name().unwrap());
    assert!(
        exported.is_file(),
        "missing shipping binary: {}",
        exported.display()
    );
    exported
}

#[derive(Clone)]
struct Fixture {
    case: Case,
    address: std::net::SocketAddr,
    parent_task: acyclic_harness::TaskId,
    effects: Arc<AtomicUsize>,
    root_calls: Arc<AtomicUsize>,
    child_calls: Arc<AtomicUsize>,
    reviews: Arc<AtomicUsize>,
}
impl Fixture {
    fn review(&self, body: &Value) -> String {
        self.reviews.fetch_add(1, Ordering::SeqCst);
        assert!(
            body["tools"].as_array().is_none_or(Vec::is_empty),
            "approver inherited tools: {body}"
        );
        assert!(body.to_string().contains("REVIEW_FIXTURE"));
        let verdict = match self.case {
            Case::LlmDeny => "deny",
            Case::Chain => "ask",
            _ => "allow",
        };
        response_text(&json!({"verdict":verdict,"reason":"independent review"}).to_string())
    }
    fn model(&self, body: &Value) -> String {
        if body["model"] == "reviewer" {
            return self.review(body);
        }
        let child = body["input"].as_array().unwrap().iter().any(|item| {
            item["role"] == "user" && item.to_string().contains("CHILD_APPROVAL_FIXTURE")
        });
        let counter = if child {
            &self.child_calls
        } else {
            &self.root_calls
        };
        let step = counter.fetch_add(1, Ordering::SeqCst);
        if !child && self.case.parent() {
            return self.parent(body, step);
        }
        if step == 0 {
            call(
                "url_fetch",
                json!({"url":format!("http://{}/effect",self.address)}),
                step,
            )
        } else {
            ANSWER.to_owned()
        }
    }
    fn parent(&self, body: &Value, step: usize) -> String {
        match step {
            0 => call(
                "spawn",
                json!({"task":"CHILD_APPROVAL_FIXTURE","name":"worker","grants":null}),
                step,
            ),
            1 => {
                let result = tool_result(body);
                let reference = result["agent"].as_str().unwrap();
                call("wait", json!({"agents":[reference],"mode":"all"}), step)
            }
            2 => {
                let result = tool_result(body);
                let agent = &result["agents"][0];
                let approval = &agent["approval"];
                assert!(
                    !approval.is_null(),
                    "parent was not woken by the approval: {result}"
                );
                let principal = format!("parent:{}", self.parent_task);
                let hash = if matches!(self.case, Case::ParentForged) {
                    "11".repeat(32)
                } else {
                    approval["request_sha256"].as_str().unwrap().to_owned()
                };
                let file = FileValue::new(FileKey::Task { task:Component::new(approval["task"].as_str().unwrap()).unwrap(),
                    file:TaskFile::ApprovalDecision { request:Component::new(approval["request"].as_str().unwrap()).unwrap(),
                        principal:Component::new(ApprovalDecisionFile::principal_file(&principal)).unwrap() } },
                    serde_json::to_vec(&json!({"principal":principal,"request_sha256":hash,"expected_version":1,
                        "verdict":if matches!(self.case,Case::ParentDeny) {"deny"} else {"approve"},"reason":null})).unwrap());
                call(
                    "send",
                    json!({"agent":agent["reference"],"message":json!({"approval_decision":file}).to_string()}),
                    step,
                )
            }
            3 => call("wait", json!({"agents":["worker"],"mode":"all"}), step),
            _ => ANSWER.to_owned(),
        }
    }
    fn router(self) -> Router {
        let fetched = self.effects.clone();
        Router::new()
            .route(
                "/count",
                post(|| async { Json(json!({"input_tokens":64})) }),
            )
            .route(
                "/effect",
                axum::routing::get(move || {
                    let fetched = fetched.clone();
                    async move {
                        fetched.fetch_add(1, Ordering::SeqCst);
                        "approval side effect"
                    }
                }),
            )
            .route(
                "/responses",
                post(move |Json(body): Json<Value>| {
                    let fixture = self.clone();
                    async move {
                        (
                            [("content-type", "text/event-stream")],
                            fixture.model(&body),
                        )
                    }
                }),
            )
    }
    fn verify(&self, output: &str) {
        assert_eq!(
            self.effects.load(Ordering::SeqCst),
            usize::from(self.case.allowed()),
            "{:?}: {output}",
            self.case
        );
        assert_eq!(
            self.reviews.load(Ordering::SeqCst),
            usize::from(matches!(
                self.case,
                Case::LlmAllow | Case::LlmDeny | Case::Chain
            ))
        );
        if self.case.parent() {
            assert!(
                self.root_calls.load(Ordering::SeqCst) >= 3,
                "parent never answered the request: {output}"
            );
            assert!(
                self.child_calls.load(Ordering::SeqCst) >= 1,
                "child never requested a tool: {output}"
            );
        }
    }
}
struct Files {
    _directory: tempfile::TempDir,
    workspace: std::path::PathBuf,
    settings: std::path::PathBuf,
    catalog: std::path::PathBuf,
    policy: std::path::PathBuf,
}
impl Files {
    fn new(case: Case, address: std::net::SocketAddr) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let settings = directory.path().join("settings");
        std::fs::create_dir(&workspace).unwrap();
        for name in ["codex-home", ".codex", "AppData/Local", "AppData/Roaming"] {
            std::fs::create_dir_all(settings.join(name)).unwrap();
        }
        std::fs::write(settings.join(".codex/config.toml"), "").unwrap();
        let catalog = catalog(directory.path(), address);
        let mut data: Value = serde_json::from_slice(&std::fs::read(&catalog).unwrap()).unwrap();
        data["graphcoder-qual"]["models"]["reviewer"] =
            data["graphcoder-qual"]["models"]["tiny"].clone();
        std::fs::write(&catalog, serde_json::to_vec(&data).unwrap()).unwrap();
        let policy_path = directory.path().join("policy.json");
        std::fs::write(&policy_path, policy(case, address).to_string()).unwrap();
        Self {
            _directory: directory,
            workspace,
            settings,
            catalog,
            policy: policy_path,
        }
    }
    fn command(&self, case: Case, operation: OperationId) -> Command {
        let mut command = self.base_command(case);
        command.args([
            "--operation",
            &operation.to_string(),
            "--input-format",
            "jsonl",
        ]);
        command
    }
    fn base_command(&self, case: Case) -> Command {
        let mut command = Command::new(executable());
        command
            .current_dir(&self.workspace)
            .env("HOME", &self.settings)
            .env("USERPROFILE", &self.settings)
            .env("LOCALAPPDATA", &self.settings)
            .env("APPDATA", &self.settings)
            .env("XDG_DATA_HOME", &self.settings)
            .env("CODEX_HOME", self.settings.join("codex-home"))
            .env_remove("OPENAI_API_KEY")
            .env_remove("GRAPHCODER_MODEL")
            .arg("--workspace")
            .arg(&self.workspace)
            .arg("--settings-root")
            .arg(&self.settings)
            .arg("--catalog")
            .arg(&self.catalog)
            .args(["--model", "graphcoder-qual/tiny"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !matches!(case, Case::Default) {
            command.arg("--approval-policy").arg(&self.policy);
        }
        command
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_prompt_mode_drives_admission_and_default_policy() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fixture = Fixture {
        case: Case::Default,
        address,
        parent_task: acyclic_harness::TaskId::from_bytes([1; 16]),
        effects: Arc::new(AtomicUsize::new(0)),
        root_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
        reviews: Arc::new(AtomicUsize::new(0)),
    };
    let app = fixture.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new(Case::Default, address);
    let mut command = files.base_command(Case::Default);
    command.args(["-p", "hello"]);
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fixture.verify(&String::from_utf8_lossy(&output.stdout));
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
}
#[derive(Default)]
struct Followed {
    request: Option<FileValue>,
    submitted: bool,
    saw_decision: bool,
}
impl Followed {
    fn observe(&mut self, case: Case, file: FileValue, stdin: &mut impl std::io::Write) {
        match &file.key {
            FileKey::Task {
                file: TaskFile::ApprovalRequest(_),
                ..
            } => self.request = Some(file),
            FileKey::Task {
                file: TaskFile::ApprovalReview { .. },
                ..
            } if case.human() && !self.submitted => {
                if String::from_utf8_lossy(&file.bytes).contains("awaiting authenticated decision")
                {
                    let principal = if matches!(case, Case::ForgedPrincipal) {
                        "rules"
                    } else {
                        "human"
                    };
                    let mut decision = decision(
                        self.request.as_ref().unwrap(),
                        principal,
                        !matches!(case, Case::HumanDeny),
                    );
                    if matches!(case, Case::ForgedSubject) {
                        let mut data: Value = serde_json::from_slice(&decision.bytes).unwrap();
                        data["request_sha256"] = json!("11".repeat(32));
                        decision.bytes = serde_json::to_vec(&data).unwrap().into();
                    }
                    writeln!(stdin, "{}", jsonl::request(&decision).unwrap()).unwrap();
                    self.submitted = true;
                }
            }
            FileKey::Task {
                file: TaskFile::ApprovalDecision { .. },
                ..
            } => {
                self.saw_decision = true;
                let value: ApprovalDecisionFile = serde_json::from_slice(&file.bytes).unwrap();
                assert_eq!(
                    value.approved(),
                    case.parent() || case.allowed(),
                    "{case:?}"
                );
            }
            _ => {}
        }
    }
}
fn drive(mut command: Command, case: Case, operation: OperationId) -> String {
    let mut process = command.spawn().unwrap();
    let mut stdin = process.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        jsonl::request(&prepare_local_prompt(operation, "hello").unwrap()).unwrap()
    )
    .unwrap();
    let mut reader = BufReader::new(process.stdout.take().unwrap());
    let mut followed = Followed::default();
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        if let Ok(change) = jsonl::parse_change(&line) {
            for file in change.file.into_iter().chain(change.files) {
                lines.push(format!(
                    "{:?}: {}\n",
                    file.key,
                    String::from_utf8_lossy(&file.bytes)
                ));
                followed.observe(case, file, &mut stdin);
            }
        }
    }
    drop(stdin);
    let output = process.wait_with_output().unwrap();
    assert!(
        output.status.code().is_some(),
        "shipping binary was terminated: {case:?}"
    );
    assert_eq!(
        output.status.success(),
        case.allowed() || matches!(case, Case::ParentDeny),
        "{case:?}: {:?}\n{}\n{}",
        output.status.code(),
        lines.concat(),
        String::from_utf8_lossy(&output.stderr)
    );
    if case.human() {
        assert!(
            followed.submitted,
            "human stage never exercised: {case:?}: {}",
            lines.concat()
        );
    }
    if !case.parent() {
        assert_eq!(
            followed.saw_decision,
            !case.forged(),
            "{case:?}: {}\n{}",
            lines.concat(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    lines.concat()
}
async fn scenario(case: Case) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let operation = OperationId::new();
    let fixture = Fixture {
        case,
        address,
        parent_task: acyclic_harness::runtime::task_id_for_admission(operation),
        effects: Arc::new(AtomicUsize::new(0)),
        root_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
        reviews: Arc::new(AtomicUsize::new(0)),
    };
    let app = fixture.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new(case, address);
    let command = files.command(case, operation);
    let output = tokio::task::spawn_blocking(move || drive(command, case, operation))
        .await
        .unwrap();
    fixture.verify(&output);
    server.abort();
    let _ = server.await;
}
macro_rules! cases { ($($name:ident => $case:ident),* $(,)?) => { $(
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn $name() { scenario(Case::$case).await; }
)* }; }
cases! {
    approvals_graphcoder_default_is_policy_data => Default,
    approvals_rule_allows => RuleAllow,
    approvals_rule_denies_before_effect => RuleDeny,
    approvals_llm_allows => LlmAllow,
    approvals_llm_denies_before_effect => LlmDeny,
    approvals_human_allows_live => HumanAllow,
    approvals_human_denies_before_effect => HumanDeny,
    approvals_forged_principal_is_refused => ForgedPrincipal,
    approvals_forged_subject_is_refused => ForgedSubject,
    approvals_rules_llm_human_chain => Chain,
    approvals_parent_allows => ParentAllow,
    approvals_parent_denies_before_effect => ParentDeny,
    approvals_parent_forgery_is_refused => ParentForged,
}
