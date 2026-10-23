use super::*;

fn defer(mut command: Command, operation: OperationId) -> FileValue {
    let mut process = command.spawn().unwrap();
    let mut input = process.stdin.take();
    writeln!(
        input.as_mut().unwrap(),
        "{}",
        jsonl::request(&prepare_local_prompt(operation, "hello").unwrap()).unwrap()
    )
    .unwrap();
    let mut request = None;
    for line in BufReader::new(process.stdout.take().unwrap()).lines() {
        let line = line.unwrap();
        if let Ok(change) = jsonl::parse_change(&line) {
            for file in change.file.into_iter().chain(change.files) {
                match file.key {
                    FileKey::Task {
                        file: TaskFile::ApprovalRequest(_),
                        ..
                    } => request = Some(file),
                    FileKey::Task {
                        file: TaskFile::ApprovalReview { .. },
                        ..
                    } => {
                        if String::from_utf8_lossy(&file.bytes)
                            .contains("awaiting authenticated decision")
                        {
                            input.take();
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let output = process.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    request.expect("durable request before disconnect")
}

fn exchange(mut command: Command, requests: &[FileValue], expected: i32) {
    let mut process = command.spawn().unwrap();
    let mut input = process.stdin.take().unwrap();
    for request in requests {
        writeln!(input, "{}", jsonl::request(request).unwrap()).unwrap();
    }
    drop(input);
    let output = process.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(expected),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn recovery(cancel: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let operation = OperationId::new();
    let fixture = Fixture {
        case: Case::HumanAllow,
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
    let files = Files::new(Case::HumanAllow, address);
    tokio::task::spawn_blocking(move || {
        let request = defer(files.command(Case::HumanAllow, operation), operation);
        let decision = decision(&request, "human", true);
        assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
        if cancel {
            let cancellation = OperationId::new();
            let mut command = prepare_local_prompt(cancellation, "cancel").unwrap();
            command.bytes =
                serde_json::to_vec(&acyclic_harness::client::CommandResolution::Submit {
                    request: acyclic_harness::client::CommandRequest::Cancel {
                        task: fixture.parent_task,
                    },
                })
                .unwrap()
                .into();
            exchange(files.command(Case::HumanAllow, cancellation), &[command], 0);
            exchange(files.command(Case::HumanAllow, operation), &[decision], 1);
            assert_eq!(
                fixture.effects.load(Ordering::SeqCst),
                0,
                "cancelled decision dispatched"
            );
        } else {
            exchange(
                files.command(Case::HumanAllow, operation),
                &[decision.clone(), decision.clone()],
                0,
            );
            exchange(files.command(Case::HumanAllow, operation), &[decision], 0);
            assert_eq!(
                fixture.effects.load(Ordering::SeqCst),
                1,
                "retry repeated the effect"
            );
            assert_eq!(
                fixture.root_calls.load(Ordering::SeqCst),
                2,
                "reopen regenerated the model"
            );
        }
    })
    .await
    .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_reopen_and_exact_decision_retry_dispatch_once() {
    recovery(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_cancelled_request_cannot_dispatch_after_reopen() {
    recovery(true).await;
}
