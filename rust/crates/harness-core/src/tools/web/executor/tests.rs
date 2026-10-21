use super::*;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

fn fixture_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn assert_published_web_contract(name: &str, value: &Value) -> Result<()> {
    let definition = crate::tools::web::definitions(true)?
        .into_iter()
        .find(|tool| tool.name == name)
        .unwrap();
    let output = jsonschema::validator_for(&definition.output_schema)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let projection = jsonschema::validator_for(&definition.projection_schema)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        output.is_valid(value),
        "actual web result violates its advertised output contract"
    );
    assert!(
        projection.is_valid(&json!({"kind":"json","value":value})),
        "actual web projection violates its advertised contract"
    );
    for bad in [
        json!({"status":"rejected"}),
        json!({"status":"rejected","code":"unauthorized","reason":"denied"}),
        json!({"status":"rejected","code":"cancelled","reason":"cancelled"}),
        json!({"status":"rejected","code":"invalid","reason":""}),
        json!({"status":"rejected","code":"invalid","reason":"denied","response":{}}),
        json!({"status":"indeterminate"}),
    ] {
        assert!(
            !output.is_valid(&bad),
            "advertised schema accepts a fatal/incomplete outcome"
        );
        assert!(!projection.is_valid(&json!({"kind":"json","value":bad})));
    }
    Ok(())
}

#[test]
fn refusals_cannot_hide_fatal_errors_or_bypass_original_work_and_output_limits() -> Result<()> {
    for error in [
        Error::Invalid("private input".into()),
        Error::Storage("private transport".into()),
        Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "unauthorized",
        },
        Error::ToolRejected {
            code: ErrorCode::Cancelled,
            reason: "cancelled",
        },
        Error::NativeBoundary {
            code: ErrorCode::Unsupported,
            reason: "unsupported producer",
        },
    ] {
        let code = error.code();
        assert_eq!(
            rejection(error, 4096, None)
                .expect_err("fatal failure became a tool result")
                .code(),
            code
        );
    }
    let invalid = || Error::ToolRejected {
        code: ErrorCode::Invalid,
        reason: "fetch HTTP request rejected",
    };
    assert!(rejection(invalid(), 1, None).is_err());
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        if !cancelled {
            budget.items_examined = 0;
        }
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let before = work.live_allocation_bytes();
        if cancelled {
            cancellation.cancel();
        }
        let error =
            rejection(invalid(), 4096, Some(&work)).expect_err("refusal bypassed original work");
        assert!(error.is_native_failure(), "{error:?}");
        if cancelled {
            assert_eq!(error.code(), ErrorCode::Cancelled);
        }
        drop(error);
        assert_eq!(work.live_allocation_bytes(), before);
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_and_native_executor_entrypoints_return_refusals_and_never_reconcile_by_redispatch()
-> Result<()> {
    let fixture = crate::durable_host::tests::created_fixture::CreationFixture::new_with_grants(&[
        "tool:call:url_fetch",
    ])
    .await?;
    let task = fixture.root;
    let fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
    let executor = WebExecutor {
        search: None,
        fetch: Arc::new(HttpFetch::new()?),
    };
    for native in [false, true] {
        for case in 0..3 {
            let (url, server) = match case {
                0 => ("not-a-url".into(), None),
                1 => {
                    let (url, server) = serve_response(503, "", "private-body".into()).await?;
                    (url, Some(server))
                }
                _ => {
                    let (url, server) = serve_response(
                        302,
                        "Location: http://127.0.0.1:9/private\r\n",
                        String::new(),
                    )
                    .await?;
                    (url, Some(server))
                }
            };
            let invocation = ToolInvocation {
                operation_id: crate::OperationId::new(),
                call_id: "entrypoint-fetch".into(),
                name: "url_fetch".into(),
                arguments: json!({"url":url}),
            };
            let value = if native {
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let task_context = fixture
                    .runtime
                    .task_context_with_native_work(task, &fence, &work)
                    .await?;
                let io = crate::executor::PreparationIo::from_native_work(&work)?;
                let context = ToolContext::new(
                    task_context,
                    invocation.operation_id,
                    &invocation.call_id,
                    Some(&io),
                )?;
                let (result, held) = executor
                    .execute_with_native_context(&context, &invocation, &work)?
                    .await?;
                assert!(held.work().shares_allocation_context(&work));
                let (reconciled, _) = executor
                    .reconcile_with_native_context(&context, &invocation, &work)?
                    .await?;
                assert!(
                    reconciled.is_none(),
                    "uncertain execution was redispatched during reconciliation"
                );
                let foreign_cancel = acyclic_fs::CancellationToken::new();
                let foreign = NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &foreign_cancel,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                assert!(matches!(
                    executor.execute_with_native_context(&context, &invocation, &foreign),
                    Err(Error::NativeBoundary {
                        code: ErrorCode::Unauthorized,
                        ..
                    })
                ));
                result.value
            } else {
                let context = ToolContext::new(
                    fixture
                        .runtime
                        .task_context(task, fence.clone())
                        .await?
                        .into_shared()?,
                    invocation.operation_id,
                    &invocation.call_id,
                    None,
                )?;
                let result = executor
                    .execute(invocation.clone(), Some(context.clone()))
                    .await?;
                assert!(
                    executor
                        .reconcile(invocation.clone(), Some(context))
                        .await?
                        .is_none()
                );
                result.value
            };
            assert_eq!(value["status"], "rejected");
            assert_eq!(value["code"], "invalid");
            assert_published_web_contract("url_fetch", &value)?;
            if let Some(server) = server {
                server.await.map_err(fixture_error)??;
            }
        }
    }
    Ok(())
}

async fn serve_response(
    status: u16,
    headers: &str,
    content: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    serve_web_response("GET", status, headers, content).await
}

async fn serve_web_response(
    method: &'static str,
    status: u16,
    headers: &str,
    content: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| Error::Storage("fixture bind failed".into()))?;
    let address = listener
        .local_addr()
        .map_err(|_| Error::Storage("fixture address failed".into()))?;
    let response = format!(
        "HTTP/1.1 {status} Test\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{content}",
        content.len()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|_| Error::Storage("fixture accept failed".into()))?;
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let count = socket
                .read(&mut buffer)
                .await
                .map_err(|_| Error::Storage("fixture read failed".into()))?;
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 8192);
            if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        assert!(request.starts_with(format!("{method} /source HTTP/1.1\r\n").as_bytes()));
        socket
            .write_all(response.as_bytes())
            .await
            .map_err(|_| Error::Storage("fixture write failed".into()))?;
        Ok(())
    });
    Ok((format!("http://{address}/source"), server))
}
