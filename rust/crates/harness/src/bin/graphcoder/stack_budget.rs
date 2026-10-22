//! A whole GraphCoder turn fits the process main thread's stack.
//!
//! `main` polls every turn on the process main thread, whose stack is fixed by
//! the platform: 1 MiB on Windows, the smallest of the release platforms. The
//! budget is measured on the target being built, through the same `serve` that
//! `main` calls, so every poll frame of the turn, the tools it runs, and the
//! filesystem work under them is counted. Each entry point `serve` selects is
//! measured: the headless prompt, JSONL over standard input, and the TUI on a
//! real pseudo-terminal (script(1) on Unix, ConPTY on Windows).
use crate::{args::Args, entry::serve};
#[path = "stack_budget/terminal.rs"]
mod terminal;
#[path = "stack_budget/unboxed.rs"]
mod unboxed;
use acyclic_test_support::stack::deepest_stack;
use axum::{Router, body::Bytes, routing::post};
use clap::Parser;
use serde_json::json;
use std::sync::{Arc, Mutex};

/// The smallest process main-thread stack of a release platform (Windows).
const MAIN_THREAD_STACK_KIB: usize = 1024;
/// Every measured turn leaves at least this many times its own depth unused.
const MARGIN: usize = 4;
const BUDGET_KIB: usize = MAIN_THREAD_STACK_KIB / MARGIN;

/// The task every scripted spawn gives its child. Quoted, so the child's own
/// request (where it is one JSON string) is told apart from the parent's
/// (where it is nested in the spawn call's argument string).
const CHILD_TASK: &str = r#"stack "child" task"#;
const CHILD_TASK_IN_REQUEST: &str = r#"stack \"child\" task"#;

/// Substitutes the file the latest tool result names for a `"$FILE"` argument.
fn with_file(arguments: &serde_json::Value, body: &str) -> String {
    let arguments = arguments.as_str().unwrap();
    if !arguments.contains("\"$FILE\"") {
        return arguments.to_owned();
    }
    let body: serde_json::Value = serde_json::from_str(body).unwrap();
    let file = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .filter(|item| item["type"] == "function_call_output")
        .find_map(|item| {
            let output: serde_json::Value = serde_json::from_str(item["output"].as_str()?).ok()?;
            output.get("file").cloned()
        })
        .expect("a prior tool result names a file");
    arguments.replace("\"$FILE\"", &file.to_string())
}

/// The provider answers each request with the next scripted turn.
fn scripted(
    turns: Vec<serde_json::Value>,
    child_writes: bool,
) -> (
    std::net::SocketAddr,
    Arc<Mutex<Vec<String>>>,
    tokio::sync::oneshot::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    // The request log is the only fixture lock; no other lock is acquired under it.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::clone(&seen);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/source", axum::routing::get(|| async { SOURCE }))
        .route(
            "/count",
            post(|| async { axum::Json(json!({"input_tokens": 64})) }),
        )
        .route(
            "/responses",
            post(move |body: Bytes| {
                let requests = Arc::clone(&requests);
                let turns = turns.clone();
                async move {
                    let mut requests = requests.lock().unwrap();
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    // Children run their own turns against the same provider;
                    // only the parent's requests advance the script.
                    let turn = if body.contains(CHILD_TASK_IN_REQUEST) {
                        // A writing child leaves a change on its own branch,
                        // so the parent's merge joins two diverged trees.
                        if child_writes && tool_outputs(&body).is_empty() {
                            let mut write = call(
                                "write",
                                json!({"path": "child.txt", "text": "child\n",
                                    "media_type": "text/plain", "display_name": "child.txt"}),
                            );
                            write["call_id"] = json!("child-write");
                            write
                        } else {
                            text("child done")
                        }
                    } else if body.contains("stack fixture") {
                        let outputs = tool_outputs(&body);
                        match outputs.len() {
                            0 => child_call("spawn"),
                            1 => {
                                let mut wait = child_call("wait");
                                wait["call_id"] = json!("fixture-wait");
                                wait
                            }
                            _ => {
                                assert_eq!(outputs[1]["agents"][0]["result"]["outcome"]["Succeeded"]["text"], "child done");
                                text("prepared")
                            }
                        }
                    } else {
                        let mut turn = turns
                            .get(requests.len())
                            .cloned()
                            .unwrap_or_else(|| json!({"type": "message", "text": "unscripted"}));
                        // A terminal driver quits the TUI once the script is served.
                        if requests.len() + 1 == turns.len()
                            && let Some(settled) = std::env::var_os(SETTLED)
                        {
                            std::fs::write(settled, b"settled").unwrap();
                        }
                        if turn["type"] == "function_call" {
                            turn["call_id"] = json!(format!("call{}", requests.len()));
                            turn["arguments"] = json!(
                                with_file(&turn["arguments"], &body)
                                    .replace("$ORIGIN", &format!("http://{address}"))
                            );
                            // Actual SDK rejection: popping before any push
                            // must not count as successful deep tool coverage.
                            if std::env::var_os(EARLY_ERROR).is_some()
                                && turn["name"] == "fs"
                                && turn["arguments"] == json!(json!({"command": "stash push"}).to_string())
                            {
                                turn["arguments"] = json!(json!({"command": "stash pop"}).to_string());
                            }
                        }
                        requests.push(body);
                        turn
                    };
                    eprintln!("stack provider response: {} {}", turn["name"], turn["arguments"]);
                    let event = if turn["type"] == "message" {
                        json!({"type": "response.output_text.delta", "delta": turn["text"]})
                    } else {
                        json!({"type": "response.output_item.done", "output_index": 0, "item": turn})
                    };
                    let events = [
                        event,
                        json!({"type": "response.completed", "response": {"status": "completed",
                            "usage": {"input_tokens": 64, "output_tokens": 2}}}),
                    ];
                    let stream: String = events
                        .iter()
                        .map(|event| format!("data: {event}\n\n"))
                        .collect();
                    ([("content-type", "text/event-stream")], stream)
                }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
    });
    (address, seen, stop, server)
}

/// How the measured invocation receives its prompt: each is a distinct `serve` path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Entry {
    /// `-p`: the headless client.
    Prompt,
    /// `--input-format jsonl`: the JSONL client over standard input.
    Jsonl,
    /// No prompt, on a terminal: the TUI and its local command loop.
    Tui,
}

/// Names the file the scripted provider writes once its last turn is served.
const SETTLED: &str = "GRAPHCODER_STACK_SETTLED";
/// Selects the case in a process whose argv a terminal host fixes.
const CASE: &str = "GRAPHCODER_STACK_CASE";
/// Fixture-only counterexample to accepting an early tool error as coverage.
const EARLY_ERROR: &str = "GRAPHCODER_STACK_EARLY_ERROR";

/// Runs one invocation through `serve` and returns its exit code and depth in bytes.
fn measured_turn<const UNBOXED: bool>(
    turns: Vec<serde_json::Value>,
    entry: Entry,
    child_writes: bool,
) -> (u8, usize, Vec<String>) {
    let spawn = turns.first().is_some_and(|turn| turn["name"] == "spawn");
    let existing_child = child_writes
        || turns
            .first()
            .is_some_and(|turn| matches!(turn["name"].as_str(), Some("send" | "wait" | "archive")));
    let (address, requests, stop, server) = scripted(turns, child_writes);
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    std::fs::write(workspace.join("calc.py"), "x = 1\n").unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"stack": {
        "adapter": "responses", "authentication": "optional", "base_url": format!("http://{address}"),
        "models": {"tiny": {"capacity": {"context_tokens": 8192, "output_tokens": 1024},
            "features": {"tools": true, "images": false}}},
        "max_event_bytes": 65536,
        "token_counter": {"path": "count", "adapter": "responses",
            "response_field": "input_tokens", "framing_tokens": 0}
    }})).unwrap())
    .unwrap();
    let operation =
        std::env::args().find_map(|arg| arg.strip_prefix("--stack-operation=").map(str::to_owned));
    let mut argv: Vec<std::ffi::OsString> = vec![
        "graphcoder".into(),
        "--workspace".into(),
        workspace.clone().into(),
        "--catalog".into(),
        catalog.clone().into(),
        "--settings-root".into(),
        settings.clone().into(),
        "--model".into(),
        "stack/tiny".into(),
    ];
    match entry {
        Entry::Prompt => argv.extend(["-p".into(), "go".into()]),
        Entry::Jsonl => argv.extend([
            "--input-format".into(),
            "jsonl".into(),
            "--operation".into(),
            operation.expect("a JSONL case names its operation").into(),
        ]),
        Entry::Tui => {}
    }
    if existing_child {
        // Prepare through the shipping entrypoint, with its ordinary child
        // admission. The measured invocation must find a real admitted child.
        let mut fixture_args = argv.clone();
        *fixture_args.last_mut().unwrap() = "stack fixture".into();
        let code = serve(
            Args::try_parse_from(fixture_args).unwrap(),
            Some(acyclic_harness_providers::Credentials::explicit(
                Default::default(),
            )),
            Some(directory.path().join("state")),
        )
        .unwrap();
        assert_eq!(code, 0, "child fixture must complete successfully");
        admitted(
            &workspace,
            &catalog,
            &settings,
            &directory.path().join("state"),
        );
    }
    let args = Args::try_parse_from(argv).unwrap();
    let entry = if UNBOXED { unboxed::serve } else { serve };
    let callback = || {
        entry(
            args,
            Some(acyclic_harness_providers::Credentials::explicit(
                Default::default(),
            )),
            Some(directory.path().join("state")),
        )
        .map_err(|error| error.to_string())
    };
    let (code, used) = deepest_stack(callback);
    if spawn {
        admitted(
            &workspace,
            &catalog,
            &settings,
            &directory.path().join("state"),
        );
    }
    let _ = stop.send(());
    server.join().unwrap();
    let requests = requests.lock().unwrap().clone();
    if let Some(first) = requests.first() {
        let body: serde_json::Value = serde_json::from_str(first).unwrap();
        let names: Vec<_> = body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        eprintln!("offered tools: {names:?}");
    }
    eprintln!(
        "GraphCoder turn: {} KiB, {} requests, unboxed CLI boundary: {UNBOXED}",
        used.div_ceil(1024),
        requests.len()
    );
    (code.unwrap(), used, requests)
}

// Run every case in its own process on the platform's process main thread.
pub(crate) fn qualify() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let cases = [
        "diff",
        "commit",
        "tools-files",
        "tools-history",
        "tools-branches",
        "tools-recovery",
        "tools-rejected",
        "merge-continue",
        "merge-abort",
        "child-merge",
        "spawn",
        "send",
        "wait",
        "archive",
        "jsonl",
        "tui",
        "unboxed",
    ];
    if args.iter().any(|arg| arg == "--list") {
        if !args.iter().any(|arg| arg == "--ignored") {
            for case in cases {
                println!("stack_budget::{case}: test");
            }
        }
        return;
    }
    if let Some(case) = std::env::var(CASE).ok().or_else(|| {
        std::env::args().find_map(|arg| arg.strip_prefix("--stack-case=").map(str::to_owned))
    }) {
        let turns = match case.as_str() {
            "diff" | "jsonl" | "tui" => {
                vec![fs("diff"), fs("commit -m seed"), fs("diff"), text("done")]
            }
            "commit" | "unboxed" => vec![fs("commit -m stack"), text("done")],
            "spawn" => vec![child_call("spawn"), text("done")],
            "tools-rejected" => tool_group("tools-recovery"),
            case if case.starts_with("tools-") => tool_group(case),
            "merge-continue" | "merge-abort" => merge_recovery(&case),
            "child-merge" => child_merge(),
            "send" | "wait" | "archive" => vec![child_call(&case), text("done")],
            _ => panic!("unknown stack case {case}"),
        };
        let entry = match case.as_str() {
            "jsonl" => Entry::Jsonl,
            "tui" => Entry::Tui,
            _ => Entry::Prompt,
        };
        let child_writes = case == "child-merge";
        let (code, used, requests) = if case == "unboxed" {
            measured_turn::<true>(turns, entry, child_writes)
        } else {
            measured_turn::<false>(turns, entry, child_writes)
        };
        if case.starts_with("tools-") {
            covers_every_entry_point(
                if case == "tools-rejected" {
                    "tools-recovery"
                } else {
                    &case
                },
                code,
                used,
                &requests,
            );
            return;
        }
        if case.starts_with("merge-") {
            covers_merge_recovery(&case, code, used, &requests);
            return;
        }
        if case == "child-merge" {
            covers_child_merge(code, used, &requests);
            return;
        }
        if case == "unboxed" {
            assert_eq!(code, 0);
            assert!(
                requests
                    .get(1)
                    .expect("tool response request")
                    .contains("Committed")
            );
            assert!(
                used >= BUDGET_KIB * 1024,
                "unboxed boundary escaped measurement: {used} bytes"
            );
        } else {
            assert!(
                used < BUDGET_KIB * 1024,
                "{case}: {used} bytes exceeds the strict {BUDGET_KIB} KiB budget"
            );
            if case == "spawn" {
                assert_eq!(code, 0, "spawn: real child operation did not complete");
                // The scripted call is turn 0, so its result is in request 1;
                // later requests may follow child notifications.
                let outputs = tool_outputs(requests.get(1).expect("spawn result request"));
                assert_eq!(outputs.last(), Some(&json!({"agent":"kid"})));
            } else if matches!(case.as_str(), "send" | "wait" | "archive") {
                assert_eq!(code, 0, "{case}: real child operation did not complete");
                let expected = match case.as_str() {
                    "send" => json!({"agent":"kid", "queued":true}),
                    "archive" => json!({"agent":"kid"}),
                    _ => serde_json::Value::Null,
                };
                let outputs = tool_outputs(requests.get(1).expect("child result request"));
                let output = outputs.last().expect("child result reached model");
                if case == "wait" {
                    assert_eq!(output["agents"][0]["reference"], "kid", "{output}");
                    assert_eq!(output["agents"][0]["result"]["turn"], 1, "{output}");
                    assert_eq!(
                        output["agents"][0]["result"]["outcome"]["Succeeded"]["text"], "child done",
                        "{output}"
                    );
                } else {
                    assert_eq!(output, &expected);
                }
            } else if !CHILD_TOOLS.contains(&case.as_str()) {
                assert_eq!(code, 0);
                assert!(requests.len() >= 2, "tool result was not returned");
                // The tool result is a JSON string inside the request body.
                let expected = if matches!(case.as_str(), "diff" | "jsonl" | "tui") {
                    r#"\"kind\":\"diff\""#
                } else {
                    "Committed"
                };
                let response = &requests[1];
                assert!(
                    response.contains(expected),
                    "{case}: tool response lacks {expected:?}: {response}"
                );
            }
        }
        return;
    }
    let filter = args.iter().find(|arg| !arg.starts_with('-'));
    let exact = args.iter().any(|arg| arg == "--exact");
    if args.iter().any(|arg| arg == "--ignored") {
        return;
    }
    assert_process_stack();
    for case in cases {
        let name = format!("stack_budget::{case}");
        if filter.is_some_and(|filter| {
            if exact {
                &name != filter
            } else {
                !name.contains(filter.as_str())
            }
        }) {
            continue;
        }
        let executable = std::env::current_exe().unwrap();
        if case == "tui" {
            terminal::run(&executable);
            continue;
        }
        #[cfg(unix)]
        let mut command = {
            let mut command = std::process::Command::new("sh");
            command
                .args(["-c", "ulimit -s 1024; exec \"$@\"", "stack-main"])
                .arg(executable);
            command
        };
        #[cfg(windows)]
        let mut command = std::process::Command::new(executable);
        command.arg(format!("--stack-case={case}"));
        if case == "tools-rejected" {
            let output = command.env(EARLY_ERROR, "1").output().unwrap();
            let error = String::from_utf8_lossy(&output.stderr);
            assert_eq!(
                output.status.code(),
                Some(101),
                "early-error control: {error}"
            );
            assert!(
                error.contains("expected the successful operation") && error.contains("stash push"),
                "early-error control failed for another reason: {error}"
            );
            continue;
        }
        let status = if case == "jsonl" {
            use std::io::Write as _;
            let operation = acyclic_harness::OperationId::new();
            let request = acyclic_harness::client::jsonl::request(
                &acyclic_harness::client::prepare_local_prompt(operation, "go").unwrap(),
            )
            .unwrap();
            let mut child = command
                .arg(format!("--stack-operation={operation}"))
                .stdin(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdin = child.stdin.take().unwrap();
            writeln!(stdin, "{request}").unwrap();
            drop(stdin);
            child.wait().unwrap()
        } else {
            command.status().unwrap()
        };
        assert!(status.success(), "main-thread {case}");
    }
}

fn call(name: &str, arguments: serde_json::Value) -> serde_json::Value {
    json!({"type": "function_call", "call_id": "call", "name": name,
        "arguments": arguments.to_string()})
}

/// Every model tool GraphCoder offers. A tool the provider sees that is not
/// listed here fails the `tools` case, so a new tool cannot ship unmeasured.
#[cfg(not(windows))]
const TOOLS: &[&str] = &[
    "archive",
    "bash",
    "edit",
    "fs",
    "read",
    "send",
    "spawn",
    "url_fetch",
    "wait",
    "write",
];
#[cfg(windows)]
const TOOLS: &[&str] = &[
    "archive",
    "edit",
    "fs",
    "powershell",
    "read",
    "send",
    "spawn",
    "url_fetch",
    "wait",
    "write",
];

/// The platform shell tool GraphCoder offers: bash on Unix, PowerShell on Windows.
const SHELL: &str = if cfg!(windows) { "powershell" } else { "bash" };

/// Writes `text` byte for byte to `path` through the platform shell. The
/// fixture texts never contain a single quote.
fn write_file(path: &str, text: &str) -> serde_json::Value {
    assert!(!text.contains('\''));
    let command = if cfg!(windows) {
        format!("[IO.File]::WriteAllText((Join-Path $PWD '{path}'), '{text}')")
    } else {
        format!("printf '%s' '{text}' > {path}")
    };
    call(SHELL, json!({ "command": command }))
}

/// Prints the files' exact bytes, in order, through the platform shell.
fn print_files(paths: &[&str]) -> serde_json::Value {
    let command = if cfg!(windows) {
        let paths = paths
            .iter()
            .map(|path| format!("'{path}'"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "foreach ($path in {paths}) {{ [Console]::Out.Write([IO.File]::ReadAllText((Join-Path $PWD $path))) }}"
        )
    } else {
        format!("cat {}", paths.join(" "))
    };
    call(SHELL, json!({ "command": command }))
}

/// The body the fixture serves to `url_fetch`, a real credential-free GET.
const SOURCE: &str = "stack source\n";

/// One `fs` command per Git-compatible command variant, in an order where each
/// finds the history it acts on. `apply` takes the typed patch path, so it is
/// the one spelling `parse_git_argv` does not admit.
const FS_COMMANDS: &[&str] = &[
    "status",
    "add -A",
    "commit -m first",
    "log -n 5",
    "show HEAD",
    "branch topic",
    "switch topic",
    "commit -m topic",
    "switch main",
    "merge topic",
    "merge --continue",
    "merge --abort",
    "rebase topic",
    "cherry-pick topic",
    "revert HEAD",
    "stash push",
    "stash pop",
    "tag v1",
    "blame notes.txt",
    "grep alpha",
    "clean -n",
    "archive HEAD",
    "apply change.patch",
    "commit -m patched",
    "bisect start",
    "rev-parse HEAD",
    "symbolic-ref HEAD",
    "merge-base HEAD topic",
    "discard topic",
    "ls-files",
    "check-ignore notes.txt",
    "restore --source HEAD notes.txt",
    "reset --soft HEAD",
    "diff",
];

/// The tools that act on child agents. Each settles the headless turn, so each
/// is measured in a process of its own.
const CHILD_TOOLS: &[&str] = &["spawn", "send", "wait", "archive"];

fn child_call(name: &str) -> serde_json::Value {
    match name {
        "spawn" => call("spawn", json!({"task": CHILD_TASK, "name": "kid"})),
        "send" => call("send", json!({"agent": "kid", "message": "hello"})),
        "wait" => call("wait", json!({"agents": ["kid"], "mode": "any"})),
        "archive" => call("archive", json!({"agent": "kid"})),
        _ => panic!("{name} is not a child tool"),
    }
}

/// A session that calls every other tool and every `fs` command on the main thread.
fn tool_group(case: &str) -> Vec<serde_json::Value> {
    let mut turns = vec![
        call(
            "write",
            json!({"path": "notes.txt", "text": "alpha beta
",
            "media_type": "text/plain", "display_name": "notes.txt"}),
        ),
        call("read", json!({"file": "$FILE"})),
        call(
            "write",
            json!({"path": "notes.txt", "text": "alpha beta
",
            "media_type": "text/plain", "display_name": "notes.txt"}),
        ),
        call(
            "edit",
            json!({"file": "$FILE", "old_text": "beta", "new_text": "gamma"}),
        ),
        call("url_fetch", json!({"url": "$ORIGIN/source"})),
        write_file(
            "change.patch",
            "diff --git a/calc.py b/calc.py\n--- a/calc.py\n+++ b/calc.py\n@@ -1 +1 @@\n-x = 1\n+x = 2\n",
        ),
    ];
    // Every group starts with the same file tools and its own complete history.
    // Group by public operation family so each semantic suite stays bounded.
    if case != "tools-branches" {
        turns.push(fs("commit -m seed"));
        turns.push(fs("branch topic"));
    }
    for command in tool_commands(case) {
        // A commit must capture a real change. An early NothingToCommit reply
        // never reaches the filesystem work whose stack this suite measures.
        if matches!(*command, "commit -m first" | "commit -m topic") {
            turns.push(write_file("notes.txt", &format!("alpha {command}\n")));
        }
        turns.push(fs(command));
    }
    if case == "tools-recovery" {
        turns.push(print_files(&["calc.py"]));
    }
    turns.push(text("done"));
    turns
}

fn tool_commands(case: &str) -> &'static [&'static str] {
    match case {
        "tools-files" => &[
            "status",
            "add -A",
            "clean -n",
            "ls-files",
            "check-ignore notes.txt",
            "restore --source HEAD notes.txt",
            "reset --soft HEAD",
            "diff",
        ],
        "tools-history" => &[
            "commit -m first",
            "log -n 5",
            "show HEAD",
            "tag v1",
            "blame notes.txt",
            "grep alpha",
            "archive HEAD",
            "rev-parse HEAD",
            "symbolic-ref HEAD",
            "merge-base HEAD topic",
            "discard topic",
        ],
        "tools-branches" => &[
            "commit -m first",
            "branch topic",
            "switch topic",
            "commit -m topic",
            "switch main",
            "merge topic",
            "rebase topic",
            "cherry-pick topic",
            "revert HEAD",
        ],
        "tools-recovery" => &[
            "stash push",
            "stash pop",
            "apply change.patch",
            "commit -m patched",
            "bisect start",
        ],
        _ => panic!("unknown tool group {case}"),
    }
}
fn covers_every_entry_point(case: &str, code: u8, used: usize, requests: &[String]) {
    assert!(
        used < BUDGET_KIB * 1024,
        "tools: {used} bytes exceeds the strict {BUDGET_KIB} KiB budget"
    );
    assert_eq!(code, 0, "tools: the session did not complete");
    let offered: serde_json::Value = serde_json::from_str(&requests[0]).unwrap();
    let mut offered: Vec<_> = offered["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect();
    offered.sort();
    assert_eq!(offered, TOOLS, "every offered tool has a measured call");
    let called: std::collections::BTreeSet<_> = tool_group(case)
        .iter()
        .filter_map(|turn| turn["name"].as_str().map(str::to_owned))
        .chain(CHILD_TOOLS.iter().map(|tool| (*tool).to_owned()))
        .collect();
    assert_eq!(called.into_iter().collect::<Vec<_>>(), TOOLS);
    let mut variants = std::collections::BTreeSet::new();
    let grouped: std::collections::BTreeSet<_> = [
        "tools-files",
        "tools-history",
        "tools-branches",
        "tools-recovery",
    ]
    .into_iter()
    .flat_map(tool_commands)
    .copied()
    .chain(["merge --continue", "merge --abort"])
    .collect();
    assert_eq!(
        grouped,
        FS_COMMANDS.iter().copied().collect(),
        "all command paths remain measured"
    );
    for command in grouped {
        let argv: Vec<String> = command.split(' ').map(str::to_owned).collect();
        let variant = if argv[0] == "apply" {
            "Apply"
        } else {
            acyclic_fs::git_compat::parse_git_argv::<std::io::Error>(&argv, "stack", 0)
                .unwrap_or_else(|error| panic!("{command}: {error}"))
                .variant_name()
        };
        variants.insert(variant);
    }
    let all: std::collections::BTreeSet<_> = acyclic_fs::git_compat::GIT_COMPAT_COMMAND_VARIANTS
        .iter()
        .copied()
        .collect();
    assert_eq!(
        variants, all,
        "every fs command variant has a measured call"
    );
    // Each result must prove the successful operation, not just a reply. The
    // conflict-only merge recovery variants have their own successful suites.
    let outputs = tool_outputs(requests.last().unwrap());
    let turns = tool_group(case);
    assert_eq!(outputs.len(), turns.len() - 1);
    for (turn, output) in turns.iter().zip(&outputs) {
        successful_tool_result(turn, output);
    }
    if case == "tools-recovery" {
        assert_eq!(outputs.last().unwrap()["stdout"], "x = 2\n");
    }
}

fn successful_tool_result(turn: &serde_json::Value, output: &serde_json::Value) {
    assert!(
        tool_result_succeeded(turn, output),
        "{turn}: expected the successful operation, received {output}"
    );
    // The former reply-count oracle accepted this same early error for every
    // entry point. Keep that counterexample beside each successful result.
    assert!(
        !tool_result_succeeded(turn, &json!({"error": "rejected before filesystem work"})),
        "{turn}: an early error passed the semantic oracle"
    );
}

fn tool_result_succeeded(turn: &serde_json::Value, output: &serde_json::Value) -> bool {
    let arguments: serde_json::Value =
        serde_json::from_str(turn["arguments"].as_str().unwrap()).unwrap();
    match turn["name"].as_str().unwrap() {
        "read" => output == "alpha beta\n",
        "write" | "edit" => output["file"].is_object(),
        name if name == SHELL => output["exit_code"] == 0 && output["success"] == true,
        "url_fetch" => output["content"] == SOURCE,
        "fs" => {
            let command = arguments["command"].as_str().unwrap();
            let Ok(result) =
                serde_json::from_value::<acyclic_fs::GitCommandOutput>(output["result"].clone())
            else {
                return false;
            };
            use acyclic_fs::{GitCommandOutput as O, GitFilesystemResult as F};
            match command.split_whitespace().next().unwrap() {
                "commit" | "merge" | "rebase" | "cherry-pick" | "revert" => {
                    matches!(result, O::Committed(_))
                }
                "status" => matches!(result, O::Status(_)),
                "log" | "show" => matches!(&result, O::Commits(commits) if !commits.is_empty()),
                "tag" => matches!(&result, O::Tags(tags) if tags.contains_key("v1")),
                "branch" => {
                    matches!(result, O::Filesystem(F::Forked { workspace_id })
                        if workspace_id.into_bytes() != [0; 16])
                }
                // A transition that moves the tree reports the applied tree;
                // one that leaves it unchanged reports no filesystem effect.
                "switch" | "stash" => {
                    result == O::NoOp || matches!(result, O::Filesystem(F::Applied { .. }))
                }
                "reset" | "add" => result == O::NoOp,
                "rev-parse" | "symbolic-ref" | "merge-base" => {
                    matches!(&result, O::Text(text) if !text.is_empty())
                }
                "ls-files" => {
                    matches!(&result, O::Paths(paths) if paths.iter().any(|path| path == "notes.txt"))
                }
                "bisect" => matches!(result, O::Bisect(_)),
                "discard" => {
                    matches!(&result, O::Branches { current, branches }
                        if current == "main" && !branches.iter().any(|branch| branch.name == "topic"))
                }
                "apply" | "restore" => matches!(result, O::Filesystem(F::Applied { .. })),
                inspection @ ("blame" | "grep" | "clean" | "archive" | "check-ignore" | "diff") => {
                    matches!(&result, O::Filesystem(F::Data { kind, .. }) if kind == inspection)
                }
                _ => panic!("missing semantic oracle for {command}"),
            }
        }
        name => panic!("missing semantic oracle for {name}"),
    }
}

fn fs(command: &str) -> serde_json::Value {
    json!({"type": "function_call", "call_id": "call", "name": "fs",
        "arguments": json!({"command": command}).to_string()})
}
fn text(text: &str) -> serde_json::Value {
    json!({"type": "message", "text": text})
}

fn tool_outputs(request: &str) -> Vec<serde_json::Value> {
    let request: serde_json::Value = serde_json::from_str(request).unwrap();
    request["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .map(|item| serde_json::from_str(item["output"].as_str().unwrap()).unwrap())
        .collect()
}

fn merge_recovery(case: &str) -> Vec<serde_json::Value> {
    let mut turns = vec![
        fs("commit -m base"),
        fs("branch topic"),
        fs("switch topic"),
        write_file("calc.py", "theirs\n"),
        fs("commit -m theirs"),
        fs("switch main"),
        write_file("calc.py", "ours\n"),
        fs("commit -m ours"),
        fs("merge topic"),
    ];
    if case == "merge-continue" {
        turns.push(write_file("calc.py", "resolved\n"));
        turns.push(fs("merge --continue"));
    } else {
        turns.push(fs("merge --abort"));
    }
    turns.push(print_files(&["calc.py"]));
    turns.push(fs("status"));
    turns.push(text("done"));
    turns
}

fn covers_merge_recovery(case: &str, code: u8, used: usize, requests: &[String]) {
    assert_eq!(code, 0, "{case}");
    assert!(used < BUDGET_KIB * 1024, "{case}: {used} bytes");
    let outputs = tool_outputs(requests.last().unwrap());
    assert_eq!(outputs.len(), merge_recovery(case).len() - 1);
    assert!(
        outputs[8].to_string().contains("unresolved conflicts"),
        "merge did not conflict: {}",
        outputs[8]
    );
    let recovery = if case == "merge-continue" { 10 } else { 9 };
    assert!(
        outputs[recovery]["result"].is_object(),
        "recovery failed: {}",
        outputs[recovery]
    );
    let expected = if case == "merge-continue" {
        "resolved\n"
    } else {
        "ours\n"
    };
    assert_eq!(
        outputs[recovery + 1]["stdout"],
        expected,
        "wrong recovered bytes: {}",
        outputs[recovery + 1]
    );
    assert!(
        outputs.last().unwrap()["result"]["Status"].is_object(),
        "status failed: {}",
        outputs.last().unwrap()
    );
}

/// The parent joins a finished child's branch into its own diverged tree:
/// the `fs merge agents/<child>` path of a spawned agent's work.
fn child_merge() -> Vec<serde_json::Value> {
    vec![
        call(
            "write",
            json!({"path": "parent.txt", "text": "parent\n",
                "media_type": "text/plain", "display_name": "parent.txt"}),
        ),
        child_call("archive"),
        fs("merge agents/kid"),
        print_files(&["child.txt", "parent.txt"]),
        text("done"),
    ]
}

fn covers_child_merge(code: u8, used: usize, requests: &[String]) {
    assert_eq!(code, 0, "child-merge: the session did not complete");
    assert!(
        used < BUDGET_KIB * 1024,
        "child-merge: {used} bytes exceeds the strict {BUDGET_KIB} KiB budget"
    );
    let outputs = tool_outputs(requests.last().unwrap());
    assert_eq!(outputs.len(), child_merge().len() - 1, "{outputs:?}");
    assert!(
        outputs[0]["file"].is_object(),
        "parent write: {}",
        outputs[0]
    );
    assert_eq!(
        outputs[1],
        json!({"agent": "kid"}),
        "archive: {}",
        outputs[1]
    );
    let merged =
        serde_json::from_value::<acyclic_fs::GitCommandOutput>(outputs[2]["result"].clone());
    assert!(
        matches!(merged, Ok(acyclic_fs::GitCommandOutput::Committed(_))),
        "child merge did not commit a join: {}",
        outputs[2]
    );
    assert_eq!(
        outputs[3]["stdout"], "child\nparent\n",
        "merged tree lacks either side: {}",
        outputs[3]
    );
}

fn admitted(
    workspace: &std::path::Path,
    catalog: &std::path::Path,
    settings: &std::path::Path,
    state: &std::path::Path,
) {
    use acyclic_harness::local::{LocalHarness, LocalHarnessConfig};
    use acyclic_harness::task_volume::{ChildFile, ChildStatus, Component, FileKey, Query};
    use acyclic_harness_providers::{Credentials, Registry};
    let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
    registry.load_catalog(catalog).unwrap();
    let provider = Arc::new(registry.resolve("stack/tiny").unwrap());
    let config = LocalHarnessConfig::new(workspace, (provider.model().clone(), provider))
        .state_directory(state)
        .settings(
            acyclic_harness::native_settings::SettingsConfig::new(workspace, settings)
                .codex_home(settings.join(".codex")),
        );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(acyclic_fs::in_heap(|| async move {
            let local = LocalHarness::open(config).await.unwrap();
            let snapshot = local
                .task_files()
                .snapshot(&Query::Child(Component::new("kid").unwrap()), 16)
                .await
                .unwrap();
            let file = snapshot
                .files
                .iter()
                .find(|file| {
                    matches!(
                        &file.key,
                        FileKey::Child {
                            file: ChildFile::Status,
                            ..
                        }
                    )
                })
                .expect("spawn must publish its admitted child status");
            let status = ChildStatus::parse(&file.bytes).unwrap();
            assert_eq!(status.inputs, 1);
            local.disconnect().await.unwrap();
        }));
}

#[cfg(windows)]
fn assert_process_stack() {
    let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
    let word = |offset| {
        u32::from_le_bytes(bytes.get(offset..offset + 4).unwrap().try_into().unwrap()) as usize
    };
    let pe = word(0x3c);
    assert_eq!(bytes.get(pe..pe + 4).unwrap(), b"PE\0\0");
    let offset = pe + 24 + 72;
    let reserved = u64::from_le_bytes(bytes.get(offset..offset + 8).unwrap().try_into().unwrap());
    assert_eq!(
        reserved,
        1024 * 1024,
        "the executable must retain the default 1 MiB stack"
    );
}
#[cfg(unix)]
fn assert_process_stack() {}
