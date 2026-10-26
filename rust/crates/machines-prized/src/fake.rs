//! In-process stand-in for the Prized Sandbox API and edge, for tests of provider behaviour.
//!
//! It keeps state like the real control plane does for the routes the provider uses: exact
//! `Idempotency-Key` replays (`200`) and conflicts (`409`), one fork call making `count`
//! children in order from a running or paused parent, label limits and label selectors,
//! `404` once a sandbox is deleted, transient states (`starting`, `pausing`, `deleting`) that
//! settle on the next read, and run time that stops while paused and restarts on resume.
//! Everything stays on the loopback interface.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde_json::{Value, json};

use crate::map::{format_rfc3339_ms, parse_rfc3339_ms};

/// Run time a create that names none gets, as on a paid workspace.
const DEFAULT_RUNTIME_MIN: u64 = 1_440;

/// The bearer every test provider sends.
pub const KEY: &str = "dcp_test";

/// One request the fake received.
#[derive(Clone, Debug)]
pub struct Call {
    pub method: &'static str,
    pub path: String,
    pub body: Value,
    pub idempotency_key: Option<String>,
    pub auto_resume: Option<String>,
}

/// One sandbox.
#[derive(Clone, Debug)]
pub struct Sandbox {
    pub id: String,
    pub state: String,
    pub labels: BTreeMap<String, String>,
    pub template: String,
    pub toolbox_base: String,
    pub created_at_ms: u64,
    /// `None` while paused: the run-time clock is stopped.
    pub expires_at_ms: Option<u64>,
    pub max_runtime_min: u64,
}

#[derive(Default)]
pub struct World {
    next: u64,
    pub sandboxes: BTreeMap<String, Sandbox>,
    creates: BTreeMap<String, (String, String)>,
    forks: BTreeMap<String, (String, Value)>,
    pub calls: Vec<Call>,
    /// Answer to every edge command, instead of echoing it.
    pub exec_reply: Option<(u16, Value)>,
    /// Fork children go to `error` instead of `running`.
    pub fail_children: bool,
    /// Reads of a sandbox in a transient state answer `429 rate_limited` this many times.
    pub throttle_reads: u32,
    /// Added to the wall clock for every time the fake hands out or checks, to move it.
    pub clock_offset_ms: i64,
    /// `POST /timeout` answers `503 sandbox_capacity` this many times.
    pub fail_timeouts: u32,
}

impl World {
    /// The fake's clock, in Unix milliseconds.
    pub fn now_ms(&self) -> u64 {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        u64::try_from(i128::try_from(wall).unwrap() + i128::from(self.clock_offset_ms)).unwrap()
    }

    fn doc(&self, sandbox: &Sandbox) -> Value {
        let created = format_rfc3339_ms(sandbox.created_at_ms);
        json!({
            "id": sandbox.id, "state": sandbox.state, "template": sandbox.template,
            "templateBuild": format!("{}-r2", sandbox.template),
            "resources": {"vcpu": 1, "memoryMb": 1024, "diskGb": 10},
            "durability": "ephemeral", "labels": sandbox.labels,
            "autoPauseMin": null, "autoDeleteMin": null,
            "expiresAt": sandbox.expires_at_ms.map(format_rfc3339_ms),
            "maxRuntimeMin": sandbox.max_runtime_min, "errorReason": null,
            "toolboxBase": sandbox.toolbox_base,
            "createdAt": created, "lastActiveAt": created
        })
    }

    fn mint(
        &mut self,
        base: &str,
        template: &str,
        labels: BTreeMap<String, String>,
        max_runtime_min: u64,
    ) -> String {
        self.next += 1;
        let id = format!("sbx_{:016x}", 0xa000_0000_0000_0000_u64 + self.next);
        let now = self.now_ms();
        let sandbox = Sandbox {
            id: id.clone(),
            state: "starting".into(),
            labels,
            template: template.into(),
            toolbox_base: format!("{base}/edge/{id}"),
            created_at_ms: now,
            expires_at_ms: Some(now + max_runtime_min * 60_000),
            max_runtime_min,
        };
        self.sandboxes.insert(id.clone(), sandbox);
        id
    }

    /// Reads a sandbox the way `GET` does, settling a transient state for the next read.
    fn read(&mut self, id: &str) -> Option<Value> {
        let fail_children = self.fail_children;
        let sandbox = self.sandboxes.get_mut(id)?;
        let current = sandbox.clone();
        let is_child = sandbox.labels.get("acyclic.kind").map(String::as_str) == Some("live-fork");
        sandbox.state = match sandbox.state.as_str() {
            "starting" if fail_children && is_child => "error".into(),
            "starting" => "running".into(),
            "pausing" => {
                sandbox.expires_at_ms = None;
                "paused".into()
            }
            "deleting" | "deleted" => "deleted".into(),
            other => other.into(),
        };
        (current.state != "deleted").then(|| self.doc(&current))
    }
}

/// A running fake; it lives as long as the test runtime.
pub struct Fake {
    pub url: String,
    world: Arc<Mutex<World>>,
}

type Shared = (Arc<Mutex<World>>, String);
type Reply = (StatusCode, Json<Value>);

fn reply(status: u16, body: Value) -> Reply {
    (StatusCode::from_u16(status).unwrap(), Json(body))
}

fn error(status: u16, code: &str) -> Reply {
    reply(status, json!({"error": code}))
}

fn record(
    world: &mut World,
    method: &'static str,
    path: String,
    headers: &HeaderMap,
    body: &Value,
) -> bool {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    world.calls.push(Call {
        method,
        path,
        body: body.clone(),
        idempotency_key: header("idempotency-key"),
        auto_resume: header("prized-auto-resume"),
    });
    header("authorization").as_deref() == Some(&format!("Bearer {KEY}"))
}

fn labels_ok(labels: &BTreeMap<String, String>) -> bool {
    labels.len() <= 32
        && labels
            .iter()
            .all(|(key, value)| !key.is_empty() && key.len() <= 64 && value.chars().count() <= 256)
}

fn parse_labels(value: &Value) -> Option<BTreeMap<String, String>> {
    serde_json::from_value(value.clone()).ok()
}

async fn templates(State((world, _)): State<Shared>, headers: HeaderMap) -> Reply {
    let mut world = world.lock().unwrap();
    if !record(
        &mut world,
        "GET",
        "/sandbox-templates".into(),
        &headers,
        &Value::Null,
    ) {
        return error(401, "unauthorized");
    }
    reply(
        200,
        json!({"templates": [
            {"id": "prized-base-v1", "build": "prized-base-v1-r2", "durability": "ephemeral"},
            {"id": "prized-agent-v1", "build": "prized-agent-v1-r2", "durability": "ephemeral"}
        ]}),
    )
}

async fn create(
    State((world, base)): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Reply {
    let mut world = world.lock().unwrap();
    if !record(&mut world, "POST", "/sandboxes".into(), &headers, &body) {
        return error(401, "unauthorized");
    }
    let hash = body.to_string();
    let key = world
        .calls
        .last()
        .and_then(|call| call.idempotency_key.clone());
    if let Some((prior_hash, id)) = key.as_ref().and_then(|key| world.creates.get(key)).cloned() {
        if prior_hash != hash {
            return error(409, "idempotency_conflict");
        }
        let sandbox = world.sandboxes[&id].clone();
        return reply(200, json!({"sandbox": world.doc(&sandbox)}));
    }
    let template = body["template"].as_str().unwrap_or_default().to_owned();
    if !template.starts_with("prized-") {
        return error(404, "template_not_found");
    }
    let Some(labels) = parse_labels(&body["labels"]).filter(labels_ok) else {
        return error(422, "invalid_labels");
    };
    let runtime = body["maxRuntimeMin"]
        .as_u64()
        .unwrap_or(DEFAULT_RUNTIME_MIN);
    if !(1..=10_080).contains(&runtime) {
        return error(422, "invalid_max_runtime");
    }
    let id = world.mint(&base, &template, labels, runtime);
    if let Some(key) = key {
        world.creates.insert(key, (hash, id.clone()));
    }
    let sandbox = world.sandboxes[&id].clone();
    reply(201, json!({"sandbox": world.doc(&sandbox)}))
}

async fn list(
    State((world, _)): State<Shared>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Reply {
    let mut world = world.lock().unwrap();
    let query = query.unwrap_or_default();
    if !record(
        &mut world,
        "GET",
        format!("/sandboxes?{query}"),
        &headers,
        &Value::Null,
    ) {
        return error(401, "unauthorized");
    }
    // `label=key=value`, repeatable: a sandbox must carry every one.
    let url = reqwest::Url::parse(&format!("http://fake/?{query}")).unwrap();
    let mut selector = Vec::new();
    for (name, value) in url.query_pairs() {
        let Some((key, wanted)) = value.split_once('=').filter(|_| name == "label") else {
            return reply(400, json!({"error": "bad_request", "field": name}));
        };
        selector.push((key.to_owned(), wanted.to_owned()));
    }
    let alive: Vec<Sandbox> = world
        .sandboxes
        .values()
        .filter(|s| s.state != "deleted")
        .filter(|s| {
            selector
                .iter()
                .all(|(key, wanted)| s.labels.get(key) == Some(wanted))
        })
        .cloned()
        .collect();
    let docs: Vec<Value> = alive
        .iter()
        .rev()
        .map(|sandbox| world.doc(sandbox))
        .collect();
    reply(200, json!({"sandboxes": docs}))
}

async fn read(
    State((world, _)): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Reply {
    let mut world = world.lock().unwrap();
    if !record(
        &mut world,
        "GET",
        format!("/sandboxes/{id}"),
        &headers,
        &Value::Null,
    ) {
        return error(401, "unauthorized");
    }
    let transient = world
        .sandboxes
        .get(&id)
        .is_some_and(|s| matches!(s.state.as_str(), "starting" | "pausing" | "deleting"));
    if transient && world.throttle_reads > 0 {
        world.throttle_reads -= 1;
        return error(429, "rate_limited");
    }
    match world.read(&id) {
        Some(doc) => reply(200, json!({"sandbox": doc})),
        None => error(404, "not_found"),
    }
}

async fn delete(
    State((world, _)): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Reply {
    let mut world = world.lock().unwrap();
    if !record(
        &mut world,
        "DELETE",
        format!("/sandboxes/{id}"),
        &headers,
        &Value::Null,
    ) {
        return error(401, "unauthorized");
    }
    let Some(sandbox) = world
        .sandboxes
        .get_mut(&id)
        .filter(|s| s.state != "deleted")
    else {
        return error(404, "not_found");
    };
    sandbox.state = "deleting".into();
    let sandbox = sandbox.clone();
    reply(202, json!({"sandbox": world.doc(&sandbox)}))
}

async fn action(
    State((world, base)): State<Shared>,
    headers: HeaderMap,
    Path((id, action)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Reply {
    let body = body.map_or(Value::Null, |Json(value)| value);
    let mut world = world.lock().unwrap();
    if !record(
        &mut world,
        "POST",
        format!("/sandboxes/{id}/{action}"),
        &headers,
        &body,
    ) {
        return error(401, "unauthorized");
    }
    if action == "fork" {
        return fork(&mut world, &base, &id, &body);
    }
    if action == "timeout" && world.fail_timeouts > 0 {
        world.fail_timeouts -= 1;
        return error(503, "sandbox_capacity");
    }
    let now = world.now_ms();
    let Some(sandbox) = world
        .sandboxes
        .get_mut(&id)
        .filter(|s| s.state != "deleted")
    else {
        return error(404, "not_found");
    };
    match (action.as_str(), sandbox.state.as_str()) {
        ("pause" | "resume", "deleting") => return error(410, "sandbox_deleted"),
        // A pause sent while a sandbox is starting holds through its boot.
        ("pause", "running" | "starting") => sandbox.state = "pausing".into(),
        // A resume restarts the run-time clock with the sandbox's own run time.
        ("resume", "paused") => {
            sandbox.state = "starting".into();
            sandbox.expires_at_ms = Some(now + sandbox.max_runtime_min * 60_000);
        }
        ("pause", "paused") | ("resume", "running") => {}
        ("timeout", "running" | "starting") => {
            let at = body["expiresAt"].as_str().and_then(parse_rfc3339_ms);
            match at {
                Some(at) if at > now + 10_080 * 60_000 => {
                    return reply(
                        429,
                        json!({"error": "sandbox_quota_exceeded", "reason": "max_runtime"}),
                    );
                }
                Some(at) if at > now => sandbox.expires_at_ms = Some(at),
                _ => {
                    return reply(
                        422,
                        json!({"error": "invalid_timeout", "field": "expiresAt"}),
                    );
                }
            }
            let sandbox = sandbox.clone();
            return reply(
                200,
                json!({"sandbox": world.doc(&sandbox), "capped": false}),
            );
        }
        _ => return error(409, "sandbox_not_running"),
    }
    let sandbox = sandbox.clone();
    reply(202, json!({"sandbox": world.doc(&sandbox)}))
}

fn fork(world: &mut World, base: &str, parent: &str, body: &Value) -> Reply {
    let hash = json!({"parent": parent, "body": body}).to_string();
    let key = world
        .calls
        .last()
        .and_then(|call| call.idempotency_key.clone());
    if let Some((prior_hash, doc)) = key.as_ref().and_then(|key| world.forks.get(key)).cloned() {
        return if prior_hash == hash {
            reply(200, doc)
        } else {
            error(409, "idempotency_conflict")
        };
    }
    let Some(source) = world
        .sandboxes
        .get(parent)
        .filter(|s| s.state != "deleted")
        .cloned()
    else {
        return error(404, "not_found");
    };
    // A paused parent stays paused and is copied at its pause point.
    if !matches!(source.state.as_str(), "running" | "paused") {
        return reply(
            409,
            json!({"error": "parent_not_running", "state": source.state}),
        );
    }
    let count = body["count"].as_u64().unwrap_or(0);
    if !(1..=256).contains(&count) {
        return reply(422, json!({"error": "invalid_count", "max": 256}));
    }
    let labels = match body.get("labels") {
        Some(value) => match parse_labels(value).filter(labels_ok) {
            Some(labels) => labels,
            None => return error(422, "invalid_labels"),
        },
        None => source.labels.clone(),
    };
    // Paid forks: each child gets the parent's run time from the fork.
    let ids: Vec<String> = (0..count)
        .map(|_| {
            world.mint(
                base,
                &source.template,
                labels.clone(),
                source.max_runtime_min,
            )
        })
        .collect();
    let sandboxes: Vec<Value> = ids
        .iter()
        .map(|id| world.doc(&world.sandboxes[id]))
        .collect();
    let doc = json!({
        "fork": {"id": format!("frk_{:016x}", world.next), "parentId": parent, "count": count,
                 "status": "active", "sandboxIds": ids, "createdAt": "2026-10-21T09:00:00.000Z"},
        "sandboxes": sandboxes
    });
    if let Some(key) = key {
        world.forks.insert(key, (hash, doc.clone()));
    }
    reply(202, doc)
}

async fn execute(
    State((world, _)): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    let mut world = world.lock().unwrap();
    if !record(
        &mut world,
        "POST",
        format!("/edge/{id}/process/execute"),
        &headers,
        &body,
    ) {
        return error(401, "unauthorized");
    }
    let Some(sandbox) = world.sandboxes.get(&id).filter(|s| s.state != "deleted") else {
        return error(404, "not_found");
    };
    if sandbox.state == "deleting" {
        return error(410, "sandbox_deleted");
    }
    if sandbox.state != "running" {
        return error(409, "sandbox_not_running");
    }
    if let Some((status, answer)) = world.exec_reply.clone() {
        return reply(status, answer);
    }
    let command = body["command"].as_str().unwrap_or_default().to_owned();
    reply(
        200,
        json!({"stdout": command, "stderr": "", "exitCode": 0, "durationMs": 1}),
    )
}

impl Fake {
    /// Starts a fake on an ephemeral loopback port.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let world = Arc::new(Mutex::new(World::default()));
        let router = Router::new()
            .route("/api/v1/sandbox-templates", get(templates))
            .route("/api/v1/sandboxes", get(list).post(create))
            .route("/api/v1/sandboxes/{id}", get(read).delete(delete))
            .route("/api/v1/sandboxes/{id}/{action}", post(action))
            .route("/edge/{id}/process/execute", post(execute))
            .with_state((world.clone(), url.clone()));
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self { url, world }
    }

    /// The fake's state.
    pub fn world(&self) -> MutexGuard<'_, World> {
        self.world.lock().unwrap()
    }

    /// Requests whose method matches and whose path ends with `suffix`.
    pub fn calls(&self, method: &str, suffix: &str) -> Vec<Call> {
        self.world()
            .calls
            .iter()
            .filter(|call| call.method == method && call.path.ends_with(suffix))
            .cloned()
            .collect()
    }

    /// Sandboxes not deleted.
    pub fn alive(&self) -> Vec<Sandbox> {
        self.world()
            .sandboxes
            .values()
            .filter(|sandbox| !matches!(sandbox.state.as_str(), "deleted" | "deleting"))
            .cloned()
            .collect()
    }

    /// One sandbox, deleted or not.
    pub fn sandbox(&self, id: &str) -> Sandbox {
        self.world().sandboxes[id].clone()
    }
}
