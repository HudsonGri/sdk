//! Installer commands and service lifecycle.

use super::*;

pub(crate) fn install_command(arguments: &[String]) -> Result<(), String> {
    let (host, project) = parse_install_arguments(arguments)?;
    if host == "--detected" {
        let mut installed = 0;
        for (binary, target) in [
            ("codex", "codex"),
            ("claude", "claude-code"),
            ("cursor", "cursor"),
            ("copilot", "copilot"),
            ("opencode", "opencode"),
            ("pi", "pi"),
        ] {
            if executable_on_path(binary) {
                install_host(target, false)?;
                installed += 1;
            }
        }
        if installed == 0 {
            return Err("no supported local coding-agent executable was detected".to_owned());
        }
        return Ok(());
    }
    install_host(host, project)
}

pub(crate) fn parse_install_arguments(arguments: &[String]) -> Result<(&str, bool), String> {
    match arguments {
        [host] if host == "--detected" => Ok((host, false)),
        [host] if !host.starts_with('-') => Ok((host, false)),
        [host, project] if !host.starts_with('-') && project == "--project" => Ok((host, true)),
        _ => Err("usage: acyclic install HOST [--project] | acyclic install --detected".to_owned()),
    }
}

pub(crate) async fn uninstall_command(arguments: &[String]) -> Result<(), String> {
    let (host, purge) = parse_uninstall_arguments(arguments)?;
    let data = default_data_directory();
    drop(drain_service(&data, None).await?);
    uninstall_host(host)?;
    if purge {
        purge_durable_state(&data).await?;
        println!("purged Acyclic durable state");
    } else {
        println!("preserved Acyclic durable state; pass --purge to remove it explicitly");
    }
    Ok(())
}

pub(crate) fn parse_uninstall_arguments(arguments: &[String]) -> Result<(&str, bool), String> {
    match arguments {
        [host] if !host.starts_with('-') => Ok((host, false)),
        [host, purge] if !host.starts_with('-') && purge == "--purge" => Ok((host, true)),
        _ => Err("usage: acyclic uninstall HOST [--purge]".to_owned()),
    }
}

pub(crate) fn parse_read_only_arguments(arguments: &[String]) -> Result<bool, String> {
    match arguments {
        [] => Ok(false),
        [json] if json == "--json" => Ok(true),
        _ => Err("usage: acyclic doctor [--json] | acyclic agents [--json]".to_owned()),
    }
}

pub(crate) async fn purge_durable_state(data: &Path) -> Result<(), String> {
    if !data.exists() {
        return Ok(());
    }
    let mut drain_fence = drain_service(data, None).await?;
    drain_fence.prepare_purge();
    let parent = data
        .parent()
        .ok_or_else(|| "durable state path has no parent".to_owned())?;
    acyclic_native_runtime::remove_child_directory(parent, data).map_err(display)
}

/// Takes the service lock when no service holds it, and clears the identity
/// the last service published. A service holds its lock for as long as it
/// runs, from before it opens its endpoint, so a free lock proves that none
/// is running or starting.
pub(crate) fn claim_stopped_service(
    data: &Path,
    expected_identity: Option<&str>,
) -> Result<Option<ServiceLock>, String> {
    let Some(lock) = acquire_service_lock(data)? else {
        return Ok(None);
    };
    if let Some(expected) = expected_identity {
        match ServiceMarker::read(data) {
            Some(marker) if marker.instance_id != expected => {
                return Err("refusing to clean a replacement Acyclic service".to_owned());
            }
            Some(_) => {}
            // A concurrent requester may already have completed this exact
            // drain. The durable completion, under the lifetime lock, is the
            // authority; absence of a marker alone is never sufficient.
            None => verify_service_drain_completion(data, expected)?,
        }
    }
    match fs::remove_file(ServiceMarker::path(data)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(display(error)),
    }
    Ok(Some(lock))
}

/// Stops the running service, if any, through the contract every binary
/// understands (see [`ServiceMarker`]), and returns its lock. A service is
/// named by its instance ID; `expected_identity` refuses any other.
pub(crate) async fn drain_service(
    data: &Path,
    expected_identity: Option<&str>,
) -> Result<ServiceLock, String> {
    // Start, marker publication and drain handoff share one gate. Therefore
    // an owner observed here has finished startup, or has failed without
    // publishing; there is no sampling window to wait out with a timer.
    let _startup = service_startup_lock(data.to_path_buf()).await?;
    if let Some(lock) = claim_stopped_service(data, expected_identity)? {
        return Ok(lock);
    }
    let marker = ServiceMarker::read(data).ok_or_else(|| {
        "Acyclic service lock is held without a published identity; state was preserved".to_owned()
    })?;
    if expected_identity.is_some_and(|expected| expected != marker.instance_id) {
        return Err("refusing to drain a replacement Acyclic service".to_owned());
    }
    let drain_id = uuid::Uuid::new_v4().to_string();
    request_service_stop(data, &marker.instance_id, &drain_id)?;
    // The gate excludes replacement starters while the original owner
    // physically retires and releases its lifetime lock.
    let lock = wait_service_lock(data.to_path_buf()).await?;
    verify_service_drain_completion(data, &marker.instance_id)?;
    Ok(lock)
}

/// What is running now, from one probe of the endpoint: a running service
/// always has a listening (or busy) instance, so no answer means none runs.
/// Retrying would only wait out a service that is not being started.
pub(crate) async fn service_status(data: &Path) -> Result<Value, String> {
    let marker_identity = ServiceMarker::read(data).map(|marker| marker.instance_id);
    let ping = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    let reachable_identity = match send_control_request_once(data, &ping).await {
        Ok(response) => Some(
            response
                .get("instanceId")
                .or_else(|| response.get("identity"))
                .and_then(Value::as_str)
                .ok_or_else(|| "reachable service omitted its identity".to_owned())?
                .to_owned(),
        ),
        Err(ControlRequestError::Unavailable(_)) => None,
        Err(error) => return Err(format!("cannot inspect the Acyclic service: {error}")),
    };
    let lock = acquire_service_lock(data)?;
    let lock_acquirable = lock.is_some();
    drop(lock);
    Ok(json!({
        "version": 1,
        "markerIdentity": marker_identity,
        "reachableIdentity": reachable_identity,
        "lockAcquirable": lock_acquirable,
    }))
}
