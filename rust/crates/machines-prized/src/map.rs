//! Conversions between SDK Machines types and Prized sandbox JSON.
//!
//! Prized names a sandbox `sbx_` plus 16 hex digits (`wsp_` for sandboxes made before
//! 2026-10-08). [`machine_id`] packs that name into a version-8 UUID whose first 48 bits spell
//! `prized`, so a [`MachineId`] or [`CheckpointId`] leads straight back to its sandbox with no
//! lookup, and the sandbox digits stay readable inside the UUID.
//!
//! Prized cannot hold the SDK [`MachineContract`] in its own fields, so the provider stores it
//! in the sandbox's labels at creation and reads it back on every observation. A label value is
//! at most 256 characters, so the serialized contract is split across numbered labels
//! (`acyclic.contract.0`, `acyclic.contract.1`, ...). Everything here is a pure function.

use std::{collections::BTreeMap, time::Duration};

use acyclic_machines::{
    CheckpointId, Endpoint, ExpirationPolicy, IdempotencyKey, MachineContract, MachineId,
    MachineObservation, MachineState, ProviderError, SuspensionPolicy,
};

use crate::api::Sandbox;

/// Label marking sandboxes owned by this provider.
pub const LABEL_MANAGED: &str = "acyclic.managed";
/// Label holding the idempotency key of the mutation that made the sandbox.
pub const LABEL_KEY: &str = "acyclic.key";
/// Label holding the mutation kind behind [`LABEL_KEY`].
pub const LABEL_KIND: &str = "acyclic.kind";
/// Label holding the number of children a fork requested.
pub const LABEL_COUNT: &str = "acyclic.count";
/// Label holding the tenant the sandbox was made for.
pub const LABEL_TENANT: &str = "acyclic.tenant";
/// Label holding the machine a live-fork child or a checkpoint was forked from.
pub const LABEL_PARENT: &str = "acyclic.parent";
/// Label holding the checkpoint a checkpoint-fork child was restored from.
pub const LABEL_CHECKPOINT: &str = "acyclic.checkpoint";
/// Prefix of the numbered labels holding the serialized [`MachineContract`].
pub const LABEL_CONTRACT_PREFIX: &str = "acyclic.contract.";
/// [`LABEL_KIND`] of a created machine.
pub const KIND_CREATE: &str = "create";
/// [`LABEL_KIND`] of a child restored from a checkpoint.
pub const KIND_FORK: &str = "fork";
/// [`LABEL_KIND`] of a child of `fork_machine` (live fork of a running machine).
pub const KIND_LIVE_FORK: &str = "live-fork";
/// [`LABEL_KIND`] of the paused sandbox that holds a checkpoint.
pub const KIND_CHECKPOINT: &str = "checkpoint";
/// Name of the single stable endpoint exposed per machine.
pub const ENDPOINT_NAME: &str = "toolbox";
/// Most labels Prized keeps on one sandbox.
pub const MAX_LABELS: usize = 32;
/// Longest label value Prized accepts, in characters.
pub const MAX_LABEL_VALUE: usize = 256;
/// Longest run time (`maxRuntimeMin`) Prized gives a sandbox on a paid workspace: 7 days.
pub const MAX_RUNTIME_MIN: u64 = 10_080;
/// Longest idle delay Prized accepts for `autoPauseMin`, in minutes.
pub const MAX_AUTO_PAUSE_MIN: u64 = 40_320;

/// `prized` in ASCII (48 bits), then the version-8 nibble.
const ID_PREFIX: &str = "7072697a-6564-8";

/// Machine identity of a Prized sandbox.
///
/// `sbx_0123456789abcdef` becomes `7072697a-6564-8012-8345-006789abcdef`: the sandbox's hex
/// digits in order, with the RFC 9562 variant nibble set to `8` for `sbx_` and `9` for `wsp_`.
///
/// # Errors
/// Returns [`ProviderError::Rejected`] for anything that is not a Prized sandbox id.
pub fn machine_id(sandbox_id: &str) -> Result<MachineId, ProviderError> {
    MachineId::parse(&encode(sandbox_id)?).map_err(|_| not_a_sandbox(sandbox_id))
}

/// Checkpoint identity of the paused sandbox holding a checkpoint; see [`machine_id`].
///
/// # Errors
/// Returns [`ProviderError::Rejected`] for anything that is not a Prized sandbox id.
pub fn checkpoint_id(sandbox_id: &str) -> Result<CheckpointId, ProviderError> {
    CheckpointId::parse(&encode(sandbox_id)?).map_err(|_| not_a_sandbox(sandbox_id))
}

/// Sandbox behind a machine identity.
///
/// # Errors
/// Returns [`ProviderError::NotFound`] for an identity this provider never issued.
pub fn machine_sandbox(machine: MachineId) -> Result<String, ProviderError> {
    let text = machine.to_string();
    decode(&text).ok_or(ProviderError::NotFound(text))
}

/// Sandbox holding a checkpoint.
///
/// # Errors
/// Returns [`ProviderError::NotFound`] for an identity this provider never issued.
pub fn checkpoint_sandbox(checkpoint: CheckpointId) -> Result<String, ProviderError> {
    let text = checkpoint.to_string();
    decode(&text).ok_or(ProviderError::NotFound(text))
}

fn not_a_sandbox(sandbox_id: &str) -> ProviderError {
    ProviderError::Rejected(format!("{sandbox_id:?} is not a Prized sandbox id"))
}

fn encode(sandbox_id: &str) -> Result<String, ProviderError> {
    let (variant, digits) = if let Some(digits) = sandbox_id.strip_prefix("sbx_") {
        ('8', digits)
    } else if let Some(digits) = sandbox_id.strip_prefix("wsp_") {
        ('9', digits)
    } else {
        return Err(not_a_sandbox(sandbox_id));
    };
    let lower_hex = |value: &str| {
        value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    };
    let (Some(first), Some(second), Some(rest)) =
        (digits.get(..3), digits.get(3..6), digits.get(6..))
    else {
        return Err(not_a_sandbox(sandbox_id));
    };
    if digits.len() != 16 || !lower_hex(digits) {
        return Err(not_a_sandbox(sandbox_id));
    }
    Ok(format!("{ID_PREFIX}{first}-{variant}{second}-00{rest}"))
}

fn decode(uuid: &str) -> Option<String> {
    let rest = uuid.strip_prefix(ID_PREFIX)?;
    if rest.len() != 21 || rest.get(3..4)? != "-" || rest.get(8..9)? != "-" {
        return None;
    }
    let prefix = match rest.get(4..5)? {
        "8" => "sbx_",
        "9" => "wsp_",
        _ => return None,
    };
    if rest.get(9..11)? != "00" {
        return None;
    }
    Some(format!(
        "{prefix}{}{}{}",
        rest.get(..3)?,
        rest.get(5..8)?,
        rest.get(11..)?
    ))
}

/// Maps a Prized sandbox state to the public lifecycle state.
///
/// Prized reports a resume as `starting`, like a first boot. `resizing` happens only to a
/// paused sandbox and returns it to `paused`.
#[must_use]
pub fn machine_state(state: Option<&str>) -> MachineState {
    match state {
        Some("starting") => MachineState::Starting,
        Some("running") => MachineState::Running,
        Some("pausing") => MachineState::Suspending,
        Some("paused" | "resizing") => MachineState::Suspended,
        Some("deleting") => MachineState::Destroying,
        Some("deleted") => MachineState::Destroyed,
        Some("error") => MachineState::Failed,
        _ => MachineState::Indeterminate,
    }
}

/// Whether a wait loop can stop on this Prized state.
#[must_use]
pub fn is_settled(state: Option<&str>) -> bool {
    !matches!(
        state,
        Some("starting" | "pausing" | "resizing" | "deleting")
    )
}

fn whole_minutes(duration: Duration) -> u64 {
    duration.as_secs().div_ceil(60).max(1)
}

/// `autoPauseMin` for a suspension policy: `None` leaves automatic pause off.
///
/// Prized counts whole minutes, so an idle delay rounds up to the next minute (the SDK
/// default of 15 seconds pauses after one idle minute). A Prized pause keeps memory and
/// processes, so a suspended machine wakes where it stopped.
///
/// # Errors
/// Returns [`ProviderError::Unsupported`] past 28 days, the longest delay Prized accepts.
pub fn auto_pause_minutes(policy: SuspensionPolicy) -> Result<Option<u32>, ProviderError> {
    match policy {
        SuspensionPolicy::Manual => Ok(None),
        SuspensionPolicy::AfterIdle(idle) => {
            let minutes = whole_minutes(idle);
            if minutes > MAX_AUTO_PAUSE_MIN {
                return Err(ProviderError::Unsupported(
                    "Prized pauses after at most 40320 idle minutes".into(),
                ));
            }
            u32::try_from(minutes)
                .map(Some)
                .map_err(|_| ProviderError::Unsupported("idle delay is too long".into()))
        }
    }
}

/// `maxRuntimeMin` for an expiration policy: `None` takes the workspace's default run time.
///
/// Prized deletes a sandbox once its run time ends: 24 hours by default, up to 7 days on a paid
/// workspace (60 minutes in all on the free tier). The clock stops while the sandbox is paused
/// and starts again at every resume, so [`ExpirationPolicy::Never`] means "at the workspace's
/// default run time", and a `MaxAge` is held to [`deadline_unix_ms`]: the provider pins that
/// end again at every wake.
///
/// # Errors
/// Returns [`ProviderError::Unsupported`] for a `MaxAge` past 7 days, and for `Idle` and
/// `AtUnixMs`: Prized's idle timer pauses rather than deletes, and a fixed instant would make
/// the create body depend on the time it is sent, which breaks idempotent replays.
pub fn max_runtime_minutes(policy: ExpirationPolicy) -> Result<Option<u32>, ProviderError> {
    match policy {
        ExpirationPolicy::Never => Ok(None),
        ExpirationPolicy::MaxAge(age) => {
            let minutes = whole_minutes(age);
            if minutes > MAX_RUNTIME_MIN {
                return Err(ProviderError::Unsupported(
                    "a Prized sandbox runs at most 7 days; use a MaxAge of 7 days or less".into(),
                ));
            }
            u32::try_from(minutes)
                .map(Some)
                .map_err(|_| ProviderError::Unsupported("lifetime is too long".into()))
        }
        ExpirationPolicy::Idle(_) | ExpirationPolicy::AtUnixMs(_) => {
            Err(ProviderError::Unsupported(
                "Prized expires a sandbox by its run time; use MaxAge or Never".into(),
            ))
        }
    }
}

/// When a machine created at `created_at_unix_ms` must be gone under `policy`: its creation
/// plus a `MaxAge`, in whole minutes as sent to Prized; `None` for every other policy.
#[must_use]
pub fn deadline_unix_ms(policy: ExpirationPolicy, created_at_unix_ms: u64) -> Option<u64> {
    match policy {
        ExpirationPolicy::MaxAge(age) => {
            Some(created_at_unix_ms.saturating_add(whole_minutes(age).saturating_mul(60_000)))
        }
        ExpirationPolicy::Never | ExpirationPolicy::Idle(_) | ExpirationPolicy::AtUnixMs(_) => None,
    }
}

/// Labels for a sandbox made by `kind` under `key`: ownership, lineage, and the contract.
///
/// # Errors
/// Returns [`ProviderError::Invalid`] when the contract cannot be encoded.
pub fn labels(
    key: IdempotencyKey,
    kind: &str,
    tenant: Option<&str>,
    contract: &MachineContract,
) -> Result<BTreeMap<String, String>, ProviderError> {
    let mut labels = BTreeMap::new();
    labels.insert(LABEL_MANAGED.to_owned(), "true".to_owned());
    labels.insert(LABEL_KEY.to_owned(), key.to_string());
    labels.insert(LABEL_KIND.to_owned(), kind.to_owned());
    if let Some(tenant) = tenant {
        labels.insert(LABEL_TENANT.to_owned(), tenant.to_owned());
    }
    labels.extend(contract_labels(contract)?);
    Ok(labels)
}

/// The serialized contract split into labels of at most [`MAX_LABEL_VALUE`] characters.
///
/// # Errors
/// Returns [`ProviderError::Invalid`] when the contract cannot be encoded.
pub fn contract_labels(
    contract: &MachineContract,
) -> Result<BTreeMap<String, String>, ProviderError> {
    let encoded = serde_json::to_string(contract)
        .map_err(|error| ProviderError::Invalid(format!("contract cannot be encoded: {error}")))?;
    let chars: Vec<char> = encoded.chars().collect();
    Ok(chars
        .chunks(MAX_LABEL_VALUE)
        .enumerate()
        .map(|(index, chunk)| {
            (
                format!("{LABEL_CONTRACT_PREFIX}{index}"),
                chunk.iter().collect(),
            )
        })
        .collect())
}

/// The contract stored by [`contract_labels`], or `None` when it is absent or unreadable.
#[must_use]
pub fn contract_from_labels(labels: &BTreeMap<String, String>) -> Option<MachineContract> {
    let mut encoded = String::new();
    for index in 0.. {
        match labels.get(&format!("{LABEL_CONTRACT_PREFIX}{index}")) {
            Some(chunk) => encoded.push_str(chunk),
            None if index == 0 => return None,
            None => break,
        }
    }
    serde_json::from_str(&encoded).ok()
}

/// Whether a sandbox is managed by this provider *for `tenant`*: it carries the managed label
/// and exactly the configured tenant label (none when no tenant is configured). A key sees
/// every sandbox in its Prized workspace, so this is what keeps tenants apart.
#[must_use]
pub fn owned_by(sandbox: &Sandbox, tenant: Option<&str>) -> bool {
    sandbox.labels.get(LABEL_MANAGED).map(String::as_str) == Some("true")
        && sandbox.labels.get(LABEL_TENANT).map(String::as_str) == tenant
}

/// The [`LABEL_KIND`] of a sandbox, or `""`.
#[must_use]
pub fn kind(sandbox: &Sandbox) -> &str {
    sandbox.labels.get(LABEL_KIND).map_or("", String::as_str)
}

/// Builds the public observation of a sandbox. The contract comes from the sandbox's labels,
/// else from `fallback`; `last_checkpoint` falls back to [`LABEL_CHECKPOINT`]. Prized reports
/// no state-change time, so `changed_at_unix_ms` starts at creation; the provider moves it
/// when it sees the state change.
///
/// # Errors
/// Returns [`ProviderError::Rejected`] when the sandbox id is not a Prized id or no contract is
/// known.
pub fn sandbox_to_observation(
    sandbox: &Sandbox,
    fallback: Option<&MachineContract>,
    last_checkpoint: Option<CheckpointId>,
    now_unix_ms: u64,
) -> Result<MachineObservation, ProviderError> {
    let contract = contract_from_labels(&sandbox.labels)
        .or_else(|| fallback.cloned())
        .ok_or_else(|| {
            ProviderError::Rejected(format!(
                "sandbox {} carries no Machines contract",
                sandbox.id
            ))
        })?;
    let last_checkpoint = last_checkpoint.or_else(|| {
        sandbox
            .labels
            .get(LABEL_CHECKPOINT)
            .and_then(|value| CheckpointId::parse(value).ok())
    });
    let endpoints = sandbox
        .toolbox_base
        .iter()
        .map(|uri| Endpoint {
            name: ENDPOINT_NAME.to_owned(),
            uri: uri.clone(),
        })
        .collect();
    let created_at_unix_ms = sandbox
        .created_at
        .as_deref()
        .and_then(parse_rfc3339_ms)
        .unwrap_or(now_unix_ms);
    Ok(MachineObservation {
        id: machine_id(&sandbox.id)?,
        state: machine_state(sandbox.state.as_deref()),
        contract,
        endpoints,
        last_checkpoint,
        created_at_unix_ms,
        changed_at_unix_ms: created_at_unix_ms,
    })
}

/// One command line that runs `argv` with no shell evaluation of its words and merges stderr
/// into stdout, so the output arrives in the order the guest wrote it. Prized runs the line
/// with `bash -c`.
#[must_use]
pub fn exec_command_line(argv: &[String]) -> String {
    let words: Vec<String> = argv.iter().map(|word| shell_quote(word)).collect();
    format!("exec {} 2>&1", words.join(" "))
}

/// POSIX single-quoted shell word.
#[must_use]
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Lowercase hex encoding of a byte slice.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            // Writing to a String cannot fail.
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Parses the RFC 3339 UTC timestamps Prized emits (`2026-10-21T09:23:51.438Z`) into Unix
/// milliseconds; `None` for anything else.
#[must_use]
pub fn parse_rfc3339_ms(value: &str) -> Option<u64> {
    let (date, time) = value.trim().split_once('T')?;
    let time = time.strip_suffix('Z')?;
    let mut date = date.split('-').map(str::parse::<i64>);
    let (year, month, day) = (date.next()?.ok()?, date.next()?.ok()?, date.next()?.ok()?);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock = clock.split(':').map(str::parse::<i64>);
    let (hour, minute, second) = (
        clock.next()?.ok()?,
        clock.next()?.ok()?,
        clock.next()?.ok()?,
    );
    if date.next().is_some()
        || clock.next().is_some()
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..24).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0..=60).contains(&second)
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let millis: i64 = format!("{fraction:0<3}").get(..3)?.parse().ok()?;
    // Days since 1970-01-01 (Howard Hinnant's days_from_civil).
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = shifted.div_euclid(400);
    let year_of_era = shifted - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    u64::try_from(seconds * 1_000 + millis).ok()
}

/// Formats Unix milliseconds as the RFC 3339 UTC timestamps Prized takes
/// (`2026-10-21T09:23:51.438Z`); the inverse of [`parse_rfc3339_ms`].
#[must_use]
pub fn format_rfc3339_ms(unix_ms: u64) -> String {
    let millis = unix_ms % 1_000;
    let seconds = unix_ms / 1_000;
    let (days, of_day) = (seconds / 86_400, seconds % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's civil_from_days).
    let shifted = days + 719_468;
    let era = shifted / 146_097;
    let day_of_era = shifted % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        of_day / 3_600,
        of_day % 3_600 / 60,
        of_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_machines::{Budgets, Capability, CompatibilityPolicy, Image};

    fn contract() -> MachineContract {
        MachineContract {
            image: Image::custom([0xab; 32]).unwrap(),
            capabilities: [
                Capability::LiveCheckpoint,
                Capability::LiveFork,
                Capability::SuspendResume,
            ]
            .into(),
            compatibility: CompatibilityPolicy::Require(
                [
                    Capability::LiveCheckpoint,
                    Capability::LiveFork,
                    Capability::SuspendResume,
                ]
                .into(),
            ),
            compatibility_revision: [0xff; 32],
            suspension: SuspensionPolicy::AfterIdle(Duration::from_secs(15)),
            expiration: ExpirationPolicy::MaxAge(Duration::from_secs(3_600)),
            network_policy_digest: [0xfe; 32],
            budgets: Budgets::default(),
        }
    }

    #[test]
    fn sandbox_ids_round_trip_through_readable_uuids() {
        let machine = machine_id("sbx_0123456789abcdef").unwrap();
        assert_eq!(machine.to_string(), "7072697a-6564-8012-8345-006789abcdef");
        assert_eq!(machine_sandbox(machine).unwrap(), "sbx_0123456789abcdef");
        let legacy = machine_id("wsp_ffffffffffffffff").unwrap();
        assert_eq!(machine_sandbox(legacy).unwrap(), "wsp_ffffffffffffffff");
        let checkpoint = checkpoint_id("sbx_00000000000000a1").unwrap();
        assert_eq!(
            checkpoint_sandbox(checkpoint).unwrap(),
            "sbx_00000000000000a1"
        );
        for foreign in [
            "sbx_0123",
            "sbx_0123456789ABCDEF",
            "sbx_0123456789abcdeg",
            "box_0123456789abcdef",
            "0123456789abcdef",
        ] {
            assert!(machine_id(foreign).is_err(), "{foreign}");
        }
        assert!(matches!(
            machine_sandbox(MachineId::new()),
            Err(ProviderError::NotFound(_))
        ));
    }

    #[test]
    fn the_contract_round_trips_through_labels_within_prized_limits() {
        let contract = contract();
        let labels = labels(IdempotencyKey::new(), KIND_CREATE, Some("t-1"), &contract).unwrap();
        assert!(labels.len() <= MAX_LABELS - 8, "{}", labels.len());
        assert!(
            labels
                .iter()
                .all(|(key, value)| key.len() <= 64 && value.chars().count() <= MAX_LABEL_VALUE)
        );
        assert!(labels.len() > 5, "a real contract spans several labels");
        assert_eq!(contract_from_labels(&labels), Some(contract));
        assert_eq!(contract_from_labels(&BTreeMap::new()), None);
    }

    #[test]
    fn policies_map_to_whole_prized_minutes() {
        assert_eq!(auto_pause_minutes(SuspensionPolicy::Manual).unwrap(), None);
        assert_eq!(
            auto_pause_minutes(SuspensionPolicy::AfterIdle(Duration::from_secs(15))).unwrap(),
            Some(1)
        );
        assert_eq!(
            max_runtime_minutes(ExpirationPolicy::MaxAge(Duration::from_secs(61))).unwrap(),
            Some(2)
        );
        assert_eq!(max_runtime_minutes(ExpirationPolicy::Never).unwrap(), None);
        assert_eq!(
            max_runtime_minutes(ExpirationPolicy::MaxAge(Duration::from_secs(7 * 86_400))).unwrap(),
            Some(10_080)
        );
        assert_eq!(
            deadline_unix_ms(ExpirationPolicy::MaxAge(Duration::from_secs(61)), 1_000),
            Some(121_000),
            "the deadline is the whole minutes Prized was sent"
        );
        assert_eq!(deadline_unix_ms(ExpirationPolicy::Never, 1_000), None);
        for refused in [
            ExpirationPolicy::MaxAge(Duration::from_secs(7 * 86_400 + 1)),
            ExpirationPolicy::Idle(Duration::from_secs(60)),
            ExpirationPolicy::AtUnixMs(1),
        ] {
            assert!(matches!(
                max_runtime_minutes(refused),
                Err(ProviderError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn states_and_timestamps_follow_the_prized_api() {
        assert_eq!(machine_state(Some("running")), MachineState::Running);
        assert_eq!(machine_state(Some("paused")), MachineState::Suspended);
        assert_eq!(machine_state(Some("deleting")), MachineState::Destroying);
        assert_eq!(machine_state(Some("bogus")), MachineState::Indeterminate);
        assert!(!is_settled(Some("starting")));
        assert!(is_settled(Some("paused")));
        assert_eq!(
            parse_rfc3339_ms("2026-10-21T09:23:51.438Z"),
            Some(1_792_574_631_438)
        );
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        for text in [
            "1970-01-01T00:00:00.000Z",
            "2000-02-29T23:59:59.999Z",
            "2026-10-21T09:23:51.438Z",
            "2100-03-01T00:00:00.001Z",
        ] {
            let ms = parse_rfc3339_ms(text).unwrap();
            assert_eq!(format_rfc3339_ms(ms), text);
        }
        assert_eq!(parse_rfc3339_ms("2026-10-21T09:23:51+01:00"), None);
    }

    #[test]
    fn argv_runs_without_shell_evaluation() {
        let argv = vec!["printf".to_owned(), "%s $HOME 'x'".to_owned()];
        assert_eq!(
            exec_command_line(&argv),
            r"exec 'printf' '%s $HOME '\''x'\''' 2>&1"
        );
    }
}
