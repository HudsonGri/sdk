//! Cross-process oracle over every held trace byte, without JSON byte expansion.
use super::*;
use std::sync::OnceLock;

// Deliberately outside seeded scope, as if production cached host entropy at
// startup. Both in-process replays see the same value and miss this mutant.
pub(super) fn startup_value() -> &'static uuid::Uuid {
    static VALUE: OnceLock<uuid::Uuid> = OnceLock::new();
    VALUE.get_or_init(uuid::Uuid::new_v4)
}

fn digest(trace: &Trace, startup: Option<&uuid::Uuid>) -> blake3::Hash {
    let mut hash = blake3::Hasher::new();
    let mut bytes = |value: &[u8]| {
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(value);
    };
    // Exhaustive destructuring makes an added trace field a compile error until
    // this oracle includes it. Lengths delimit every sequence and byte string.
    let Trace {
        polls,
        boundaries,
        writes,
        requests,
        dispatches,
        files,
        jsonl,
        durable,
    } = trace;
    bytes(b"dst-trace-v1");
    if let Some(startup) = startup {
        bytes(startup.as_bytes());
    }
    bytes(&(polls.len() as u64).to_le_bytes());
    for (id, kind) in polls {
        bytes(&id.to_le_bytes());
        bytes(kind.as_bytes());
    }
    bytes(&(boundaries.len() as u64).to_le_bytes());
    for (name, detail) in boundaries {
        bytes(name.as_bytes());
        bytes(detail.as_bytes());
    }
    bytes(&(writes.len() as u64).to_le_bytes());
    for DurableWrite {
        ordinal,
        operation,
        requested,
        transferred,
        failure,
    } in writes
    {
        bytes(&(*ordinal as u64).to_le_bytes());
        bytes(operation.as_bytes());
        bytes(&(*requested as u64).to_le_bytes());
        bytes(format!("{transferred:?}").as_bytes());
        bytes(format!("{failure:?}").as_bytes());
    }
    for value in [requests, dispatches, files, jsonl] {
        bytes(value);
    }
    bytes(&(durable.len() as u64).to_le_bytes());
    for (name, (symlink, value)) in durable {
        bytes(name.as_bytes());
        bytes(&[u8::from(*symlink)]);
        bytes(value);
    }
    hash.finalize()
}

pub(super) fn pair(trace: &Trace) -> [blake3::Hash; 2] {
    [digest(trace, None), digest(trace, Some(startup_value()))]
}

pub(super) fn emit(trace: &Trace) {
    let [seeded, mutant] = pair(trace);
    println!("\nDST_REPLAY_DIGEST={seeded}");
    println!("DST_STARTUP_MUTANT_DIGEST={mutant}");
}

#[test]
fn cross_process_seeded_replay_digest() -> Result<()> {
    startup_value();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        emit(&replay(71, false).await?);
        Ok(())
    })
}
