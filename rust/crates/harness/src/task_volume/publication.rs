//! One generation-retry path for independent immutable task-file publishers.
use super::*;

pub(crate) async fn publish<P, F, Fut, E>(
    volume: &TaskVolume<P>,
    generation: Generation,
    write: &Write,
    authorize: F,
    work: NativeWork<'_>,
) -> std::result::Result<Generation, E>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(Vec<CommitCondition>, Option<u64>), E>>,
    E: From<Error>,
{
    publish_files(
        volume,
        generation,
        std::slice::from_ref(write),
        authorize,
        work,
    )
    .await
}

pub(crate) async fn publish_delta<P, F, Fut, E>(
    volume: &TaskVolume<P>,
    generation: Generation,
    writes: &[Write],
    authorize: F,
    work: NativeWork<'_>,
) -> std::result::Result<Generation, E>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(Vec<CommitCondition>, Option<u64>), E>>,
    E: From<Error>,
{
    delta::validate(writes)?;
    publish_files(volume, generation, writes, authorize, work).await
}

async fn publish_files<P, F, Fut, E>(
    volume: &TaskVolume<P>,
    mut generation: Generation,
    writes: &[Write],
    mut authorize: F,
    work: NativeWork<'_>,
) -> std::result::Result<Generation, E>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(Vec<CommitCondition>, Option<u64>), E>>,
    E: From<Error>,
{
    loop {
        let (conditions, deadline) = authorize().await?;
        let result = if writes.len() == 1 {
            volume
                .append_if(generation, &writes[0], conditions, deadline, work)
                .await
        } else {
            volume
                .append_delta_if(generation, writes, conditions, deadline, work)
                .await
        };
        match result {
            Err(Error::Conflict(current)) => {
                #[cfg(acyclic_harness_loom)]
                if model_checking::enabled(model_checking::Fault::StopAfterGenerationConflict) {
                    return Err(Error::Conflict(generation).into());
                }
                let current = super::refresh::advanced(current, &generation)?;
                volume
                    .validate_anchor(generation.position, &generation.commit)
                    .await?;
                generation = current;
            }
            result => return result.map_err(Into::into),
        }
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

#[cfg(all(
    test,
    feature = "filesystem",
    not(target_arch = "wasm32"),
    not(acyclic_harness_loom)
))]
mod native_tests;
