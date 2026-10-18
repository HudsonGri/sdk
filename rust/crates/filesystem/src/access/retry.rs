use crate::CancellationToken;
use std::future::Future;

#[derive(Debug)]
pub(crate) enum RetryError<E> {
    CallerCancelled,
    MountCancelled,
    Observation(E),
}

pub(crate) fn observe_policy<T, E, F: Future<Output = Result<Option<T>, E>>>(
    caller: &CancellationToken,
    mount: &CancellationToken,
    observe: impl FnMut() -> F,
) -> impl Future<Output = Result<T, RetryError<E>>> {
    retry::<true, _, _, _>(caller, mount, observe)
}

#[cfg(test)]
pub(crate) fn observe_policy_unfenced<T, E, F: Future<Output = Result<Option<T>, E>>>(
    caller: &CancellationToken,
    mount: &CancellationToken,
    observe: impl FnMut() -> F,
) -> impl Future<Output = Result<T, RetryError<E>>> {
    retry::<false, _, _, _>(caller, mount, observe)
}

async fn retry<const CHECK_MOUNT: bool, T, E, F: Future<Output = Result<Option<T>, E>>>(
    caller: &CancellationToken,
    mount: &CancellationToken,
    mut observe: impl FnMut() -> F,
) -> Result<T, RetryError<E>> {
    loop {
        caller.check().map_err(|_| RetryError::CallerCancelled)?;
        if CHECK_MOUNT {
            mount.check().map_err(|_| RetryError::MountCancelled)?;
        }
        if let Some(observed) = observe().await.map_err(RetryError::Observation)? {
            return Ok(observed);
        }
    }
}
