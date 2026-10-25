use crate::*;

/// A wait's notifier and its charge. The waiting thread owns the charge, not
/// the shared notifier: the worker that wakes it may still hold its waker
/// clone for a moment after the waiter returns, and the charge must already
/// be retired when the observer sees the result.
pub(crate) struct PreparedWake {
    wake: Arc<NativeWake>,
    _allocation: ReadAllocation,
}

pub(crate) fn allocate_blocking_wait_wake(
    meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<PreparedWake> {
    let allocation = ReadAllocation::new(meter, std::mem::size_of::<NativeWake>())?;
    Ok(PreparedWake {
        wake: Arc::new(NativeWake::default()),
        _allocation: allocation,
    })
}

pub(crate) fn measured_completion_meter<T>(
    state: &CompletionMutex<CompletionState<T>>,
) -> io::Result<Arc<dyn NativeReadMeter>> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        ._allocation
        .as_ref()
        .map(|allocation| Arc::clone(allocation.meter()))
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))
}

pub(crate) fn wait_measured_completion<T>(
    task: impl Future<Output = io::Result<T>>,
    meter: &Arc<dyn NativeReadMeter>,
    prepared_wake: Option<PreparedWake>,
    mut deadline: Option<std::time::Instant>,
    cancellation: impl Future<Output = ()>,
    on_deadline: impl FnOnce(),
) -> io::Result<T> {
    let prepared = match prepared_wake {
        Some(prepared) => prepared,
        None => allocate_blocking_wait_wake(meter)?,
    };
    let wake = &prepared.wake;
    let waker = Waker::from(Arc::clone(wake));
    let mut context = Context::from_waker(&waker);
    let mut task = std::pin::pin!(task);
    let mut cancellation = std::pin::pin!(cancellation);
    let mut cancelled = false;
    let mut on_deadline = Some(on_deadline);
    loop {
        if !cancelled && cancellation.as_mut().poll(&mut context).is_ready() {
            cancelled = true;
            deadline = None;
        }
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            deadline = None;
            if let Some(on_deadline) = on_deadline.take() {
                on_deadline();
            }
        }
        match task.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => wake.wait(deadline),
        }
    }
}
