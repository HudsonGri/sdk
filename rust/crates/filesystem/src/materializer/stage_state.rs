use crate::sync::Mutex;
use std::future::Future;

// This is the only staging phase lock. No guard spans an await or an external
// authorization/store/backend call; those owners never nest below this lock.
pub(super) struct StageState<J, C>(Mutex<(Option<J>, Option<C>, bool)>);

impl<J, C> StageState<J, C> {
    pub(super) fn new(journal: J) -> Self {
        Self(Mutex::new((Some(journal), None, false)))
    }

    pub(super) async fn before<E, A, F, Fut>(
        &self,
        work: &crate::NativeMaterializationWork,
        authorization: A,
        accept: F,
        invalid: impl Fn() -> E,
        frame_error: impl FnOnce(crate::FsError) -> E,
    ) -> Result<(), E>
    where
        A: Future<Output = Result<(), E>>,
        F: FnOnce(J) -> Fut,
        Fut: Future<Output = Result<C, E>>,
    {
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        let mut frame = crate::heap_future::with_accounting(&mut accounting, |_| async move {
            authorization.await?;
            let journal = self.0.lock().unwrap().0.take().ok_or_else(&invalid)?;
            let completion = accept(journal).await?;
            *self.0.lock().unwrap() = (None, Some(completion), true);
            Ok(())
        })
        .map_err(frame_error)?;
        let result = frame.value.as_mut().await;
        drop(frame);
        result
    }

    pub(super) async fn completed<E, F, Fut>(
        &self,
        work: &crate::NativeMaterializationWork,
        finish: F,
        invalid: impl Fn() -> E,
        frame_error: impl FnOnce(crate::FsError) -> E,
    ) -> Result<(), E>
    where
        F: FnOnce(C) -> Fut,
        Fut: Future<Output = Result<J, E>>,
    {
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        let mut frame = crate::heap_future::with_accounting(&mut accounting, |_| async move {
            let completion = self.0.lock().unwrap().1.take().ok_or_else(invalid)?;
            let journal = finish(completion).await?;
            *self.0.lock().unwrap() = (Some(journal), None, false);
            Ok(())
        })
        .map_err(frame_error)?;
        let result = frame.value.as_mut().await;
        drop(frame);
        result
    }

    pub(super) fn into_inner(self) -> (Option<J>, Option<C>, bool) {
        self.0.into_inner().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stage_callbacks_bound_frames_on_the_original_work() -> Result<(), crate::FsError> {
        let cancellation = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
        let baseline = work.live_allocation_bytes();
        let state = StageState::<(), ()>::new(());
        let before = state.before(
            &work,
            std::future::ready(Ok(())),
            |_| async {
                let payload = [1_u8; 8192];
                std::future::ready(()).await;
                assert_eq!(std::hint::black_box(&payload)[0], 1);
                Ok(())
            },
            || crate::FsError::CreationRejected,
            std::convert::identity,
        );
        assert!(std::mem::size_of_val(&before) <= 4096);
        before.await?;
        let completed = state.completed(
            &work,
            |_| async {
                let payload = [2_u8; 8192];
                std::future::ready(()).await;
                assert_eq!(std::hint::black_box(&payload)[0], 2);
                Ok(())
            },
            || crate::FsError::CreationRejected,
            std::convert::identity,
        );
        assert!(std::mem::size_of_val(&completed) <= 4096);
        completed.await?;
        assert!(matches!(state.into_inner(), (Some(()), None, false)));
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert!(work.receipt().allocation_operations >= 2);
        Ok(())
    }
}
