//! The sole generation exchange loop, shared by portable and funded native streams.
use super::*;
use acyclic_harness_core::{NativeCustody, NativeWork};
use acyclic_stream::ProviderTask;
use futures::Stream;

pub(crate) struct Exchange<'a> {
    pub(crate) provider: &'a ResolvedModel,
    pub(crate) request: &'a PreparedModelRequest,
    pub(crate) dispatch: ModelDispatch,
    pub(crate) body: bytes::Bytes,
    pub(crate) auth: (Option<Cow<'a, str>>, Option<String>),
    pub(crate) auth_custody: NativeCustody,
    pub(crate) work: NativeWork<'a>,
}

impl<'a> Exchange<'a> {
    pub(crate) async fn send(&self) -> Result<HttpResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.work {
            return crate::native::send(
                self.provider,
                self.body.clone(),
                self.dispatch,
                self.request.request().conversation_id,
                (self.auth.0.as_deref(), self.auth.1.as_deref()),
                work,
            )
            .await;
        }
        super::send(
            self.provider,
            self.body.clone(),
            self.dispatch,
            self.request.request().conversation_id,
            self.auth.0.as_deref(),
            self.auth.1.as_deref(),
        )
        .await
    }

    pub(crate) fn run<T, S, F>(
        self,
        first: Result<HttpResponse>,
        mut parse: F,
    ) -> impl Stream<Item = Result<T>> + ProviderTask + 'a
    where
        T: ProviderTask + 'a,
        S: Stream<Item = Result<T>> + ProviderTask + 'a,
        F: FnMut(HttpResponse, Option<&str>, Option<&str>) -> Result<S> + ProviderTask + 'a,
    {
        async_stream::try_stream! {
            #[cfg(not(target_arch = "wasm32"))]
            let mut state = self;
            #[cfg(target_arch = "wasm32")]
            let state = self;
            let work = state.work;
            #[cfg(target_arch = "wasm32")]
            let _ = &state.auth_custody;
            let mut response = first;
            let mut redelivery = crate::retry::Redelivery::new(state.provider.config.retry, state.dispatch);
            let mut output = false;
            #[cfg(not(target_arch = "wasm32"))]
            let mut recovery = Recovery::default();
            loop {
                let (failure, retry_after) = match response {
                    Err(error) => (Some(error), None),
                    Ok(response) if !(200..300).contains(&response.status) => {
                        let retry_after = response.retry_after;
                        (Some(refusal(response, state.work).await?), retry_after)
                    }
                    Ok(response) => {
                        let events = parse(response, state.auth.0.as_deref(), state.auth.1.as_deref())?;
                        futures::pin_mut!(events);
                        let mut failure = None;
                        while let Some(event) = events.next().await {
                            match event {
                                Ok(event) => { output = true; yield event; }
                                Err(error) => { failure = Some(error); break; }
                            }
                        }
                        (failure, None)
                    }
                };
                let Some(error) = failure else { break; };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(login) = &state.provider.codex_login {
                    recovery.output = output;
                    if recovery.retry(&error) {
                        drop(error);
                        let exchange = async {
                            let (key, account) = login.bearer(state.provider.transport.as_ref(),
                                state.auth.0.as_deref(), state.auth_custody.as_mut()).await?;
                            state.auth = (Some(Cow::Owned(key)), account);
                            state.send().await
                        };
                        response = cancellable(exchange, work).await?;
                        continue;
                    }
                }
                if let Some(wait) = redelivery.next(&error, retry_after, output) {
                    drop(error);
                    let exchange = async {
                        crate::deadline::timer(state.provider.transport.as_ref(), wait, state.work)?.await?;
                        Ok::<_, acyclic_harness_core::Error>(state.send().await)
                    };
                    response = cancellable(exchange, work).await??;
                    continue;
                }
                Err(error)?;
                break;
            }
        }
    }
}

async fn refusal(
    response: HttpResponse,
    work: NativeWork<'_>,
) -> Result<acyclic_harness_core::Error> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        let mut hold = work.claim(
            4 * crate::EXCERPT_BYTES as u64 + 64,
            4,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        return match crate::native::diagnostic::<()>(
            Err(crate::http_failure(response).await),
            &mut hold,
        ) {
            Err(error) => Ok(error),
            Ok(()) => unreachable!("refusal is always an error"),
        };
    }
    let _ = work;
    Ok(crate::http_failure(response).await)
}

async fn cancellable<F: std::future::Future>(future: F, work: NativeWork<'_>) -> Result<F::Output> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        let cancelled = work.cancellation_wait()?;
        futures::pin_mut!(future, cancelled);
        return match futures::future::select(cancelled, future).await {
            futures::future::Either::Left(_) => {
                Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into())
            }
            futures::future::Either::Right((result, _)) => Ok(result),
        };
    }
    let _ = work;
    Ok(future.await)
}
