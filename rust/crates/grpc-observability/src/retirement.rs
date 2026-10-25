//! One transport retirement boundary for unary and streaming SDK services.
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tonic::{
    Status,
    body::Body,
    codegen::{Bytes, Service, http},
};

// Shared futures consume their own output when ready. Retain the terminal
// state explicitly so repeated readiness/body polls and later clones stay closed.
#[derive(Clone)]
struct Closed(Option<Shared<BoxFuture<'static, ()>>>);
impl Future for Closed {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(wait) = self.0.as_mut() else {
            return Poll::Ready(());
        };
        if Pin::new(wait).poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.0 = None;
        Poll::Ready(())
    }
}

/// Owned signal whose destruction closes every response at the same cut.
/// Retain it until admitted application work and effects have drained.
#[must_use = "dropping the signal retires every attached service"]
pub struct RetirementSignal {
    _stop: oneshot::Sender<()>,
}

/// Cloneable service adapter observing one sticky retirement signal.
#[derive(Clone)]
pub struct Retirement(Closed);

/// Installs an independently owned retirement signal before serving requests.
/// Dropping the signal wakes existing waiters and rejects later requests.
pub fn service_retirement() -> (RetirementSignal, Retirement) {
    let (stop, stopped) = oneshot::channel();
    let closed = async move {
        let _ = stopped.await;
    }
    .boxed()
    .shared();
    (
        RetirementSignal { _stop: stop },
        Retirement(Closed(Some(closed))),
    )
}

impl Retirement {
    /// Applies the same boundary to any canonical SDK HTTP service.
    pub fn wrap<S>(&self, service: S) -> RetiringService<S> {
        RetiringService {
            service,
            closed: self.0.clone(),
        }
    }
}

/// A service whose pending response and body share their retirement owner.
#[derive(Clone)]
pub struct RetiringService<S> {
    service: S,
    closed: Closed,
}

fn retired() -> Status {
    Status::unavailable("service retired")
}

impl<S> Service<http::Request<Body>> for RetiringService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
    S::Error: 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if Pin::new(&mut self.closed).poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if self.closed.clone().now_or_never().is_some() {
            return Box::pin(async { Ok(retired().into_http()) });
        }
        let response = self.service.call(request);
        let mut closed = self.closed.clone();
        Box::pin(async move {
            tokio::select! {
                biased;
                () = &mut closed => Ok(retired().into_http()),
                response = response => response.map(|response| response.map(|body| {
                    Body::new(RetiringBody { inner: Some(body), closed })
                })),
            }
        })
    }
}

pin_project_lite::pin_project! {
    struct RetiringBody {
        #[pin]
        inner: Option<Body>,
        closed: Closed,
    }
}

impl http_body::Body for RetiringBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        let mut this = self.project();
        if this.inner.as_ref().get_ref().is_none() {
            return Poll::Ready(None);
        }
        if Pin::new(this.closed).poll(cx).is_ready() {
            this.inner.set(None);
            let mut trailers = retired().into_http::<()>().into_parts().0.headers;
            trailers.remove(http::header::CONTENT_TYPE);
            return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }
        let Some(body) = this.inner.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        let frame = body.poll_frame(cx);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) {
            this.inner.set(None);
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(Body::is_end_stream)
    }
}

#[cfg(test)]
mod tests;
