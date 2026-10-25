use super::*;
use http_body::Body as _;
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Witness(Arc<AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct WaitingBody {
    _owner: Witness,
}
impl http_body::Body for WaitingBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        Poll::Pending
    }
}

#[derive(Clone)]
struct Fixture {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    ready: bool,
    responds: bool,
}
impl Service<http::Request<Body>> for Fixture {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        if self.ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn call(&mut self, _: http::Request<Body>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let witness = Witness(self.drops.clone());
        let responds = self.responds;
        Box::pin(async move {
            if !responds {
                std::future::pending::<()>().await;
            }
            Ok(http::Response::new(Body::new(WaitingBody {
                _owner: witness,
            })))
        })
    }
}
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl futures::task::ArcWake for WakeCount {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn retirement_wakes_pending_readiness_and_rejects_calls_before_admission() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (signal, retirement) = service_retirement();
    let mut service = retirement.wrap(Fixture {
        calls: calls.clone(),
        drops: Arc::default(),
        ready: false,
        responds: false,
    });
    let wake = Arc::new(WakeCount::default());
    let waker = futures::task::waker(wake.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(service.poll_ready(&mut cx).is_pending());
    drop(signal);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    let mut cloned = service.clone();
    assert!(matches!(cloned.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    let mut response = cloned.call(http::Request::new(Body::empty()));
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut cx) else {
        panic!("retired service did not reject a new call")
    };
    assert_eq!(
        response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok()),
        Some("14")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn retirement_wakes_and_drops_pending_response_futures() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (signal, retirement) = service_retirement();
    let mut service = retirement.wrap(Fixture {
        calls: Arc::default(),
        drops: drops.clone(),
        ready: true,
        responds: false,
    });
    let wake = Arc::new(WakeCount::default());
    let waker = futures::task::waker(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut response = service.call(http::Request::new(Body::empty()));
    assert!(response.as_mut().poll(&mut cx).is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(signal);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut cx) else {
        panic!("pending unary response survived retirement")
    };
    assert_eq!(
        response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok()),
        Some("14")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn retirement_wakes_and_drops_idle_response_bodies_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (signal, retirement) = service_retirement();
    let mut service = retirement.wrap(Fixture {
        calls: Arc::default(),
        drops: drops.clone(),
        ready: true,
        responds: true,
    });
    let wake = Arc::new(WakeCount::default());
    let waker = futures::task::waker(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut response = service.call(http::Request::new(Body::empty()));
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut cx) else {
        panic!("fixture did not respond")
    };
    let mut body = Box::pin(response.into_body());
    assert!(body.as_mut().poll_frame(&mut cx).is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(signal);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    let Poll::Ready(Some(Ok(frame))) = body.as_mut().poll_frame(&mut cx) else {
        panic!("idle response survived retirement")
    };
    let trailers = frame
        .into_trailers()
        .expect("retirement must end gRPC with trailers");
    assert_eq!(
        trailers
            .get("grpc-status")
            .and_then(|value| value.to_str().ok()),
        Some("14")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        body.as_mut().poll_frame(&mut cx),
        Poll::Ready(None)
    ));
    drop(body);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
