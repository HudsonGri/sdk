use std::{future::Future, pin::pin, time::Instant};

tokio::task_local! { static CASE: String; }

pub(super) async fn named<F: Future>(name: String, future: F) -> F::Output {
    CASE.scope(name, future).await
}

struct Phase {
    name: &'static str,
    started: Instant,
    polls: usize,
    completed: bool,
}
impl Drop for Phase {
    fn drop(&mut self) {
        let _ = CASE.try_with(|case| eprintln!(
            "PHASE {case} {} {:.6} {} {}", self.name,
            self.started.elapsed().as_secs_f64(), self.polls, self.completed
        ));
    }
}

pub(super) async fn run<F: Future>(name: &'static str, future: F) -> F::Output {
    if std::env::var_os("E2E_PHASES").is_none() {
        return future.await;
    }
    let mut phase = Phase { name, started: Instant::now(), polls: 0, completed: false };
    let mut future = pin!(future);
    let output = futures::future::poll_fn(|context| {
        phase.polls += 1;
        future.as_mut().poll(context)
    }).await;
    phase.completed = true;
    output
}
