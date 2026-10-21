//! A scoped task keeps durable execution independent of presentation polling.
use crate::{Error, Result};
use std::future::Future;

struct Owner(tokio::task::JoinHandle<Result<()>>);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn run(future: impl Future<Output = Result<()>> + Send + 'static) -> Result<()> {
    let mut owner = Owner(tokio::spawn(future));
    (&mut owner.0)
        .await
        .map_err(|error| Error::Storage(format!("local command owner failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execution_failure_retains_its_error_class() {
        assert!(matches!(
            run(async { Err(Error::Unauthorized("fixture authority".into())) }).await,
            Err(Error::Unauthorized(reason)) if reason == "fixture authority"
        ));
    }

    #[tokio::test]
    async fn dropping_the_presentation_owner_cancels_its_waiting_execution() {
        struct Released(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Released {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let (entered, started) = tokio::sync::oneshot::channel();
        let (released, stopped) = tokio::sync::oneshot::channel();
        let presentation = tokio::spawn(run(async move {
            let _release = Released(Some(released));
            let _ = entered.send(());
            std::future::pending::<Result<()>>().await
        }));
        started.await.unwrap();
        presentation.abort();
        assert!(presentation.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), stopped)
            .await
            .expect("dropped presentation retained its execution task")
            .unwrap();
    }
}
