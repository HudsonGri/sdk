//! Publication notifications cannot bypass one exchange's recovery budget.
use super::FileClientError;
use acyclic_stream::runtime::Backoff;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Ready,
    Waiting,
    Stopped,
}

pub(crate) struct PublicationRecovery {
    backoff: Backoff,
    phase: Phase,
}
impl PublicationRecovery {
    pub(crate) fn new() -> Self {
        Self {
            backoff: Backoff::connection(),
            phase: Phase::Ready,
        }
    }
    /// Only the admitted timer may wake a waiting exchange. An unrelated
    /// publication neither accelerates the delay nor revives an exhausted one.
    pub(crate) fn on_publication(&self) -> bool {
        self.phase == Phase::Ready
    }
    pub(crate) fn on_timer(&mut self) -> bool {
        if self.phase != Phase::Waiting {
            return false;
        }
        self.phase = Phase::Ready;
        true
    }
    pub(crate) fn stopped(&self) -> bool {
        self.phase == Phase::Stopped
    }
    pub(crate) fn failed(&mut self, error: &FileClientError, remote: bool) -> Option<Duration> {
        let delay = if remote {
            self.backoff.next(
                matches!(error, FileClientError::Unavailable { .. }),
                None,
                false,
            )
        } else {
            None
        };
        self.phase = if delay.is_some() {
            Phase::Waiting
        } else {
            Phase::Stopped
        };
        delay
    }
    pub(crate) fn succeeded(&mut self) {
        self.backoff = Backoff::connection();
        self.phase = Phase::Ready;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::runtime::BackoffPolicy;

    fn exercise(ignore_phase: bool) {
        let mut recovery = PublicationRecovery::new();
        let error = FileClientError::Unavailable {
            reason: "lost acknowledgement".into(),
        };
        for _ in 0..BackoffPolicy::CONNECTION.attempts {
            assert!(recovery.failed(&error, true).is_some());
            for _ in 0..128 {
                let automatic = ignore_phase || recovery.on_publication();
                assert!(!automatic, "publication cannot bypass recovery ownership");
            }
            assert!(recovery.on_timer());
            assert!(!recovery.on_timer());
        }
        assert!(recovery.failed(&error, true).is_none());
        assert!(recovery.stopped());
        for _ in 0..128 {
            assert!(!recovery.on_publication());
            assert!(!recovery.on_timer());
        }
        recovery.succeeded();
        assert!(!recovery.stopped());
        assert!(recovery.on_publication());
        assert!(recovery.failed(&error, true).is_some());
    }
    #[test]
    fn only_the_owned_timer_can_resume_a_failed_publication() {
        exercise(false);
    }
    #[test]
    #[should_panic(expected = "publication cannot bypass recovery ownership")]
    fn negative_control_notification_bypasses_recovery_phase() {
        exercise(true);
    }
    #[test]
    fn permanent_and_local_failures_are_terminal() {
        let mut recovery = PublicationRecovery::new();
        assert!(
            recovery
                .failed(&FileClientError::ByteQuota { limit: 1 }, true)
                .is_none()
        );
        assert!(recovery.stopped());
        recovery.succeeded();
        assert!(
            recovery
                .failed(
                    &FileClientError::Unavailable {
                        reason: "local".into()
                    },
                    false
                )
                .is_none()
        );
        assert!(recovery.stopped());
    }
}
