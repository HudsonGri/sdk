//! One deterministic, bounded redelivery schedule for all transport adapters.
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Policy for an exchange's additional attempts. Zero attempts forbids redelivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackoffPolicy {
    /// Maximum additional attempts before returning the transport failure.
    pub attempts: u32,
    /// First attempt's upper delay bound.
    pub base_delay_ms: u32,
    /// Upper bound, including an endpoint's Retry-After hint.
    pub max_delay_ms: u32,
}

impl BackoffPolicy {
    /// Bounded recovery of a previously admitted connection.
    pub const CONNECTION: Self = Self {
        attempts: 8,
        base_delay_ms: 10,
        max_delay_ms: 1_000,
    };
}

/// Owned by one logical exchange, never shared between clients or reset per attempt.
pub struct Backoff {
    policy: BackoffPolicy,
    seed: [u8; 32],
    attempt: u32,
}

impl Backoff {
    /// Number of additional attempts already admitted for this exchange.
    #[must_use]
    pub const fn attempts_used(&self) -> u32 {
        self.attempt
    }

    /// Starts one connection recovery exchange with a runtime-owned fresh identity.
    #[must_use]
    pub fn connection() -> Self {
        Self::new(BackoffPolicy::CONNECTION, &crate::new_identity_bytes())
    }

    /// Seeds equal jitter from the exchange identity. Distinct clients must supply
    /// distinct identities; a replay of the same exchange produces the same delays.
    #[must_use]
    pub fn new(policy: BackoffPolicy, identity: &[u8]) -> Self {
        Self {
            policy,
            seed: Sha256::digest(identity).into(),
            attempt: 0,
        }
    }

    /// Returns a delay only for a classified, safely replayable failure before
    /// output. Rejected hints and final failures do not consume the budget.
    pub fn next(
        &mut self,
        replayable_failure: bool,
        retry_after: Option<Duration>,
        output_observed: bool,
    ) -> Option<Duration> {
        if !replayable_failure || output_observed || self.attempt >= self.policy.attempts {
            return None;
        }
        let maximum = Duration::from_millis(u64::from(self.policy.max_delay_ms));
        let delay = match retry_after {
            Some(delay) if delay > maximum => return None,
            Some(delay) => delay,
            None => {
                let ceiling = u64::from(self.policy.base_delay_ms)
                    .saturating_mul(1_u64 << self.attempt.min(63))
                    .min(u64::from(self.policy.max_delay_ms));
                let mut hash = Sha256::new();
                hash.update(self.seed);
                hash.update(self.attempt.to_le_bytes());
                let digest = hash.finalize();
                let (sample, _) = digest.split_first_chunk::<8>()?;
                let half = ceiling / 2;
                Duration::from_millis(half + u64::from_le_bytes(*sample) % (ceiling - half + 1))
            }
        };
        self.attempt += 1;
        Some(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_output_and_endpoint_hints_share_one_gate() {
        let mut retry = Backoff::new(BackoffPolicy::CONNECTION, b"exchange");
        assert_eq!(retry.next(false, None, false), None);
        assert_eq!(retry.next(true, None, true), None);
        assert_eq!(retry.next(true, Some(Duration::from_secs(2)), false), None);
        assert_eq!(
            retry.next(true, Some(Duration::from_millis(123)), false),
            Some(Duration::from_millis(123))
        );
        for _ in 1..BackoffPolicy::CONNECTION.attempts {
            assert!(retry.next(true, None, false).is_some());
        }
        assert_eq!(retry.next(true, None, false), None);
    }

    #[test]
    fn independent_exchanges_spread_and_replay_exactly() {
        let schedule = |identity: &[u8]| {
            let mut retry = Backoff::new(BackoffPolicy::CONNECTION, identity);
            std::iter::from_fn(|| retry.next(true, None, false)).collect::<Vec<_>>()
        };
        let schedules = (0_u32..128)
            .map(|id| schedule(&id.to_le_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(schedules.first(), Some(&schedule(&0_u32.to_le_bytes())));
        assert!(
            schedules
                .iter()
                .zip(schedules.iter().skip(1))
                .any(|(a, b)| a != b)
        );
        for delays in &schedules {
            for (attempt, delay) in delays.iter().enumerate() {
                let ceiling = (10_u64 << attempt).min(1_000);
                assert!(
                    (u128::from(ceiling / 2)..=u128::from(ceiling)).contains(&delay.as_millis())
                );
            }
        }
        // Negative control: the former constant schedule cannot distinguish any clients.
        let synchronized = vec![vec![Duration::from_millis(100); 8]; 128];
        assert!(
            !synchronized
                .iter()
                .zip(synchronized.iter().skip(1))
                .any(|(a, b)| a != b)
        );
    }
}
