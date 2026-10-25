//! Retry scheduling shared by protocol adapters. Delivery safety belongs to the adapter;
//! this module owns attempt limits, jitter, server hints and the remaining elapsed budget.
use sha2::{Digest as _, Sha256};
use std::time::{Duration, SystemTime};

/// Finite scheduling limits; `attempts` counts redeliveries after the initial send.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// Maximum number of redeliveries.
    pub attempts: u32,
    /// Initial exponential delay ceiling.
    pub base_delay: Duration,
    /// Largest delay, including a server hint.
    pub max_delay: Duration,
}

/// One exchange's schedule. Reusing a body never reuses another exchange's jitter.
pub struct Backoff {
    policy: Policy,
    seed: [u8; 32],
    attempt: u32,
    ceiling: Duration,
}

impl Backoff {
    /// Creates a schedule using the shared host/simulation entropy seam and request identity.
    #[must_use]
    pub fn new(policy: Policy, identity: &[u8]) -> Self {
        Self::with_entropy(policy, identity, crate::new_identity())
    }

    /// Creates a reproducible schedule with caller-owned entropy, for deterministic simulation.
    #[must_use]
    pub fn with_entropy(policy: Policy, identity: &[u8], entropy: [u8; 16]) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"acyclic.retry.v1\0");
        hash.update(entropy);
        hash.update(identity);
        Self {
            policy,
            seed: hash.finalize().into(),
            attempt: 0,
            ceiling: policy.base_delay.min(policy.max_delay),
        }
    }

    /// Returns the next delay only if another attempt can begin inside `remaining`.
    /// A rejected hint is never clamped or treated as absent, which would retry too early.
    pub fn next(&mut self, hint: Option<Duration>, remaining: Duration) -> Option<Duration> {
        if self.attempt >= self.policy.attempts {
            return None;
        }
        let wait = match hint {
            Some(wait) if wait > self.policy.max_delay => return None,
            Some(wait) => wait,
            None => {
                let ceiling = self.ceiling;
                let mut hash = Sha256::new();
                hash.update(self.seed);
                hash.update(self.attempt.to_le_bytes());
                let digest = hash.finalize();
                let mut sample = [0; 16];
                sample.copy_from_slice(digest.get(..16)?);
                // Nanosecond arithmetic retains sub-millisecond gRPC/browser policies.
                let half = ceiling.as_nanos() / 2;
                let width = ceiling.as_nanos() - half + 1;
                let nanos = half + u128::from_le_bytes(sample) % width;
                Duration::new(
                    u64::try_from(nanos / 1_000_000_000).ok()?,
                    u32::try_from(nanos % 1_000_000_000).ok()?,
                )
            }
        };
        if wait >= remaining {
            return None;
        }
        self.attempt += 1;
        self.ceiling = self.ceiling.saturating_mul(2).min(self.policy.max_delay);
        Some(wait)
    }

    /// Adapts the same schedule to the native/browser Executor's millisecond clock.
    /// Rounding never removes a yield or admits a wait outside the remaining budget.
    pub fn next_millis(&mut self, remaining: Duration) -> Option<u32> {
        let wait = self.next(None, remaining)?;
        let millis = wait.as_millis() + u128::from(wait.subsec_nanos() % 1_000_000 != 0);
        let millis = u32::try_from(millis).ok()?;
        (Duration::from_millis(u64::from(millis)) < remaining).then_some(millis)
    }
}

/// The two HTTP Retry-After forms, interpreted against a caller-supplied wall clock.
pub struct RetryAfter;

impl RetryAfter {
    /// Preserves an overflowing delta as an excessive hint, never as permission to retry.
    #[must_use]
    pub fn parse(value: &str, now: SystemTime) -> Option<Duration> {
        let value = value.trim();
        if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
        }
        let date = httpdate::parse_http_date(value).ok()?;
        Some(date.duration_since(now).unwrap_or(Duration::ZERO))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> Policy {
        Policy {
            attempts: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
        }
    }

    #[test]
    fn identical_requests_draw_fresh_entropy_but_owned_schedules_replay() {
        let first = Backoff::new(policy(), b"same body");
        let second = Backoff::new(policy(), b"same body");
        assert_ne!(
            first.seed, second.seed,
            "identical requests synchronized their retries"
        );
        let mut first = Backoff::with_entropy(policy(), b"body", [7; 16]);
        let mut replay = Backoff::with_entropy(policy(), b"body", [7; 16]);
        for _ in 0..4 {
            assert_eq!(
                first.next(None, Duration::MAX),
                replay.next(None, Duration::MAX)
            );
        }
    }

    #[test]
    fn remaining_budget_never_restarts_and_hints_cannot_be_shortened() {
        let mut retry = Backoff::with_entropy(policy(), b"body", [7; 16]);
        assert_eq!(
            retry.next(Some(Duration::from_millis(20)), Duration::from_millis(20)),
            None
        );
        assert_eq!(retry.next(None, Duration::ZERO), None);
        assert_eq!(
            retry.next(Some(Duration::from_secs(1)), Duration::MAX),
            None
        );
        for _ in 0..3 {
            assert_eq!(
                retry.next(Some(Duration::ZERO), Duration::from_nanos(1)),
                Some(Duration::ZERO)
            );
        }
        assert_eq!(retry.next(None, Duration::MAX), None);
    }

    #[test]
    fn both_hint_forms_and_numeric_overflow_preserve_the_server_floor() {
        let clock = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert_eq!(
            RetryAfter::parse(" 7 ", clock),
            Some(Duration::from_secs(7))
        );
        let date = httpdate::fmt_http_date(clock + Duration::from_secs(7));
        assert_eq!(
            RetryAfter::parse(&date, clock),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            RetryAfter::parse("9999999999999999999999999", clock),
            Some(Duration::from_secs(u64::MAX))
        );
        assert_eq!(RetryAfter::parse("invalid", clock), None);
    }

    #[test]
    fn millisecond_adapters_keep_yield_and_exponential_growth_does_not_wrap() {
        let mut retry = Backoff::with_entropy(
            Policy {
                attempts: 200,
                base_delay: Duration::from_nanos(1),
                max_delay: Duration::MAX,
            },
            b"wide policy",
            [1; 16],
        );
        assert_eq!(retry.next_millis(Duration::from_millis(2)), Some(1));
        for _ in 1..100 {
            assert!(retry.next(None, Duration::MAX).is_some());
        }
        assert!(
            retry
                .next(None, Duration::MAX)
                .is_some_and(|wait| wait > Duration::from_secs(1_000_000))
        );
        let mut retry = Backoff::with_entropy(
            Policy {
                attempts: 1,
                base_delay: Duration::from_nanos(1),
                max_delay: Duration::from_nanos(1),
            },
            b"rounding",
            [1; 16],
        );
        assert_eq!(retry.next_millis(Duration::from_millis(1)), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    proptest::proptest! {
        #[test]
        fn every_schedule_obeys_its_attempt_delay_and_elapsed_limits(
            base in 0..u32::MAX, maximum in 0..u32::MAX,
            attempts in 0..32u32, remaining in 0..u32::MAX,
            entropy in proptest::array::uniform16(proptest::prelude::any::<u8>()),
        ) {
            let policy = Policy { attempts, base_delay: Duration::from_millis(u64::from(base)), max_delay: Duration::from_millis(u64::from(maximum)) };
            let mut retry = Backoff::with_entropy(policy, b"request", entropy);
            for attempt in 0..=attempts {
                let wait = retry.next(None, Duration::from_millis(u64::from(remaining)));
                let ceiling = policy.base_delay.saturating_mul(1_u32 << attempt.min(31)).min(policy.max_delay);
                if let Some(wait) = wait {
                    proptest::prop_assert!(attempt < attempts);
                    proptest::prop_assert!(wait <= ceiling && wait >= ceiling / 2);
                    proptest::prop_assert!(wait < Duration::from_millis(u64::from(remaining)));
                } else { break; }
            }
        }
    }
}
