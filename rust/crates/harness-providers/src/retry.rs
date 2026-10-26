use crate::RetrySpec;
use acyclic_harness_core::{Error, model::ModelDispatch, wire::ErrorCode};
use std::time::Duration;

/// One exchange's redelivery budget. Only a request the endpoint refused before any output is
/// redelivered, so no caller-visible event can repeat.
pub(crate) struct Redelivery(acyclic_stream::runtime::backoff::Backoff);

impl Redelivery {
    fn policy(spec: RetrySpec) -> acyclic_stream::runtime::backoff::Policy {
        acyclic_stream::runtime::backoff::Policy {
            attempts: u32::from(spec.attempts),
            base_delay: Duration::from_millis(u64::from(spec.base_delay_ms)),
            max_delay: Duration::from_millis(u64::from(spec.max_delay_ms)),
        }
    }

    /// One schedule per physical exchange, even for identical dispatches or bodies.
    pub(crate) fn new(spec: RetrySpec, dispatch: ModelDispatch) -> Self {
        let mut identity = blake3::Hasher::new();
        identity.update(&dispatch.operation_id.into_bytes());
        identity.update(&dispatch.step.to_le_bytes());
        Self(acyclic_stream::runtime::backoff::Backoff::new(
            Self::policy(spec),
            identity.finalize().as_bytes(),
        ))
    }

    pub(crate) fn for_request(spec: RetrySpec, body: &[u8]) -> Self {
        Self(acyclic_stream::runtime::backoff::Backoff::new(
            Self::policy(spec),
            body,
        ))
    }

    /// Protocol classification stays here; all delay policy lives in Stream.
    pub(crate) fn next(
        &mut self,
        error: &Error,
        retry_after: Option<Duration>,
        output: bool,
    ) -> Option<Duration> {
        if output || error.code() != ErrorCode::Unavailable {
            return None;
        }
        self.0.next(retry_after, Duration::MAX)
    }
}

/// Both HTTP forms of `Retry-After`, relative to the platform clock.
pub(crate) fn retry_after(value: &str) -> Option<Duration> {
    retry_after_at(value, || {
        #[cfg(not(target_arch = "wasm32"))]
        let now = std::time::SystemTime::now();
        #[cfg(target_arch = "wasm32")]
        let now = std::time::UNIX_EPOCH + Duration::from_secs_f64(js_sys::Date::now() / 1_000.0);
        now
    })
}

pub(crate) fn retry_after_at(
    value: &str,
    now: impl FnOnce() -> std::time::SystemTime,
) -> Option<Duration> {
    acyclic_stream::runtime::backoff::RetryAfter::parse(value, now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness_core::OperationId;

    fn dispatch() -> ModelDispatch {
        ModelDispatch {
            operation_id: OperationId::from_bytes([3; 16]),
            step: 1,
            request_digest: [0; 32],
        }
    }
    fn unavailable() -> Error {
        Error::ModelUnavailable {
            failure: acyclic_harness_core::ModelTransportFailure::Status(503),
            excerpt: String::new(),
        }
    }

    #[test]
    fn redelivery_is_bounded_jittered_and_honors_retry_after() {
        let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        let waits: Vec<_> = (0..4)
            .map(|_| retry.next(&unavailable(), None, false))
            .collect();
        assert_eq!(waits.last(), Some(&None));
        for (attempt, wait) in waits.iter().take(3).enumerate() {
            let ceiling = Duration::from_millis(500 << attempt);
            assert!(wait.is_some_and(|wait| wait >= ceiling / 2 && wait <= ceiling));
        }
        let mut hinted = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(
            hinted.next(&unavailable(), Some(Duration::from_secs(2)), false),
            Some(Duration::from_secs(2))
        );
        // Negative controls: output escaped, other classes, an over-long hint, a zero budget.
        let mut fresh = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(fresh.next(&unavailable(), None, true), None);
        assert_eq!(
            fresh.next(&Error::Storage("lost".into()), None, false),
            None
        );
        assert_eq!(fresh.next(&Error::Invalid("bad".into()), None, false), None);
        let transport = |failure| Error::ModelUnavailable {
            failure,
            excerpt: String::new(),
        };
        // TLS failures meet the same peer again; every other transport class is redelivered.
        assert_eq!(
            fresh.next(
                &transport(acyclic_harness_core::ModelTransportFailure::Tls),
                None,
                false
            ),
            None
        );
        for failure in [
            acyclic_harness_core::ModelTransportFailure::Connect,
            acyclic_harness_core::ModelTransportFailure::Timeout,
            acyclic_harness_core::ModelTransportFailure::Interrupted,
        ] {
            let mut each = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            assert!(each.next(&transport(failure), None, false).is_some());
            assert_eq!(each.next(&transport(failure), None, true), None);
        }
        assert_eq!(
            fresh.next(&unavailable(), Some(Duration::from_secs(9)), false),
            None
        );
        let mut none = Redelivery::new(
            RetrySpec {
                attempts: 0,
                ..RetrySpec::DEFAULT
            },
            dispatch(),
        );
        assert_eq!(none.next(&unavailable(), None, false), None);
        assert_eq!(retry_after(" 7 "), Some(Duration::from_secs(7)));
        assert_eq!(retry_after("not a date"), None);
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(128))]
        #[test]
        fn retry_after_forms_round_trip_and_obey_the_same_budget(
            at in 0..4_000_000_000u64, now in 0..4_000_000_000u64,
            delta in proptest::prelude::any::<u64>(),
            overflow_digits in "[0-9]{20,80}",
        ) {
            let clock = std::time::UNIX_EPOCH + Duration::from_secs(now);
            let date = httpdate::fmt_http_date(std::time::UNIX_EPOCH + Duration::from_secs(at));
            let wait = retry_after_at(&date, || clock).unwrap();
            proptest::prop_assert_eq!(wait, Duration::from_secs(at.saturating_sub(now)));
            proptest::prop_assert_eq!(retry_after_at(&delta.to_string(), || clock), Some(Duration::from_secs(delta)));
            let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            proptest::prop_assert_eq!(retry.next(&unavailable(), Some(wait), false),
                (wait <= Duration::from_millis(RetrySpec::DEFAULT.max_delay_ms.into())).then_some(wait));
            // Negative control: the former delta-only parser ignores every date hint.
            proptest::prop_assert_ne!(date.parse::<u64>().ok().map(Duration::from_secs), Some(wait));
            let overflow = format!("1{overflow_digits}");
            let wait = retry_after_at(&overflow, || clock);
            proptest::prop_assert_eq!(wait, Some(Duration::from_secs(u64::MAX)));
            let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            proptest::prop_assert_eq!(retry.next(&unavailable(), wait, false), None);
            // Negative control: discarding numeric overflow would permit an early retry.
            let mut former = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            proptest::prop_assert!(former.next(&unavailable(), overflow.parse::<u64>().ok().map(Duration::from_secs), false).is_some());
        }
    }
}
