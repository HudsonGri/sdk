//! Availability of the existing Cloud HST-01..12 histories in this shared suite.
//!
//! The ordinary five-case suite supplies partial observations only. None of the
//! complete HST histories is exported yet. This module makes that missing scope
//! explicit to native, local, browser and endpoint callers; it does not certify
//! runtime results, source identities, negative controls or durable recovery.

use super::{Case, RECOVERY};

/// Stable history identities adopted from the existing Cloud hostability matrix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryId {
    /// Serializable namespace cut, composed domains and spawn envelope.
    NamespaceCut,
    /// Accepted, unresolved intent and exact-key recovery.
    PendingRecovery,
    /// Successful bindings survive namespace-lifetime retention and recovery.
    NamespaceKeyLifetime,
    /// Late messages cannot affect a newer attempt's effects or holds.
    AttemptGenerationFence,
    /// Durable decision, participant roots and receipt publish coherently.
    DurableDecisionPublication,
    /// Forks retain their transitive history across GC and restore.
    ForkClosureRetention,
    /// Nested forks use the pre-commit cut and segment exclusions.
    NestedExclusions,
    /// Subtree authority is checked before descendant exposure.
    ForkAuthority,
    /// Existing inherited-path follows wake across root publication.
    NamespaceFollowWake,
    /// Session credit, channel ownership and revocation remain bounded.
    SessionCreditAuthority,
    /// Leader movement and reconnect preserve applied cursor continuity.
    LeaderResume,
    /// Actual fork/query work is bounded independently of descendants.
    CountedWork,
}

impl HistoryId {
    /// All twelve identities in their original, stable order.
    pub const ALL: [Self; 12] = [
        Self::NamespaceCut,
        Self::PendingRecovery,
        Self::NamespaceKeyLifetime,
        Self::AttemptGenerationFence,
        Self::DurableDecisionPublication,
        Self::ForkClosureRetention,
        Self::NestedExclusions,
        Self::ForkAuthority,
        Self::NamespaceFollowWake,
        Self::SessionCreditAuthority,
        Self::LeaderResume,
        Self::CountedWork,
    ];

    /// Existing matrix identifier, suitable for machine-readable output.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::NamespaceCut => "HST-01",
            Self::PendingRecovery => "HST-02",
            Self::NamespaceKeyLifetime => "HST-03",
            Self::AttemptGenerationFence => "HST-04",
            Self::DurableDecisionPublication => "HST-05",
            Self::ForkClosureRetention => "HST-06",
            Self::NestedExclusions => "HST-07",
            Self::ForkAuthority => "HST-08",
            Self::NamespaceFollowWake => "HST-09",
            Self::SessionCreditAuthority => "HST-10",
            Self::LeaderResume => "HST-11",
            Self::CountedWork => "HST-12",
        }
    }

    /// Ordinary cases that supply only some observations for this history.
    ///
    /// This describes compiled case availability, never execution or proof
    /// credit. Passing every returned case does not qualify the HST history.
    #[must_use]
    pub const fn partial_cases(self) -> &'static [Case] {
        match self {
            Self::NamespaceCut => &[Case::SubtreeFork, Case::ComposedFork],
            Self::PendingRecovery | Self::NamespaceKeyLifetime | Self::AttemptGenerationFence => {
                &[Case::AppendRetry, Case::CommitRetry]
            }
            Self::DurableDecisionPublication => &[Case::ComposedFork],
            Self::ForkClosureRetention
            | Self::NestedExclusions
            | Self::ForkAuthority
            | Self::NamespaceFollowWake
            | Self::SessionCreditAuthority
            | Self::LeaderResume
            | Self::CountedWork => &[],
        }
    }
}

/// A missing capability is distinct from a failed semantic assertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnqualifiedReason {
    /// No public shared entrypoint executes the complete canonical history.
    CanonicalHistoryNotExported,
}

impl UnqualifiedReason {
    /// Stable reason used by endpoint coverage reports.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CanonicalHistoryNotExported => "CanonicalHistoryNotExported",
        }
    }
}

/// One explicitly unqualified history; there is no skipped/pass substitution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnqualifiedHistory {
    /// Stable history identity.
    pub history: HistoryId,
    /// Why this build cannot qualify the complete history.
    pub reason: UnqualifiedReason,
}

/// Complete missing-HST coverage in bounded, stable order.
///
/// This is provider-independent export availability. It does not probe an
/// endpoint or substitute a memory provider when a hosted capability is absent.
#[must_use]
pub fn hst_coverage() -> [UnqualifiedHistory; 12] {
    HistoryId::ALL.map(|history| UnqualifiedHistory {
        history,
        reason: UnqualifiedReason::CanonicalHistoryNotExported,
    })
}

/// A request to qualify the complete HST suite cannot be satisfied by this build.
#[derive(Debug, thiserror::Error)]
#[error(
    "step=require_hst_qualification missing={missing:?}; recovery: use the shared HST implementation with matching positive/control receipts, then rerun {RECOVERY}"
)]
pub struct Unqualified {
    /// All missing histories; ordinary-case success cannot remove these rows.
    pub missing: [UnqualifiedHistory; 12],
}

/// Refuses complete HST qualification until canonical histories are exported.
///
/// Call this for an HST qualification request rather than treating successful
/// ordinary cases or an empty selection as a complete-suite pass.
pub fn require_hst_qualification() -> Result<(), Unqualified> {
    Err(Unqualified {
        missing: hst_coverage(),
    })
}
