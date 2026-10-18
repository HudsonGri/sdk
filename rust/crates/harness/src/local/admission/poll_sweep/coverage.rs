//! Independent, reviewed boundary counts and complete cut plans.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn permute<T>(pairs: &mut [T], seed: u64) {
    let mut random = blake3::Hasher::new_derive_key("acyclic.harness.test.cut-order.v1");
    random.update(&seed.to_le_bytes());
    let mut random = random.finalize_xof();
    for upper in (1..pairs.len()).rev() {
        let mut draw = [0; 8];
        random.fill(&mut draw);
        pairs.swap(
            upper,
            (u64::from_le_bytes(draw) % (upper as u64 + 1)) as usize,
        );
    }
}

pub(super) fn check_interleaved(order: impl IntoIterator<Item = Scenario>) -> Result<()> {
    let mut runs = BTreeMap::new();
    let mut previous = None;
    for scenario in order {
        if previous != Some(scenario) {
            *runs.entry(scenario as u8).or_insert(0) += 1;
            previous = Some(scenario);
        }
    }
    if runs.len() != 3 || runs.values().any(|runs| *runs < 2) {
        return Err(Error::Storage(
            "the matrix still groups cuts by scenario".into(),
        ));
    }
    Ok(())
}

fn manifest_pairs() -> Vec<(Scenario, usize)> {
    [
        Scenario::Text,
        Scenario::EffectAndContinuation,
        Scenario::Compaction,
    ]
    .into_iter()
    .flat_map(|scenario| {
        let total = durable_total(scenario).unwrap();
        (1..=total).map(move |cut| (scenario, cut))
    })
    .collect()
}

#[test]
fn seed_711_interleaves_the_actual_manifest_and_rejects_the_grouped_control() -> Result<()> {
    let mut pairs = manifest_pairs();
    assert!(check_interleaved(pairs.iter().map(|(scenario, _)| *scenario)).is_err());
    permute(&mut pairs, 711);
    check_interleaved(pairs.into_iter().map(|(scenario, _)| scenario))
}

fn expected(scenario: Scenario) -> Result<BTreeMap<String, usize>> {
    let bytes: &[u8] = match scenario {
        Scenario::Text => include_bytes!("coverage/text.json"),
        Scenario::EffectAndContinuation => include_bytes!("coverage/effect_goal.json"),
        Scenario::Compaction => include_bytes!("coverage/compaction.json"),
    };
    serde_json::from_slice(bytes).map_err(storage)
}

/// Every durable boundary in the reviewed manifest is one replay cut.
fn durable_total(scenario: Scenario) -> Result<usize> {
    Ok(expected(scenario)?
        .iter()
        .filter(|(name, _)| durable(name))
        .map(|(_, count)| *count)
        .sum())
}

pub(super) fn check_boundaries(
    scenario: Scenario,
    actual: &[&str],
    ready_reads: usize,
) -> Result<()> {
    let mut counts = BTreeMap::new();
    for name in actual {
        *counts.entry((*name).to_owned()).or_insert(0) += 1;
    }
    counts.insert("stream:read:ready".into(), ready_reads);
    let expected = expected(scenario)?;
    if counts != expected {
        return Err(Error::Storage(format!(
            "{scenario:?} independent boundary coverage: expected {expected:?}, actual {counts:?}"
        )));
    }
    Ok(())
}

pub(super) fn check_cuts(
    scenario: Scenario,
    plan: &[Cut],
    cuts: impl IntoIterator<Item = Cut>,
) -> Result<()> {
    // Every occurrence of every durable and operation boundary in the reviewed
    // manifest is exactly one planned cut.
    let mut required = BTreeSet::new();
    for (name, count) in expected(scenario)? {
        if let Some(name) = schedule::DURABLE
            .into_iter()
            .chain(schedule::OPERATION)
            .find(|planned| *planned == name)
        {
            required.extend((1..=count).map(|occurrence| (name, occurrence)));
        }
    }
    let cuts: Vec<_> = cuts.into_iter().collect();
    let planned: BTreeSet<_> = plan.iter().copied().collect();
    if plan
        .iter()
        .map(|cut| (cut.name, cut.occurrence))
        .collect::<BTreeSet<_>>()
        != required
        || planned.len() != plan.len()
        || cuts.len() != plan.len()
        || cuts.into_iter().collect::<BTreeSet<_>>() != planned
    {
        return Err(Error::Storage(
            "the real sweep omitted or duplicated a required cut".into(),
        ));
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn broken_real_sweep_cannot_drop_a_cut(which in 0u8..3, omitted in 0usize..4096) {
        let scenario = match which { 0 => Scenario::Text, 1 => Scenario::EffectAndContinuation, _ => Scenario::Compaction };
        let counts = expected(scenario).unwrap();
        let plan: Vec<_> = schedule::DURABLE
            .into_iter()
            .chain(schedule::OPERATION)
            .flat_map(|name| {
                (1..=counts.get(name).copied().unwrap_or(0))
                    .map(move |occurrence| Cut { name, occurrence, durable: 0 })
            })
            .collect();
        check_cuts(scenario, &plan, plan.iter().copied()).unwrap();
        let omitted = omitted % plan.len();
        // This is the actual replay check, deliberately missing a real cut.
        proptest::prop_assert!(check_cuts(scenario, &plan, plan.iter().copied().filter(|cut| *cut != plan[omitted])).is_err());
        // A planner that drops a boundary fails even if the sweep follows it.
        let mut short = plan.clone();
        short.remove(omitted);
        proptest::prop_assert!(check_cuts(scenario, &short, short.iter().copied()).is_err());
    }

    #[test]
    fn seeded_permutation_preserves_every_manifest_pair(seed in proptest::prelude::any::<u64>()) {
        let expected = manifest_pairs();
        let mut first = expected.clone();
        let mut replay = expected.clone();
        permute(&mut first, seed);
        permute(&mut replay, seed);
        proptest::prop_assert_eq!(&first, &replay);
        first.sort_unstable_by_key(|(scenario, cut)| (*scenario as u8, *cut));
        proptest::prop_assert_eq!(first, expected);
    }
}

#[test]
fn omitted_actual_hook_and_cut_are_rejected() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let observed = runtime.block_on(case(Scenario::Text, None, &[], None))?;
    let actual = observed.points;
    check_boundaries(Scenario::Text, &actual, observed.ready_reads)?;
    let omitted = actual
        .iter()
        .position(|name| *name == "task:submitted")
        .unwrap();
    let mut broken = actual.clone();
    broken.remove(omitted);
    let plan = plan(&actual);
    let omitted = plan
        .iter()
        .position(|cut| cut.name == "task:submitted")
        .unwrap();
    assert!(
        check_boundaries(Scenario::Text, &broken, observed.ready_reads).is_err(),
        "missing real hook went undetected"
    );
    check_cuts(Scenario::Text, &plan, plan.iter().copied())?;
    assert!(
        check_cuts(
            Scenario::Text,
            &plan,
            plan.iter().copied().filter(|cut| *cut != plan[omitted])
        )
        .is_err(),
        "missing real replay went undetected"
    );
    Ok(())
}
