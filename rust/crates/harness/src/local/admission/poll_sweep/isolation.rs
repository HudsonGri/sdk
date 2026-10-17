//! Compare successive replays on one driver with independently owned drivers.
use super::*;
use futures::FutureExt;

pub(super) fn idle(runtime: &tokio::runtime::Runtime, local: &mut tokio::task::LocalSet) {
    assert!(
        runtime.block_on(async { local.now_or_never() }).is_some(),
        "replay driver retained work from an earlier cut"
    );
}

fn equivalent(fresh: &Coverage, reused: &Coverage) {
    assert_eq!(
        fresh.points, reused.points,
        "a reused driver changed the cut"
    );
    assert_eq!(fresh.ready_reads, reused.ready_reads);
    assert_eq!(fresh.unadmitted, reused.unadmitted);
    assert_eq!(fresh.admission_submission, reused.admission_submission);
    assert_eq!(fresh.admission_committed, reused.admission_committed);
    assert_eq!(
        fresh.observations.semantic_trace(),
        reused.observations.semantic_trace(),
        "replay driver carried admission or effect state between cuts"
    );
}

#[test]
fn successive_cuts_match_fresh_drivers_in_both_orders() -> Result<()> {
    let (runtime, _) = replay_runtime()?;
    let mut reused = tokio::task::LocalSet::new();
    let mut inputs = Vec::new();
    for scenario in [
        Scenario::Text,
        Scenario::EffectAndContinuation,
        Scenario::Compaction,
    ] {
        let seed = runtime.block_on(reused.run_until(
            acyclic_native_runtime::testing::with_local_tasks(Seed::new(scenario)),
        ))?;
        let baseline = replay_case(&runtime, &reused, scenario, None, &[], Some(&seed))?;
        idle(&runtime, &mut reused);
        coverage::check_boundaries(scenario, &baseline.points, baseline.ready_reads)?;
        inputs.push((scenario, seed, baseline.points));
    }
    for reverse in [false, true] {
        let mut plan: Vec<_> = inputs
            .iter()
            .flat_map(|(scenario, seed, points)| {
                [1, points.len()]
                    .into_iter()
                    .map(move |cut| (*scenario, seed, points, cut))
            })
            .collect();
        if reverse {
            plan.reverse();
        }
        for (scenario, seed, points, cut) in plan {
            let pair = crate::test_support::differential::pair(
                tokio::task::LocalSet::new,
                |mut fresh, independent| {
                    let driver = if independent { &mut fresh } else { &mut reused };
                    let result =
                        replay_case(&runtime, driver, scenario, Some(cut), points, Some(seed));
                    idle(&runtime, driver);
                    if let Ok(coverage) = &result {
                        for point in &coverage.points {
                            crate::test_support::differential::record(point, 1);
                        }
                        crate::test_support::differential::record(
                            "ready-read",
                            coverage.ready_reads as u64,
                        );
                    }
                    result
                },
            );
            let [(expected, expected_trace), (observed, observed_trace)] = pair;
            equivalent(&expected?, &observed?);
            assert_eq!(expected_trace, observed_trace);
        }
    }
    Ok(())
}

#[test]
fn surviving_task_control_fails_the_same_driver_isolation_check() -> Result<()> {
    let (runtime, _) = replay_runtime()?;
    let mut local = tokio::task::LocalSet::new();
    // A real detached owner from the preceding cut, carrying shared state.
    let dirty = std::rc::Rc::new(std::cell::Cell::new(false));
    let state = dirty.clone();
    local.spawn_local(async move {
        state.set(true);
        std::future::pending::<()>().await;
    });
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| idle(&runtime, &mut local)));
    assert!(dirty.get(), "the deliberately surviving owner never ran");
    assert!(
        result.is_err(),
        "dirty driver escaped the same isolation check"
    );
    Ok(())
}
