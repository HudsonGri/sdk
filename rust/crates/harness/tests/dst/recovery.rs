//! Crash the shipping local composition, then reopen with no retained agent state.
use super::*;
use acyclic_native_runtime::testing::DurableFault;
use driver::{Driver, Outcome};
use std::cell::Cell;

fn local_turn<'a>(
    directory: &'a Path,
    schedule: &'a acyclic_native_runtime::testing::Schedule,
    first_turn_write: &'a Cell<Option<usize>>,
) -> EntryFuture<'a, Result<()>> {
    entry(async move {
        let model = Model::new(
            "scripted".into(),
            "dst".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
            "done".into(),
        )]));
        let config = local_harness_config(directory.join("checkout"), (model, provider))?
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        first_turn_write.set(Some(schedule.durable_writes().len()));
        prompt(&harness, "finish this turn", "done", false).await?;
        Ok(())
    })
}

#[test]
fn checkout_reopens_after_first_turn_write() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let baseline_directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(baseline_directory.path().join("checkout")).map_err(storage)?;
        let mut baseline = Driver::new(71, None);
        let marker = Cell::new(None);
        let schedule = baseline.schedule.clone();
        match baseline
            .run(local_turn(baseline_directory.path(), &schedule, &marker))
            .await
        {
            Outcome::Completed(result) => result?,
            Outcome::Crashed(cause) => panic!("uncut local turn crashed: {cause}"),
        }
        let cut = marker.get().expect("local turn was never admitted");
        assert!(
            baseline.schedule.durable_writes().len() > cut,
            "turn made no durable writes"
        );
        drop(baseline);
        drop(schedule);
        let directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
        let mut crash = Driver::new(71, Some((cut, DurableFault::After)));
        let schedule = crash.schedule.clone();
        let marker = Cell::new(None);
        let Outcome::Crashed(cause) = crash
            .run(local_turn(directory.path(), &schedule, &marker))
            .await
        else {
            panic!("turn escaped its durable write cut");
        };
        assert_eq!(cause.ordinal, cut);
        assert_eq!(cause.fault, DurableFault::After);
        assert_eq!(marker.get(), Some(cut), "crashed a different operation");
        drop(schedule);
        drop(crash);
        // This epoch derives from the same seed. Its providers, stores, clocks,
        // identity source and executor are constructed independently.
        let mut recovery = Driver::new(72, None);
        let schedule = recovery.schedule.clone();
        let marker = Cell::new(None);
        match recovery
            .run(local_turn(directory.path(), &schedule, &marker))
            .await
        {
            Outcome::Completed(result) => result,
            Outcome::Crashed(cause) => panic!("uncut recovery crashed: {cause}"),
        }
    })
}
