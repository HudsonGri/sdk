//! Replay owns one admitted input, independent of later source-file changes.
use acyclic_harness_conformance::{
    CaseObservation,
    runner::Corpus,
    substrate::{Local, Substrate},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct Observed(Mutex<Vec<Value>>);
impl Substrate for Observed {
    fn capabilities(&self) -> BTreeSet<String> {
        Local.capabilities()
    }

    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, Result<CaseObservation>> {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| "recipe observations poisoned")?
                .push(recipe.clone());
            Local.execute(recipe, paths).await
        })
    }
}

fn recipe(answer: &str) -> Value {
    json!({"prompt":"Say hello", "turns":[answer], "expect":{"text":answer}})
}

#[tokio::test]
async fn admitted_corpus_is_pinned_and_a_new_admission_observes_source_changes() -> Result {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("case.json");
    std::fs::write(&source, serde_json::to_vec(&recipe("original answer"))?)?;
    let admitted = Corpus::load(directory.path())?;
    assert_eq!(admitted.root(), directory.path());
    std::fs::write(&source, serde_json::to_vec(&recipe("changed answer"))?)?;
    let observed = Arc::new(Observed::default());
    let pinned = admitted.run(observed.clone(), None, 1).await?;
    assert!(pinned.success(), "{}", serde_json::to_string(&pinned)?);
    assert_eq!(
        observed
            .0
            .lock()
            .map_err(|_| "recipe observations poisoned")?
            .first()
            .and_then(|value| value.get("turns"))
            .cloned(),
        Some(json!(["original answer"])),
        "replay reopened the source instead of its admitted recipe"
    );
    let changed = Corpus::load(directory.path())?
        .run(observed.clone(), None, 1)
        .await?;
    assert!(changed.success(), "{}", serde_json::to_string(&changed)?);
    std::fs::remove_file(source)?;
    let retained = admitted.run(observed.clone(), None, 1).await?;
    assert!(retained.success(), "{}", serde_json::to_string(&retained)?);
    assert!(Corpus::load(directory.path()).is_err());
    let observations = observed
        .0
        .lock()
        .map_err(|_| "recipe observations poisoned")?;
    let answers: Vec<_> = observations
        .iter()
        .map(|value| {
            value
                .get("turns")
                .and_then(Value::as_array)
                .and_then(|turns| turns.first())
                .and_then(Value::as_str)
        })
        .collect();
    assert_eq!(
        answers,
        [
            Some("original answer"),
            Some("changed answer"),
            Some("original answer")
        ]
    );
    Ok(())
}

#[tokio::test]
async fn invalid_admitted_cases_remain_failures_when_filtered_or_unsupported() -> Result {
    let directory = tempfile::tempdir()?;
    std::fs::write(
        directory.path().join("invalid.json"),
        br#"{"driver":"unknown"}"#,
    )?;
    let admitted = Corpus::load(directory.path())?;
    for filter in [None, Some("absent")] {
        let report = admitted.run(Arc::new(Local), filter, 1).await?;
        assert_eq!(report.failed, 1);
        assert_eq!(report.skipped, 0);
        assert!(!report.success());
    }
    Ok(())
}
