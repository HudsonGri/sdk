use crate::{
    CaseObservation, TestResult, composer::Composer, observation, scenario::Scenario,
    scenario_data, substrate::Substrate,
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    future::Future,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[cfg(test)]
mod task_tests;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Terminal result of one discovered case.
pub enum Status {
    /// All applicable oracles passed.
    Pass,
    /// Execution, protocol validation or an oracle failed.
    Fail,
    /// Explicit filter exclusion or missing declared capability.
    Skip,
}
#[derive(Debug, Serialize, Deserialize)]
/// Exactly one record per case, ordered by corpus-relative path.
pub struct Record {
    /// Stable corpus-relative path with forward slashes.
    pub case: String,
    /// Terminal case result.
    pub status: Status,
    /// Human-readable diagnostic, including every skip reason.
    pub reason: String,
    /// Capabilities required by the resolved driver.
    pub requires: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize)]
/// Deterministic report without run identifiers or timings.
pub struct Report {
    /// JSON report schema version.
    pub version: u32,
    /// All discovered cases, including filtered cases.
    pub records: Vec<Record>,
    /// Number of successful cases.
    pub passed: usize,
    /// Number of failing cases.
    pub failed: usize,
    /// Number of explicitly skipped cases.
    pub skipped: usize,
}
impl Report {
    /// Qualification requires at least one pass and no failures.
    pub fn success(&self) -> bool {
        self.failed == 0 && self.passed > 0
    }
}

/// Capability tagging is based on the resolved driver, never a case name.
/// Concrete fixture drivers have capabilities distinct from hosted execution.
pub fn requirements(value: &Value) -> TestResult<Vec<String>> {
    match value.get("driver") {
        None => Ok(vec!["executor".into()]),
        Some(Value::String(driver)) if driver == "executor" => Ok(vec!["executor".into()]),
        Some(Value::String(driver)) if driver == "composer" => Ok(vec!["composer".into()]),
        Some(Value::String(driver)) if driver == "filesystem-policy" => {
            Ok(vec!["filesystem-policy".into()])
        }
        Some(Value::String(driver))
            if [
                "goal-local",
                "agents",
                "composer_entry",
                "notifications",
                "task_query",
                "subagents",
                "local",
                "swarm",
                "plans",
                "plan-query",
            ]
            .contains(&driver.as_str()) =>
        {
            Ok(vec![format!("local-driver:{driver}")])
        }
        _ => Err("unknown scenario driver".into()),
    }
}

/// Drives independent cases on runtime tasks with one bounded admission queue.
///
/// # Errors
/// Returns an error if an admitted task panics or is cancelled.
pub async fn run_cases<I, F>(cases: I, concurrency: NonZeroUsize) -> TestResult<Vec<F::Output>>
where
    I: IntoIterator<Item = F>,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let mut cases = cases.into_iter();
    let mut active = FuturesUnordered::new();
    for case in cases.by_ref().take(concurrency.get()) {
        active.push(acyclic_native_runtime::spawn(case).cancel_on_drop());
    }
    let mut results = Vec::new();
    while let Some(result) = active.next().await {
        results.push(result?);
        if let Some(case) = cases.next() {
            active.push(acyclic_native_runtime::spawn(case).cancel_on_drop());
        }
    }
    Ok(results)
}

/// Validated recipes and their independent oracles, admitted once for replay.
/// Filesystem changes after admission affect a new corpus, never this one.
pub struct Corpus {
    root: PathBuf,
    cases: Vec<Arc<PreparedCase>>,
}

struct PreparedCase {
    name: String,
    requires: Vec<String>,
    input: Result<CaseInput, String>,
}

struct CaseInput {
    recipe: Value,
    oracle: Option<Case>,
    missing_reason: &'static str,
}

impl PreparedCase {
    fn load(root: &Path, path: &Path) -> Self {
        let name = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let mut requires = Vec::new();
        let admitted = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut recipe = scenario_data::load_value(root, path)?;
            requires = requirements(&recipe)?;
            let missing_reason = fixture_reason(&recipe);
            let oracle = if requires
                .iter()
                .any(|capability| capability.starts_with("local-driver:"))
            {
                crate::native_schema::validate(recipe.clone())?;
                None
            } else {
                Some(Case::parse(&mut recipe)?)
            };
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(CaseInput {
                recipe,
                oracle,
                missing_reason,
            })
        }));
        let input = match admitted {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err("substrate or case runner panicked".into()),
        };
        Self {
            name,
            requires,
            input,
        }
    }
}

impl Corpus {
    /// Discover and validate every recipe, including cases a host cannot execute.
    /// Invalid cases retain failing records rather than disappearing from reports.
    pub fn load(root: &Path) -> TestResult<Self> {
        let mut paths = Vec::new();
        scenario_data::discover(root, &mut paths)?;
        paths.sort();
        if paths.is_empty() {
            return Err("no scenarios discovered".into());
        }
        Ok(Self {
            root: root.to_owned(),
            cases: paths
                .into_iter()
                .map(|path| Arc::new(PreparedCase::load(root, &path)))
                .collect(),
        })
    }

    /// Source provenance; replay never reopens this path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Replay the same admitted recipes and oracles through another host.
    pub async fn run(
        &self,
        substrate: Arc<dyn Substrate>,
        filter: Option<&str>,
        concurrency: usize,
    ) -> TestResult<Report> {
        self.run_at(
            substrate,
            filter,
            concurrency,
            tokio::time::Instant::now() + Duration::from_secs(55),
        )
        .await
    }

    async fn run_at(
        &self,
        substrate: Arc<dyn Substrate>,
        filter: Option<&str>,
        concurrency: usize,
        deadline: tokio::time::Instant,
    ) -> TestResult<Report> {
        let concurrency =
            NonZeroUsize::new(concurrency).ok_or("scenario concurrency must be positive")?;
        let capabilities = substrate.capabilities();
        let inputs = Arc::new((substrate, filter.map(str::to_owned), capabilities));
        let mut records = run_cases(
            self.cases.iter().cloned().map(|case| {
                let inputs = Arc::clone(&inputs);
                async move {
                    let (substrate, filter, capabilities) = inputs.as_ref();
                    run_case(
                        &case,
                        substrate.as_ref(),
                        filter.as_deref(),
                        capabilities,
                        deadline,
                    )
                    .await
                }
            }),
            concurrency,
        )
        .await?;
        records.sort_by(|a, b| a.case.cmp(&b.case));
        Ok(Report {
            version: 1,
            passed: records.iter().filter(|r| r.status == Status::Pass).count(),
            failed: records.iter().filter(|r| r.status == Status::Fail).count(),
            skipped: records.iter().filter(|r| r.status == Status::Skip).count(),
            records,
        })
    }
}

/// Runs every discovered case with bounded admission and client-side oracles.
pub async fn run(
    root: &Path,
    substrate: Arc<dyn Substrate>,
    filter: Option<&str>,
    concurrency: usize,
) -> TestResult<Report> {
    // Preserve the original single deadline, including recipe admission and
    // queued cases. Explicit corpus replay has already admitted those recipes.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(55);
    Corpus::load(root)?
        .run_at(substrate, filter, concurrency, deadline)
        .await
}

async fn run_case(
    case: &PreparedCase,
    substrate: &dyn Substrate,
    filter: Option<&str>,
    capabilities: &std::collections::BTreeSet<String>,
    deadline: tokio::time::Instant,
) -> Record {
    let result = AssertUnwindSafe(async {
        let input = case.input.as_ref().map_err(Clone::clone)?;
        if filter.is_some_and(|filter| !case.name.contains(filter)) {
            return Ok((Status::Skip, "excluded by --filter".into()));
        }
        let missing: Vec<_> = case
            .requires
            .iter()
            .filter(|capability| !capabilities.contains(*capability))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Ok((
                Status::Skip,
                format!("requires {}: {}", missing.join(", "), input.missing_reason),
            ));
        }
        let parsed = input
            .oracle
            .as_ref()
            .ok_or("no portable adapter for declared native capability")?;
        let paths = parsed.paths();
        let mut value = input.recipe.clone();
        parsed.prepare(&mut value)?;
        let observed = tokio::time::timeout_at(deadline, substrate.execute(value, paths))
            .await
            .map_err(|_| "corpus execution budget exhausted")??;
        parsed.check(&observed)?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            Status::Pass,
            "all scenario oracles passed".into(),
        ))
    })
    .catch_unwind()
    .await;
    let (status, reason) = match result {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => (Status::Fail, error.to_string()),
        Err(_) => (Status::Fail, "substrate or case runner panicked".into()),
    };
    Record {
        case: case.name.clone(),
        status,
        reason,
        requires: case.requires.clone(),
    }
}

fn fixture_reason(value: &Value) -> &'static str {
    match value["driver"].as_str() {
        Some("local" | "goal-local" | "agents" | "composer_entry") => {
            "driver inspects native LocalHarness state or native client entrypoints"
        }
        Some("swarm") => {
            "driver requires an instrumented in-memory provider and synchronous wire accounting"
        }
        Some("subagents") => "driver creates its own in-process Streams session/server fixture",
        Some("task_query") => {
            "driver asserts synchronous delivery from its concrete MemoryStream fixture"
        }
        Some("notifications") => "driver inspects its fixture-owned notification projection",
        Some("plans" | "plan-query") => {
            "driver uses native storage.builder/run_prompt and fixture-owned file-query projections"
        }
        _ => "host did not declare the required capability",
    }
}

enum Case {
    Executor(Box<Scenario>),
    Composer(Box<Composer>),
    FilesystemPolicy(crate::policy_owner::Case),
}
impl Case {
    fn parse(value: &mut Value) -> TestResult<Self> {
        let driver = value
            .as_object_mut()
            .ok_or("scenario must be an object")?
            .remove("driver");
        match driver.as_ref().and_then(Value::as_str) {
            Some("filesystem-policy") => Ok(Self::FilesystemPolicy(
                crate::policy_owner::Case::parse(value.clone())?,
            )),
            Some("composer") => Ok(Self::Composer(Box::new(Composer::parse(value.clone())?))),
            Some("executor") => Ok(Self::Executor(Box::new(Scenario::from_value(
                value.clone(),
            )?))),
            None if driver.is_none() => Ok(Self::Executor(Box::new(Scenario::from_value(
                value.clone(),
            )?))),
            _ => Err("no portable adapter for declared driver".into()),
        }
    }
    fn paths(&self) -> Vec<String> {
        match self {
            Self::Executor(case) => case.expect.files.keys().cloned().collect(),
            Self::Composer(case) => case.paths(),
            Self::FilesystemPolicy(_) => Vec::new(),
        }
    }
    fn prepare(&self, value: &mut Value) -> TestResult {
        let object = value.as_object_mut().ok_or("scenario must be an object")?;
        object.remove("expect");
        match self {
            Self::FilesystemPolicy(_) => {
                object.insert("driver".into(), "filesystem-policy".into());
            }
            Self::Composer(_) => {
                object.insert("driver".into(), "composer".into());
            }
            Self::Executor(case) => {
                // Resume strings are error oracles; the host needs only their count.
                object.insert(
                    "resume".into(),
                    serde_json::to_value(vec!["fixture interruption"; case.resume.len()])?,
                );
            }
        }
        Ok(())
    }
    fn check(&self, actual: &CaseObservation) -> TestResult {
        match (self, actual) {
            (Self::Executor(case), CaseObservation::Executor(actual)) => {
                observation::check(case, actual)
            }
            (Self::Composer(case), CaseObservation::Composer(actual)) => case.check(actual),
            (Self::FilesystemPolicy(case), CaseObservation::FilesystemPolicy(actual)) => {
                case.check(actual)
            }
            _ => Err("host returned observations for a different driver".into()),
        }
    }
}
