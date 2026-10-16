use super::{TestResult, matchers};
use acyclic_harness::{
    Error, LocalHarness, LocalHarnessConfig, Result,
    context::{ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    task_volume::{FileKey, Query},
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    prompt: String,
    #[serde(default)]
    seed: BTreeMap<String, String>,
    models: BTreeMap<String, Vec<ScriptedTurn>>,
    expect: Expected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    text: String,
    requests: BTreeMap<String, usize>,
    task_files: BTreeMap<String, Value>,
    #[serde(default)]
    model_requests: BTreeMap<String, Vec<Value>>,
    #[serde(default)]
    files: BTreeMap<String, Option<String>>,
}

struct Models(BTreeMap<String, Arc<ScriptedModelProvider>>);

impl Models {
    fn get(&self, name: &str) -> Result<&ScriptedModelProvider> {
        self.0
            .get(name)
            .map(Arc::as_ref)
            .ok_or_else(|| Error::NotFound(format!("scripted model {name}")))
    }
}

impl ModelProvider for Models {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.get(&model.name)?.context_capacity(model)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            self.get(&request.request().model.name)?
                .count_tokens(request)
                .await
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        match self.get(&request.request().model.name) {
            Ok(provider) => {
                eprintln!(
                    "agent scenario: model {} request {}",
                    request.request().model.name,
                    provider.requests().len() + 1
                );
                provider.generate(request, dispatch)
            }
            Err(error) => Box::pin(futures::stream::iter([Err(error)])),
        }
    }
}

pub(super) async fn run(value: Value) -> TestResult {
    let case: Case = serde_json::from_value(value)?;
    if case.models.keys().ne(case.expect.requests.keys()) {
        return Err("every scripted model needs an exact request count".into());
    }
    let provider = Arc::new(Models(
        case.models
            .into_iter()
            .map(|(name, turns)| {
                (
                    name,
                    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(
                        ModelContextCapacity {
                            context_tokens: 32_768,
                            output_tokens: 4096,
                        },
                    )),
                )
            })
            .collect(),
    ));
    let directory = tempfile::tempdir()?;
    for (path, text) in case.seed {
        let path = file_path(directory.path(), &path)?;
        std::fs::create_dir_all(path.parent().ok_or("missing seed parent")?)?;
        std::fs::write(path, text)?;
    }
    let model = Model::new("scripted".into(), "parent".into(), "1".into(), Value::Null)?;
    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model, provider.clone()),
    ))
    .await?;
    let output = match harness.run(&case.prompt).await {
        Ok(output) => output,
        Err(error) => {
            let requests: BTreeMap<_, _> = provider
                .0
                .iter()
                .map(|(name, model)| (name.as_str(), model.requests().len()))
                .collect();
            let children = harness
                .task_files()
                .observe(Query::Children, None, 64)
                .await?
                .snapshot
                .ok_or("missing child failure snapshot")?;
            let statuses: BTreeMap<_, _> = children
                .files
                .iter()
                .filter(|file| file.key.path().ends_with("/status.json"))
                .map(|file| {
                    (
                        file.key.path(),
                        String::from_utf8_lossy(&file.bytes).into_owned(),
                    )
                })
                .collect();
            return Err(format!(
                "{error}; model requests: {requests:?}; child statuses: {statuses:?}"
            )
            .into());
        }
    };
    if output.text != case.expect.text {
        return Err(format!("unexpected parent output: {:?}", output.text).into());
    }
    for (name, expected) in case.expect.requests {
        let model = provider.get(&name)?;
        if model.requests().len() != expected || model.remaining() != 0 {
            return Err(format!("model {name}: expected {expected} requests and no unused turns, got {} requests and {} unused turns", model.requests().len(), model.remaining()).into());
        }
    }
    for (name, expected) in case.expect.model_requests {
        let actual = provider
            .get(&name)?
            .requests()
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        matchers::ordered(&expected, &actual, &name)?;
    }
    for (path, expected) in case.expect.files {
        let actual = match std::fs::read_to_string(file_path(directory.path(), &path)?) {
            Ok(text) => Some(text),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if actual != expected {
            return Err(format!("workspace {path}: expected {expected:?}, got {actual:?}").into());
        }
    }
    for (path, expected) in case.expect.task_files {
        let key = FileKey::parse(&path)?;
        let snapshot = harness
            .task_files()
            .observe(Query::File(key), None, 1)
            .await?
            .snapshot
            .ok_or("missing file snapshot")?;
        let file = snapshot
            .files
            .first()
            .ok_or_else(|| format!("missing {path}"))?;
        let actual = serde_json::from_slice(&file.bytes)?;
        if !matchers::subset(&expected, &actual) {
            return Err(format!("{path}: expected {expected}, got {actual}").into());
        }
    }
    Ok(())
}

fn file_path(root: &std::path::Path, path: &str) -> TestResult<std::path::PathBuf> {
    let path = std::path::Path::new(path);
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err("scenario file must stay inside its workspace".into());
    }
    Ok(root.join(path))
}
