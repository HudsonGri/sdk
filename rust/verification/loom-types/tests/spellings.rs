use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeMap, path::PathBuf, process::Command};

static CASE: AtomicUsize = AtomicUsize::new(0);

fn compile(source: &str) -> Result<BTreeMap<String, String>, String> {
    let directory = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(
            "loom-type-property-{}-{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let reports = directory.join("reports");
    std::fs::create_dir_all(&reports).map_err(|e| e.to_string())?;
    for report in std::fs::read_dir(&reports).map_err(|e| e.to_string())? {
        std::fs::remove_file(report.map_err(|e| e.to_string())?.path())
            .map_err(|e| e.to_string())?;
    }
    std::fs::write(directory.join("model.rs"), source).map_err(|e| e.to_string())?;
    std::fs::write(
        directory.join("child.rs"),
        "pub fn child() { let _ = std::sync::Mutex::new(0); }",
    )
    .map_err(|e| e.to_string())?;
    std::fs::write(directory.join("models"), "model.rs\n").map_err(|e| e.to_string())?;
    let sysroot = Command::new("rustc")
        .args(["+nightly-2026-07-01", "--print", "sysroot"])
        .output()
        .map_err(|e| e.to_string())?;
    if !sysroot.status.success() {
        return Err("pinned compiler sysroot query failed".into());
    }
    let sysroot = String::from_utf8(sysroot.stdout).map_err(|e| e.to_string())?;
    let result = Command::new(env!("CARGO_BIN_EXE_loom-type-scan"))
        .args([
            "--loom-direct",
            "--crate-name",
            "spelling",
            "--crate-type",
            "lib",
            "--edition",
            "2024",
            "--emit",
            "metadata",
            "--sysroot",
            sysroot.trim(),
            "-Awarnings",
        ])
        .arg(directory.join("model.rs"))
        .arg("-o")
        .arg(directory.join("model.rmeta"))
        .env("LOOM_MODEL_ROOT", &directory)
        .env("LOOM_MODEL_FILES", directory.join("models"))
        .env("LOOM_MODEL_REPORTS", &reports)
        .output()
        .map_err(|e| e.to_string())?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).into_owned());
    }
    let mut found = BTreeMap::new();
    for report in std::fs::read_dir(&reports).map_err(|e| e.to_string())? {
        let output = std::fs::read_to_string(report.map_err(|e| e.to_string())?.path())
            .map_err(|e| e.to_string())?;
        for line in output.lines() {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 3 || fields[0] != "model.rs" {
                return Err(format!("invalid compiler response: {line}"));
            }
            if found
                .insert(fields[2].to_string(), fields[1].to_string())
                .is_some()
            {
                return Err(format!("duplicate body: {line}"));
            }
        }
    }
    std::fs::remove_dir_all(directory).map_err(|e| e.to_string())?;
    Ok(found)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn compiled_spellings_never_report_a_false_zero(raw in any::<bool>(), aliases in 0usize..5, wrapper in 0usize..4) {
        let ty = if raw { "std::sync::r#Mutex<u8>" } else { "std::sync::Mutex<u8>" };
        let method = if raw { "r#new" } else { "new" };
        let mut source = "use std::default::Default as D0;\n".to_string();
        for index in 0..aliases { source.push_str(&format!("use D{index} as D{};\n", index + 1)); }
        source.push_str(&format!("trait Factory {{ type Lock: Default; }} impl Factory for () {{ type Lock = {ty}; }}\n"));
        source.push_str("macro_rules! wrap { ($value:expr) => {{ $value }} }\n");
        let value = match wrapper {
            0 => format!("std::sync::Mutex::{method}(0u8)"),
            1 => format!("D{aliases}::default()"),
            2 => "<() as Factory>::Lock::default()".to_string(),
            _ => format!("wrap!(D{aliases}::default())"),
        };
        source.push_str(&format!("pub fn constructed() {{ let _: {ty} = {value}; }}\n"));
        source.push_str("pub fn plain() -> u8 { Default::default() }\n");
        let found = compile(&source).map_err(TestCaseError::fail)?;
        let body = |name: &str| found.iter().find_map(|(path, verdict)| (path.rsplit("::").next() == Some(name)).then_some(verdict.as_str()));
        let verdict = body("constructed").ok_or_else(|| TestCaseError::fail(format!("missing model body: {found:?}")))?;
        prop_assert!(verdict == "1" || verdict.starts_with("!indeterminate:"), "false zero: {source}\n{found:?}");
        prop_assert_eq!(body("plain"), Some("0"));
    }
}

#[test]
fn a_plain_non_lock_file_is_clean() {
    let found = compile("pub fn plain() -> u8 { let n: u8 = Default::default(); n }").unwrap();
    assert!(!found.is_empty());
    assert!(found.values().all(|verdict| verdict == "0"));
}

#[test]
fn unresolved_value_calls_and_generic_outputs_remain_findings() {
    let found = compile("fn consume(_: u8) {} pub fn shadow(consume: fn(std::sync::Mutex<()>)) { consume(Default::default()); } pub fn generic<T: Default>() -> T { T::default() }").unwrap();
    for name in ["shadow", "generic"] {
        assert!(
            found
                .iter()
                .any(|(path, verdict)| path.rsplit("::").next() == Some(name)
                    && verdict.starts_with("!indeterminate:")),
            "{found:?}"
        );
    }
}

#[test]
fn included_module_bodies_are_analyzed() {
    let found =
        compile("mod hidden { include!(\"child.rs\"); } pub fn entry() { hidden::child(); }")
            .unwrap();
    assert!(
        found
            .iter()
            .any(|(path, verdict)| path.ends_with("hidden::child") && verdict == "1"),
        "{found:?}"
    );
}
