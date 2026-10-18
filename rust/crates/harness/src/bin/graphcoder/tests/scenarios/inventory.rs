use super::super::{TestResult, root, scenario_data};
use serde_json::Value;
use std::collections::BTreeSet;

fn calls(value: &Value, names: &mut BTreeSet<String>) {
    match value {
        Value::Object(fields) => {
            if value["kind"] == "tool_call"
                && let Some(name) = value["name"].as_str()
            {
                names.insert(name.split('@').next().unwrap_or(name).into());
            }
            for value in fields.values() {
                calls(value, names);
            }
        }
        Value::Array(values) => {
            for value in values {
                calls(value, names);
            }
        }
        _ => {}
    }
}

fn covered(cases: &[Value], required: &BTreeSet<String>) -> bool {
    let mut found = BTreeSet::new();
    for case in cases {
        calls(&case["turns"], &mut found);
        calls(&case["models"], &mut found);
    }
    required.is_subset(&found)
}

fn remove_call(value: &mut Value, name: &str) {
    match value {
        Value::Array(values) => {
            values.retain(|event| {
                event["kind"] != "tool_call"
                    || event["name"]
                        .as_str()
                        .is_none_or(|actual| actual.split('@').next() != Some(name))
            });
            for value in values {
                remove_call(value, name);
            }
        }
        Value::Object(fields) => {
            for value in fields.values_mut() {
                remove_call(value, name);
            }
        }
        _ => {}
    }
}

pub(super) fn check_catalog() -> TestResult {
    let mut cases = Vec::new();
    for family in ["scenarios", "local-scenarios"] {
        let root = root().join(family);
        let mut paths = Vec::new();
        scenario_data::discover(&root, &mut paths)?;
        for path in paths {
            let (case, enabled) = scenario_data::load_tiers(&root, &path)?;
            if enabled {
                cases.push(case);
            }
        }
    }
    let required = acyclic_harness::tools::catalog()
        .map(|tool| tool.name.to_owned())
        .collect::<BTreeSet<_>>();
    assert!(
        covered(&cases, &required),
        "a production tool has no enabled binary case"
    );
    for name in &required {
        let mut missing = cases.clone();
        for case in &mut missing {
            remove_call(&mut case["turns"], name);
            remove_call(&mut case["models"], name);
        }
        assert!(
            !covered(&missing, &required),
            "missing {name} cases passed inventory"
        );
    }
    Ok(())
}

pub(super) fn check_generated_call(name: &str, text: &str) {
    let required = BTreeSet::from([name.to_owned()]);
    let mut case = serde_json::json!({"models":{"parent":[[
        {"kind":"tool_call","name":format!("{name}@revision"),"arguments":{"text":text}}
    ]]}});
    assert!(covered(&[case.clone()], &required));
    remove_call(&mut case["models"], name);
    assert!(!covered(&[case], &required));
    assert!(!covered(
        &[serde_json::json!({"turns":[text,name]})],
        &required
    ));
}
