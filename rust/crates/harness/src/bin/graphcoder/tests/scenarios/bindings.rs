use super::TestResult;
use serde_json::Value;

// Select what the model actually sees, without duplicating the provider's
// private canonical-name encoding. Descriptions are declared by the fixture.
pub fn tool<'a>(request: &'a Value, description: &str) -> TestResult<&'a str> {
    let mut matches = request["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|tool| tool["description"] == description);
    let definition = matches
        .next()
        .ok_or("advertised tool description missing")?;
    if matches.next().is_some() {
        return Err("advertised tool description is ambiguous".into());
    }
    definition["name"]
        .as_str()
        .ok_or_else(|| "advertised tool name missing".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    proptest! {
        #[test]
        fn advertised_names_are_selected_independently_of_alias_spelling(
            name in "[a-zA-Z_][a-zA-Z0-9_]{0,63}", description in ".{1,64}",
        ) {
            let mut request = json!({"tools":[{"name":name,"description":description}]});
            let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&request)?)?;
            prop_assert_eq!(tool(&decoded, &description).unwrap(), name.as_str());
            let duplicate = request["tools"][0].clone();
            request["tools"].as_array_mut().unwrap().push(duplicate);
            prop_assert!(tool(&request, &description).is_err(), "first-match control accepted ambiguous tool");
            request["tools"] = json!([]);
            prop_assert!(tool(&request, &description).is_err(), "missing tool passed");
        }
    }
}
