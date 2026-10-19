use crate::{Error, Result};

#[doc(hidden)]
pub fn frontmatter_end(prefix: &[u8]) -> Result<(usize, usize, usize)> {
    let mut offset = 0;
    let mut header_start = None;
    let mut header_end = None;
    for line in prefix.split_inclusive(|byte| *byte == b'\n') {
        let delimiter = line.strip_suffix(b"\n").unwrap_or(line);
        let delimiter = delimiter.strip_suffix(b"\r").unwrap_or(delimiter);
        if offset == 0 {
            if delimiter != b"---" || !line.ends_with(b"\n") {
                return Err(Error::Invalid(
                    "skill requires opening frontmatter delimiter".into(),
                ));
            }
            header_start = Some(line.len());
        } else if delimiter == b"---" && line.ends_with(b"\n") {
            header_end = Some((offset, offset + line.len()));
            break;
        }
        offset += line.len();
    }
    let (Some(start), Some((end, body))) = (header_start, header_end) else {
        return Err(Error::Invalid(
            "skill frontmatter is missing or exceeds prefix bound".into(),
        ));
    };
    Ok((start, end, body))
}

#[doc(hidden)]
pub fn parse_frontmatter<T: serde::de::DeserializeOwned>(prefix: &[u8]) -> Result<T> {
    let (start, end, _) = frontmatter_end(prefix)?;
    let header = prefix
        .get(start..end)
        .ok_or_else(|| Error::Invalid("invalid skill header".into()))?;
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            // Keep the library's stack-safety guards for structural and flow
            // nesting, including unknown fields skipped by Deserialize.
            max_events: usize::MAX, max_nodes: usize::MAX, max_documents: 1,
            max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
            max_total_scalar_bytes: usize::MAX,
        },
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        reject_unsupported_tags: true,
        emit_comments: false,
        with_snippet: false,
    };
    serde_saphyr::from_slice_with_options(header, options)
        .map_err(|error| Error::Invalid(format!("invalid frontmatter: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Header {
        name: String,
        description: String,
    }

    proptest! {
        // Both collection syntaxes reach the same depth guard, even when
        // Deserialize ignores the entire nested value.
        #[test]
        fn nested_unknown_fields_obey_the_parser_stack_guard(depth in 0usize..256, flow in any::<bool>()) {
            let mut input = "---\nname: fixture\ndescription: text\nextra: ".to_owned();
            if flow {
                input.push_str(&"[".repeat(depth));
                input.push_str("null");
                input.push_str(&"]".repeat(depth));
                input.push('\n');
            } else {
                input.push('\n');
                for level in 1..=depth {
                    input.push_str(&"  ".repeat(level));
                    input.push_str("child:\n");
                }
                input.push_str(&"  ".repeat(depth + 1));
                input.push_str("null\n");
            }
            input.push_str("---\nbody");
            let result = parse_frontmatter::<Header>(input.as_bytes());
            let limit = serde_saphyr::Budget::default().max_depth;
            prop_assert_eq!(result.is_ok(), depth < limit);
            if let Ok(header) = result {
                let canonical = format!("---\nname: {}\ndescription: {}\n---\n",
                    serde_json::to_string(&header.name).unwrap(), serde_json::to_string(&header.description).unwrap());
                prop_assert_eq!(parse_frontmatter::<Header>(canonical.as_bytes()).unwrap(), header);
            }
        }
    }
}
