//! Catalog space is the intersection of the saved file, projected context and
//! complete model request bounds. Count borrowed views, never copy their text.
use super::*;
use crate::{
    conversation::{FileDescriptor, VolumeRef},
    model::{ModelMessage, ModelRole},
    tool::ToolDefinition,
};
use serde::{Serialize, ser::SerializeSeq};

#[derive(Serialize)]
struct TextMessage<'a> {
    role: ModelRole,
    content: &'a str,
}

struct Messages<'a> {
    conventions: &'a [String],
    discovered: &'a [ModelMessage],
    prompt: &'a str,
}

impl Serialize for Messages<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(None)?;
        for content in self.conventions {
            sequence.serialize_element(&TextMessage {
                role: ModelRole::System,
                content,
            })?;
        }
        for message in self.discovered {
            sequence.serialize_element(message)?;
        }
        sequence.serialize_element(&TextMessage {
            role: ModelRole::User,
            content: self.prompt,
        })?;
        sequence.end()
    }
}

#[derive(Serialize)]
struct Request<'a> {
    model: &'a Model,
    messages: Messages<'a>,
    tools: &'a [ToolDefinition],
    max_output_tokens: Option<u32>,
}

fn bytes(value: &impl Serialize, maximum: u64) -> Result<u64> {
    crate::executor::json::json_byte_length(value, maximum, &mut None)
}

/// Filesystem publication versions are hex generation keys (at most 32 bytes).
/// Hash bytes use their widest decimal encoding, and the length uses the largest
/// possible manifest. This bounds the eventual reference without publishing a
/// speculative file or depending on its digest/length to size its own contents.
pub(super) fn manifest_reference_bound(
    volume: &VolumeRef,
    path: &str,
    limits: Limits,
) -> Result<FileRef> {
    FileRef::new(
        volume.clone(),
        path.into(),
        "f".repeat(64),
        FileDescriptor::new(
            [255; 32],
            limits.file_bytes.min(limits.render_bytes),
            "application/json".into(),
        )?,
        "context-manifest.json".into(),
        &mut None,
    )
}

pub(super) fn catalog_budget(
    manifest: &ContextManifest,
    model: &Model,
    prompt: &str,
    max_output_tokens: Option<u32>,
    reference: &FileRef,
    limits: Limits,
) -> Result<u64> {
    let maximum = limits.file_bytes.min(limits.render_bytes);
    let file = maximum
        .saturating_sub(bytes(manifest, maximum)?)
        .saturating_sub(1);
    let discovered = manifest.discovery.messages(limits, None)?;
    if manifest.conventions.len() + discovered.len() + 1 >= limits.context_messages {
        return Ok(0);
    }
    let messages = || Messages {
        conventions: &manifest.conventions,
        discovered: &discovered,
        prompt,
    };
    let mut render = limits.render_bytes;
    crate::model::reserve_instruction_bodies(&discovered, &mut render, None)?;
    let request_allowance = render.min(limits.file_bytes);
    let request = bytes(
        &Request {
            model,
            messages: messages(),
            tools: &manifest.tools,
            max_output_tokens,
        },
        request_allowance,
    )?;
    #[derive(Serialize)]
    struct Metadata<'a> {
        #[serde(rename = "context.manifest")]
        manifest: &'a FileRef,
    }
    #[derive(Serialize)]
    struct Projection<'a> {
        messages: Messages<'a>,
        metadata: Metadata<'a>,
        current_input_index: Option<u32>,
    }
    let projection = bytes(
        &Projection {
            messages: messages(),
            metadata: Metadata {
                manifest: reference,
            },
            // Account for the extra catalog message even if it ultimately cannot fit.
            current_input_index: Some(
                u32::try_from(manifest.conventions.len() + discovered.len() + 1)
                    .map_err(|_| Error::Invalid("context message count exceeds u32".into()))?,
            ),
        },
        render,
    )?;
    // frame() includes the JSON string's quotes; the enclosing system message
    // and its array separator are additional bytes in both projections.
    let wrapper = bytes(
        &TextMessage {
            role: ModelRole::System,
            content: "",
        },
        u64::MAX,
    )? - 2
        + 1;
    Ok(file.min(
        request_allowance
            .saturating_sub(request)
            .min(render.saturating_sub(projection))
            .saturating_sub(wrapper),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        conversation::{VolumeClass, VolumeOwner},
        model::{ModelContent, ModelRequest},
        resources::ProviderRef,
    };
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn volume() -> VolumeRef {
        VolumeRef::new(
            ProviderRef::new("fixture".into(), "filesystem".into(), "1".into(), &mut None).unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::new()),
            &mut None,
        )
        .unwrap()
    }

    fn request(
        manifest: &ContextManifest,
        model: &Model,
        prompt: &str,
        output: Option<u32>,
        limits: Limits,
    ) -> ModelRequest {
        let mut messages = manifest
            .conventions
            .iter()
            .map(|text| ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text.clone()),
            })
            .collect::<Vec<_>>();
        messages.extend(manifest.discovery.messages(limits, None).unwrap());
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(prompt.into()),
        });
        ModelRequest {
            model: model.clone(),
            messages,
            tools: manifest.tools.clone(),
            max_output_tokens: output,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]
        // The oracle builds the real production wire types, including arbitrary
        // escaped catalog data, conventions, tools, prompt, model and instructions.
        #[test]
        fn full_built_request_fits_for_any_catalog(
            catalog in prop::collection::vec(("[a-z]{1,8}", ".{0,160}", ".{0,80}"), 0..24),
            conventions in prop::collection::vec(".{0,80}", 0..5),
            prompt in ".{0,180}", tool_text in ".{0,180}", options in ".{0,80}",
            body in 0u64..256, available in 0u64..8192, output in prop::option::of(1u32..u32::MAX),
        ) {
            let model = Model::new("fixture".into(), "fixture".into(), "1".into(), serde_json::json!({"text":options})).unwrap();
            let volume = volume();
            let mut manifest = ContextManifest { conventions, ..Default::default() };
            manifest.tools.push(ToolDefinition {
                name: "fixture".into(), revision: "1".into(), description: tool_text,
                input_schema: serde_json::json!({"type":"object"}), output_schema: serde_json::json!({"type":"object"}), projection_schema: serde_json::json!({"type":"object"}),
            });
            manifest.discovery.instructions.push(FileRef::new(volume.clone(), "AGENTS.md".into(), "f".repeat(64), FileDescriptor::new([255;32], body, "text/plain".into()).unwrap(), "AGENTS.md".into(), &mut None).unwrap().into());
            let limits = Limits::default();
            let reference = manifest_reference_bound(&volume, "inputs/fixture.manifest.json", limits).unwrap();
            let fixed = request(&manifest, &model, &prompt, output, limits);
            let discovered = manifest.discovery.messages(limits, None).unwrap();
            let borrowed = Request { model: &model, messages: Messages { conventions: &manifest.conventions, discovered: &discovered, prompt: &prompt }, tools: &manifest.tools, max_output_tokens: output };
            prop_assert_eq!(serde_json::to_value(&borrowed).unwrap(), serde_json::to_value(&fixed).unwrap());
            let mut bodies = 0;
            for instruction in &manifest.discovery.instructions { bodies += instruction.source().framed_bytes().unwrap(); }
            let fixed_context = crate::context::Context { messages: fixed.messages.clone(), metadata: BTreeMap::from([("context.manifest".into(), reference.clone())]), current_input_index: Some(fixed.messages.len() as u32) };
            let base = bytes(&fixed, u64::MAX).unwrap().max(bytes(&fixed_context, u64::MAX).unwrap()).max(bytes(&manifest, u64::MAX).unwrap()) + bodies;
            let limits = Limits { file_bytes: base + available, render_bytes: base + available, ..limits };
            let reference = manifest_reference_bound(&volume, "inputs/fixture.manifest.json", limits).unwrap();
            let budget = catalog_budget(&manifest, &model, &prompt, output, &reference, limits).unwrap();
            let catalog = catalog.into_iter().map(|(name, description, path)| skills::Listed { name, description, path }).collect::<Vec<_>>();
            manifest.conventions.extend(skills::frame(&catalog, budget));
            let built = request(&manifest, &model, &prompt, output, limits);
            prop_assert!(bytes(&built, u64::MAX).unwrap() + bodies <= limits.render_bytes);
            let context = crate::context::Context { messages: built.messages, metadata: BTreeMap::from([("context.manifest".into(), reference)]), current_input_index: Some(manifest.conventions.len() as u32 + discovered.len() as u32) };
            prop_assert!(crate::context::validate_projected_context(&context, limits, None).is_ok());
            prop_assert!(manifest.encode(limits).is_ok());
        }
    }
}
