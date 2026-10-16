use crate::{Adapter, ResolvedModel, invalid};
use acyclic_harness::{
    Result,
    model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelDataPart, ToolResultContent,
    },
};
use base64::Engine as _;
use serde_json::{Value, json};

pub(super) struct Media<'p, 'i, 'b> {
    pub provider: &'p ResolvedModel,
    pub files: &'i mut dyn Iterator<Item = &'b [u8]>,
    pub planning: bool,
}
impl Media<'_, '_, '_> {
    pub fn file(
        &mut self,
        file: &acyclic_harness::conversation::FileRef,
        policy: &FileProjectionPolicy,
        allowed: bool,
    ) -> Result<Value> {
        file_part(
            self.provider,
            file,
            policy,
            self.files,
            self.planning,
            allowed,
        )
    }
    pub fn text(&mut self, content: &ModelContent) -> Result<String> {
        text_content(self.provider, content, self.files, self.planning)
    }
    pub fn result(&mut self, content: &ToolResultContent) -> Result<Vec<Value>> {
        result_parts(self.provider, content, self.files, self.planning)
    }
}

fn text_part(adapter: Adapter, text: &str) -> Value {
    match adapter {
        Adapter::Gemini => json!({"text":text}),
        Adapter::Responses => json!({"type":"input_text","text":text}),
        _ => json!({"type":"text","text":text}),
    }
}

fn image_part(
    adapter: Adapter,
    file: &acyclic_harness::conversation::FileRef,
    policy: &acyclic_harness::model::NativeMediaPolicy,
    bytes: &[u8],
) -> Value {
    let data = base64::engine::general_purpose::STANDARD.encode(bytes);
    let mime = file.descriptor().media_type();
    let detail = match policy.intent {
        acyclic_harness::model::NativeMediaIntent::Image { detail } => match detail {
            acyclic_harness::model::ImageDetail::Auto => "auto",
            acyclic_harness::model::ImageDetail::Low => "low",
            acyclic_harness::model::ImageDetail::High => "high",
        },
        _ => "auto",
    };
    match adapter {
        Adapter::Anthropic => {
            json!({"type":"image","source":{"type":"base64","media_type":mime,"data":data}})
        }
        Adapter::Gemini => json!({"inlineData":{"mimeType":mime,"data":data}}),
        Adapter::Responses => {
            json!({"type":"input_image","image_url":format!("data:{mime};base64,{data}"),"detail":detail})
        }
        Adapter::Chat => {
            json!({"type":"image_url","image_url":{"url":format!("data:{mime};base64,{data}"),"detail":detail}})
        }
    }
}

pub(super) fn result_parts<'a>(
    provider: &ResolvedModel,
    content: &ToolResultContent,
    images: &mut dyn Iterator<Item = &'a [u8]>,
    counting: bool,
) -> Result<Vec<Value>> {
    match content {
        ToolResultContent::Json { value } => {
            let text = serde_json::to_string(value).map_err(|error| invalid(error.to_string()))?;
            Ok(vec![text_part(provider.config.adapter, &text)])
        }
        ToolResultContent::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                ModelDataPart::Text { text } => Ok(text_part(provider.config.adapter, text)),
                ModelDataPart::File { file, policy } => {
                    file_part(provider, file, policy, images, counting, true)
                }
            })
            .collect(),
    }
}

pub(super) fn text_content<'a>(
    provider: &ResolvedModel,
    content: &ModelContent,
    images: &mut dyn Iterator<Item = &'a [u8]>,
    counting: bool,
) -> Result<String> {
    if let ModelContent::Text(text) = content {
        return Ok(text.clone());
    }
    content
        .parts()
        .iter()
        .map(|part| match part {
            ModelContentPart::Text { text } => Ok(text.clone()),
            ModelContentPart::File { file, policy } => {
                let part = file_part(provider, file, policy, images, counting, false)?;
                part["text"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("system image was not reduced to a fallback note"))
            }
            _ => Err(invalid("system messages cannot contain tool exchanges")),
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("\n"))
}

pub(super) fn file_part<'a>(
    provider: &ResolvedModel,
    file: &acyclic_harness::conversation::FileRef,
    policy: &FileProjectionPolicy,
    images: &mut dyn Iterator<Item = &'a [u8]>,
    counting: bool,
    allowed: bool,
) -> Result<Value> {
    if matches!(policy, FileProjectionPolicy::BoundedFull) {
        let bytes = match images.next() {
            Some(bytes) => bytes,
            None if counting => &[],
            None => return Err(invalid("text file was not resolved before dispatch")),
        };
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("text file is not UTF-8"))?;
        return Ok(text_part(provider.config.adapter, text));
    }
    if let FileProjectionPolicy::Native(policy) = policy
        && matches!(
            policy.intent,
            acyclic_harness::model::NativeMediaIntent::Image { .. }
        )
    {
        if allowed && crate::images::support(provider, file, policy)?.is_some() {
            let bytes = match images.next() {
                Some(bytes) => bytes,
                None if counting => &[],
                None => return Err(invalid("image file was not resolved before dispatch")),
            };
            return Ok(image_part(provider.config.adapter, file, policy, bytes));
        }
        let note = format!(
            "[Image {} omitted: {}/{} does not support this image format or message role]",
            file.display_name(),
            provider.model.provider,
            provider.model.name
        );
        return Ok(text_part(provider.config.adapter, &note));
    }
    Ok(text_part(
        provider.config.adapter,
        &file_text(file, policy)?,
    ))
}

fn file_text(
    file: &acyclic_harness::conversation::FileRef,
    policy: &FileProjectionPolicy,
) -> Result<String> {
    if !matches!(policy, FileProjectionPolicy::Reference) {
        return Err(invalid(
            "file bytes require an owner-mediated text projection before dispatch",
        ));
    }
    let value = serde_json::to_string(file).map_err(|e| invalid(e.to_string()))?;
    Ok(format!("[file: {value}]"))
}
