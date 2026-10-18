//! Renders the pinned skill catalog into each request projection.
//!
//! The allowance is the smaller of two bounds. Tokens: 5% of the selected
//! model's usable input (see [`super::skill_catalog::token_allowance`]).
//! Bytes: what the request and its projection leave of `render_bytes`, so the
//! catalog alone can never make a projection fail.
use super::*;
use crate::{
    context::skill_catalog,
    model::{Model, ModelProvider},
};

/// The model a manifest is bound for; its catalog renders against it.
pub(crate) struct CatalogModel<'a> {
    pub(crate) model: &'a Model,
    pub(crate) provider: &'a dyn ModelProvider,
    pub(crate) compaction: &'a CompactionPolicy,
}

impl CatalogModel<'_> {
    /// Token allowance, absent when the provider reports no capacity: then no
    /// token admission or compaction can reject the catalog either.
    fn tokens(&self, io: Option<&PreparationIo>) -> Result<Option<u64>> {
        match crate::executor::stock_context_capacity(self.provider, self.model, io) {
            Ok(capacity) => Ok(Some(skill_catalog::token_allowance(
                capacity.context_tokens,
                match self.compaction {
                    CompactionPolicy::Threshold(policy) => policy.response_reserve_tokens,
                    CompactionPolicy::Disabled => capacity.output_tokens,
                },
            ))),
            Err(error) if error.code() == crate::wire::ErrorCode::Unsupported => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// A request's bytes other than its messages, for the widest output bound.
/// `manifest.tools` is the complete catalog before caller attenuation, so a
/// request's own tool list never encodes longer.
#[derive(Serialize)]
struct RequestFrame<'a> {
    model: &'a Model,
    messages: [(); 0],
    tools: &'a [ToolDefinition],
    max_output_tokens: Option<u32>,
}

pub(super) struct StageCatalog {
    catalog: SkillCatalog,
    /// Message position: after the conventions, before discovered instructions.
    index: usize,
    tokens: Option<u64>,
    request: u64,
}

impl StageCatalog {
    pub(super) fn new(
        catalog: SkillCatalog,
        index: usize,
        model: &CatalogModel<'_>,
        tools: &[ToolDefinition],
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let frame = RequestFrame {
            model: model.model,
            messages: [],
            tools,
            max_output_tokens: Some(u32::MAX),
        };
        let request = crate::executor::with_history_io(io, |work| {
            crate::executor::json::json_byte_length(&frame, u64::MAX, work)
        })? - 2;
        Ok(Self {
            catalog,
            index,
            tokens: model.tokens(io)?,
            request,
        })
    }

    /// JSON bytes the catalog block may occupy in `context`, quotes included.
    pub(super) fn allowance(
        &self,
        context: &Context,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<u64> {
        if context.messages.len() >= limits.context_messages || self.index > context.messages.len()
        {
            return Ok(0);
        }
        let (projection, messages) = crate::executor::with_history_io(io, |work| {
            Ok::<_, Error>((
                crate::executor::json::json_byte_length(context, u64::MAX, work)?,
                crate::executor::json::json_byte_length(&context.messages, u64::MAX, work)?,
            ))
        })?;
        let mut bodies = u64::MAX;
        crate::model::reserve_instruction_bodies(&context.messages, &mut bodies, io)?;
        let bodies = u64::MAX - bodies;
        // The inserted message and its separator, around the block's string.
        let wrapper = crate::executor::with_history_io(io, |work| {
            crate::executor::json::json_byte_length(
                &ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(String::new()),
                },
                u64::MAX,
                work,
            )
        })? - 2
            + 1;
        // The current input index moves by one and may gain a digit.
        let used = (projection + 1)
            .max(self.request.saturating_add(messages))
            .saturating_add(bodies)
            .saturating_add(wrapper);
        Ok(limits
            .render_bytes
            .saturating_sub(used)
            .min(self.tokens.unwrap_or(u64::MAX)))
    }

    /// Inserts the block that fits this projection, if any does.
    pub(super) fn project(
        &self,
        context: &mut Context,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let budget = self.allowance(context, limits, io)?;
        let mut text = String::new();
        if !skill_catalog::render_into(&self.catalog, budget, &mut text, |text, part| {
            crate::executor::stock_string_push(text, part, budget, "skill catalog exceeds its allowance", io)
        })? {
            return Ok(());
        }
        crate::executor::with_history_io(io, |work| {
            let wanted = context.messages.len() + 1;
            compaction::reserve_vector(&mut context.messages, wanted, work)
        })?;
        context.messages.insert(
            self.index,
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text),
            },
        );
        if let Some(current) = &mut context.current_input_index
            && *current as usize >= self.index
        {
            *current += 1;
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        context::{CompactionRetention, ModelContextCapacity, ThresholdCompaction},
        model::{ModelRequest, PreparedModelRequest},
        test_support::ScriptedModelProvider,
    };
    use proptest::prelude::*;

    fn bytes(value: &impl Serialize) -> u64 {
        serde_json::to_vec(value).unwrap().len() as u64
    }

    fn text(role: ModelRole, text: String) -> ModelMessage {
        ModelMessage {
            role,
            content: ModelContent::Text(text),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Property: for every capacity, compaction, catalog and request, the
        // projected block fits both the token and byte allowance, is exactly
        // the catalog's own longest-prefix rendering at that allowance, and
        // leaves a projection and request that fit whenever they did without
        // it. The scripted tokenizer fixture charges it no more than its bytes.
        #[test]
        fn projection_never_fails_on_the_catalog(
            skills in prop::collection::vec(("[a-z]{1,8}", "[ -~é\n<&]{1,60}", "[ -~]{1,30}"), 0..40),
            unlisted in 0u64..3,
            conventions in prop::collection::vec(".{0,80}", 0..3),
            prior in prop::collection::vec(".{0,200}", 0..4),
            prompt in ".{0,200}",
            context_tokens in 3u32..200_000,
            output_tokens in 1u32..65_536,
            reserve in prop::option::of(1u32..65_536),
            extra in 0u64..16_384,
        ) {
            let catalog = SkillCatalog {
                skills: skills.into_iter().map(|(name, description, path)| CatalogSkill { name, description, path }).collect(),
                unlisted,
            };
            let capacity = ModelContextCapacity { context_tokens, output_tokens: output_tokens.min(context_tokens) };
            let provider = ScriptedModelProvider::new([]).with_context_capacity(capacity);
            let compaction = reserve.map_or(CompactionPolicy::Disabled, |response_reserve_tokens| {
                CompactionPolicy::Threshold(ThresholdCompaction {
                    response_reserve_tokens,
                    recent_tokens: 1,
                    retention: CompactionRetention::default(),
                })
            });
            let model = Model::new("fixture".into(), "fixture".into(), "1".into(), serde_json::Value::Null).unwrap();
            let tools = vec![ToolDefinition {
                name: "fixture".into(), revision: "1".into(), description: "tool".into(),
                input_schema: serde_json::json!({"type":"object"}),
                output_schema: serde_json::json!({"type":"object"}),
                projection_schema: serde_json::json!({"type":"object"}),
            }];
            let index = conventions.len();
            let mut messages = conventions.into_iter().map(|value| text(ModelRole::System, value)).collect::<Vec<_>>();
            messages.push(text(ModelRole::User, prompt));
            messages.extend(prior.into_iter().map(|value| text(ModelRole::Assistant, value)));
            let base = Context { messages, metadata: BTreeMap::new(), current_input_index: Some(index as u32) };
            let request = |context: &Context| ModelRequest {
                model: model.clone(), messages: context.messages.clone(), tools: tools.clone(), max_output_tokens: Some(capacity.output_tokens),
            };
            let limits = Limits {
                render_bytes: bytes(&base).max(bytes(&request(&base))) + extra,
                ..Limits::default()
            };
            prop_assert!(validate_projected_context(&base, limits, None).is_ok());
            let selected = CatalogModel { model: &model, provider: &provider, compaction: &compaction };
            let tokens = selected.tokens(None).unwrap().unwrap();
            prop_assert_eq!(tokens, crate::context::skill_catalog::token_allowance(
                capacity.context_tokens,
                reserve.unwrap_or(capacity.output_tokens),
            ));
            let stage = StageCatalog::new(catalog.clone(), index, &selected, &tools, None).unwrap();
            let allowance = stage.allowance(&base, limits, None).unwrap();
            prop_assert!(allowance <= tokens);
            let mut projected = base.clone();
            stage.project(&mut projected, limits, None).unwrap();
            let expected = catalog.render(allowance);
            if let Some(block) = &expected {
                prop_assert_eq!(projected.messages.len(), base.messages.len() + 1);
                prop_assert_eq!(&projected.messages[index], &text(ModelRole::System, block.clone()));
                prop_assert_eq!(projected.current_input_index, Some(index as u32 + 1));
                prop_assert!(bytes(block) <= allowance);
                prop_assert!(validate_projected_context(&projected, limits, None).is_ok());
                prop_assert!(bytes(&request(&projected)) <= limits.render_bytes);
                let count = |context: &Context| {
                    let prepared = PreparedModelRequest::prepare(request(context), limits, None).unwrap();
                    prepared.serialized_token_upper_bound(None).unwrap().0.input_tokens
                };
                let mut empty = base.clone();
                empty.messages.insert(index, text(ModelRole::System, "x".into()));
                prop_assert!(count(&projected) - count(&empty) <= bytes(block) - 2);
            } else {
                prop_assert_eq!(&projected, &base);
            }
        }
    }
}
