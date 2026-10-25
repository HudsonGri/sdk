use super::*;
use acyclic_harness_core::model::{
    Model, ModelContent, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn usage_anchor_counts_new_input_and_rejects_unrelated_resume_prefixes(
        input in ".{1,128}", delta in ".{1,128}", tokens in 1u64..100_000,
        output in 0u64..u64::MAX,
    ) {
        let mut request = ModelRequest {
            model: Model::new("openai".into(), "fixture".into(), "1".into(), json!({})).unwrap(),
            messages: Vec::new(), tools: Vec::new(), max_output_tokens: None,
        };
        request.messages = vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(input) }];
        let first = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let counter = Counter::default();
        counter.observe(&first, &json!({"usage":{"input_tokens":tokens,"output_tokens":output}})).unwrap();
        prop_assert_eq!(counter.count(&first).unwrap(), tokens);
        // Generated assistant content is new input exactly once, independent
        // of the output-token count from the response which produced it.
        request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text(delta) });
        let next = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        // Independent pre-consolidation Serde bytes are the wire oracle. The
        // shared encoder must preserve both the total and every prefix hash.
        let header = serde_json::to_vec(&(&request.model, &request.tools)).unwrap();
        let mut wire = header;
        let mut pretty_wire = serde_json::to_vec_pretty(&(&request.model, &request.tools)).unwrap();
        for (index, message) in request.messages.iter().enumerate() {
            wire.extend(serde_json::to_vec(message).unwrap());
            pretty_wire.extend(serde_json::to_vec_pretty(message).unwrap());
            let (digest, _) = footprint(&next, index + 1).unwrap();
            prop_assert_eq!(digest, *blake3::hash(&wire).as_bytes());
        }
        prop_assert_eq!(footprint(&next, request.messages.len()).unwrap().1, wire.len() as u64);
        // A formatter change is observable even when it decodes to the same
        // values; the real old/new compact byte oracle rejects that control.
        prop_assert_ne!(blake3::hash(&wire), blake3::hash(&pretty_wire));
        let (_, before) = footprint(&first, 1).unwrap();
        let (_, after) = footprint(&next, 1).unwrap();
        prop_assert_eq!(counter.count(&next).unwrap(), tokens + (after-before).div_ceil(3));
        request.messages[0].content = ModelContent::Text("unrelated resume".into());
        let resumed = PreparedModelRequest::prepare(request, Default::default(), None).unwrap();
        let (prefix, total) = footprint(&resumed, 1).unwrap();
        let anchor = counter.0.get().unwrap();
        if anchor.unwrap().digest != prefix {
            prop_assert_eq!(counter.count(&resumed).unwrap(), total.div_ceil(3));
        }
    }
    #[test]
    fn local_delta_never_rounds_down_or_discards_authoritative_usage(
        bytes in 0u64..u64::MAX/2, delta in 0u64..u64::MAX/2, tokens in 0u64..u64::MAX/2,
    ) {
        let snapshot = Usage { digest:[1;32], messages:1, bytes, tokens };
        let counted = estimate(Some(snapshot), [1;32], bytes+delta);
        prop_assert!(counted >= tokens);
        prop_assert!(counted.saturating_sub(tokens).saturating_mul(3) >= delta);
    }
}
