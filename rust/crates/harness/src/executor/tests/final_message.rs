use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Any earlier step text and any UTF-8 delta partition leave the final step
    // unchanged, including when that step has no text. Replay must agree.
    #[test]
    fn final_message_is_independent_of_prior_steps(
        earlier in prop::collection::vec(".{0,16}", 1..4),
        final_chunks in prop::collection::vec(".{0,16}", 0..5),
    ) {
        let mut turns = Vec::new();
        for (index, text) in earlier.iter().enumerate() {
            turns.push(tool_turn([
                ModelEvent::Content { delta: text.clone() },
                echo_call(&format!("call-{index}"), json!({})),
            ]));
        }
        turns.push(tool_turn(final_chunks.iter().map(|delta| ModelEvent::Content { delta: delta.clone() })));
        let provider = Arc::new(ScriptedModelProvider::new(turns).with_token_counter(fixture_token_count));
        let executor = uncompacted_executor(
            Model::new("fixture".into(), "final".into(), "1".into(), Value::Null)?,
            provider.clone(), ContextPipeline::default(),
            echo_tools(Arc::new(FakeTool(AtomicUsize::new(0))), json!({"type":"object"}),
                json!({}), crate::tool::json_projection_schema(json!({})))?,
        ).with_tool_authority(RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]), Limits::default())?, None)?;
        let journal = Journal::default();
        let input = TurnInput { operation_id: OperationId::new(), input: ModelContent::Text("x".into()),
            selected_context: None, max_steps: 4 };
        let actual = futures::executor::block_on(executor.execute(input.clone(), &journal))?;
        let expected = final_chunks.concat();
        prop_assert_eq!(&actual.text, &expected);
        prop_assert_eq!(actual.steps as usize, earlier.len() + 1);
        prop_assert_eq!(actual, futures::executor::block_on(executor.execute(input, &journal))?);
        prop_assert_eq!(provider.requests().len(), earlier.len() + 1);
        // Restoring the former accumulation is detected whenever the prefix is nonempty.
        let prefix = earlier.concat();
        if !prefix.is_empty() {
            prop_assert_ne!(prefix + &expected, expected);
        }
    }
}
