use super::*;
use crate::conversation::{FileDescriptor, VolumeOwner, VolumeRef};
use proptest::prelude::*;

fn reference(bytes: &[u8], media: &str) -> FileRef {
    FileRef::new(
        VolumeRef::new(
            crate::resources::ProviderRef::new(
                "test".into(),
                "files".into(),
                "1".into(),
                &mut None,
            )
            .unwrap(),
            "task".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
            &mut None,
        )
        .unwrap(),
        ".tasks/t/turns/1/chunks/0".into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, media, &mut None).unwrap(),
        "chunk".into(),
        &mut None,
    )
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn raw_model_files_and_json_replay_the_same_delta(text in ".{0,256}", reasoning in any::<bool>()) {
        let media = if reasoning { REASONING_DELTA } else { TEXT_DELTA };
        let file = reference(text.as_bytes(), media);
        let expected = if reasoning { ModelEvent::Reasoning { delta: text } } else { ModelEvent::Content { delta: text } };
        let event = decode_model_event(&file, match &expected { ModelEvent::Content { delta } | ModelEvent::Reasoning { delta } => delta.as_bytes(), _ => unreachable!() }, None)?.0;
        prop_assert_eq!(&event, &expected);
        let json = crate::contract::canonical_json_bytes(&event)?;
        prop_assert_eq!(decode_model_event(&reference(&json, "application/json"), &json, None)?.0, expected);
    }

    #[test]
    fn raw_model_file_acceptance_is_exactly_utf8(bytes in prop::collection::vec(any::<u8>(), 0..256), reasoning in any::<bool>()) {
        let file = reference(&bytes, if reasoning { REASONING_DELTA } else { TEXT_DELTA });
        prop_assert_eq!(decode_model_event(&file, &bytes, None).is_ok(), std::str::from_utf8(&bytes).is_ok());
        prop_assert!(decode_model_event(&reference(&bytes, "text/plain"), &bytes, None).is_err());
    }
}
