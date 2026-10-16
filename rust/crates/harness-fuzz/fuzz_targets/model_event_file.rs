#![no_main]
use acyclic_harness::{
    AgentId,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    model::ModelEvent,
    resources::ProviderRef,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if input.is_empty() || input.len() > 4096 {
        return;
    }
    let reasoning = input[0] & 1 != 0;
    let bytes = &input[1..];
    let media = if reasoning {
        "text/x-acyclic-model-reasoning"
    } else {
        "text/x-acyclic-model-text"
    };
    let reference = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fuzz".into(), "files".into(), "1".into(), &mut None).unwrap(),
            "task".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )
        .unwrap(),
        ".tasks/t/turns/1/chunks/0".into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, media, &mut None).unwrap(),
        "chunk".into(),
        &mut None,
    )
    .unwrap();
    let result = acyclic_harness::test_support::decode_model_event_file(&reference, bytes);
    match std::str::from_utf8(bytes) {
        Err(_) => assert!(result.is_err()),
        Ok(text) => assert_eq!(
            result.unwrap(),
            if reasoning {
                ModelEvent::Reasoning { delta: text.into() }
            } else {
                ModelEvent::Content { delta: text.into() }
            }
        ),
    }
});
