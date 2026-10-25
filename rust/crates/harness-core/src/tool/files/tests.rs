use super::*;
use crate::{Capabilities, OperationId, conversation::Limits};

#[test]
fn lazy_source_read_schema_and_decoder_reject_ambiguous_selectors() -> Result<()> {
    let file = crate::tool::tests::file(b"exact")?;
    let (generation, _) =
        crate::resources::GenerationRef::new(file.volume().provider(), b"generation", None, None)?;
    let source = crate::context::PinnedContextPath {
        root: crate::context::ContextRoot {
            volume: file.volume().clone(),
            directory: String::new(),
        },
        generation,
        path: file.path().into(),
    };
    let definition = read()?.definition;
    // Typed references generate their actual wire shape without carrying the
    // schema for unrelated resource kinds into every model request.
    for unused in ["ResourceRef", "ResourceKind"] {
        assert!(definition.input_schema["$defs"].get(unused).is_none());
    }
    let schema = crate::executor::compile_json_schema(&definition.input_schema, "read", &mut None)?;
    for value in [json!({"file":file}), json!({"source":source})] {
        assert!(schema.is_valid(&value));
        assert!(serde_json::from_value::<ReadInput>(value).is_ok());
    }
    for value in [
        json!({"file":file,"source":source}),
        json!({"source":source,"range":{"start":0,"end":1}}),
        json!({"source":source,"path":"other"}),
        json!({}),
    ] {
        assert!(!schema.is_valid(&value));
        assert!(serde_json::from_value::<ReadInput>(value).is_err());
    }
    let mut wrong_kind = json!({"source":source});
    wrong_kind["source"]["generation"]["kind"] = json!("workspace");
    assert!(!schema.is_valid(&wrong_kind));
    assert!(serde_json::from_value::<ReadInput>(wrong_kind).is_err());
    let mut unknown_field = json!({"source":source});
    unknown_field["source"]["generation"]["extra"] = json!(true);
    assert!(!schema.is_valid(&unknown_field));
    assert!(serde_json::from_value::<ReadInput>(unknown_field).is_err());
    Ok(())
}

#[test]
fn lazy_source_read_projection_rejects_substitution() -> Result<()> {
    let file = crate::tool::tests::file(b"exact")?;
    let source = crate::context::PinnedContextPath {
        root: crate::context::ContextRoot {
            volume: file.volume().clone(),
            directory: String::new(),
        },
        generation: crate::resources::GenerationRef::new(
            file.volume().provider(),
            b"generation",
            None,
            None,
        )?
        .0,
        path: file.path().into(),
    };
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "source".into(),
        name: "read".into(),
        arguments: json!({"source":source}),
    };
    for mode in [
        schema::ProjectionMode::Full,
        schema::ProjectionMode::Reference,
    ] {
        for mutation in 0..6 {
            let mut value = json!({"file":file,"text":"exact"});
            match mutation {
                1 => value["file"]["volume"]["id"] = json!("another-volume"),
                2 => value["file"]["path"] = json!("another-path"),
                3 => value["text"] = json!("wrong"),
                4 => value = json!("exact"),
                5 => value = Value::Null,
                _ => {}
            }
            let result =
                FileProjection::Read(mode).project(&invocation, &ToolResult { value }, None);
            assert_eq!(result.is_ok(), mutation == 0);
        }
    }
    Ok(())
}

#[test]
fn lazy_source_read_requires_original_directory_authority() -> Result<()> {
    let file = crate::tool::tests::file(b"exact")?;
    let (generation, _) =
        crate::resources::GenerationRef::new(file.volume().provider(), b"generation", None, None)?;
    let source = crate::context::PinnedContextPath {
        root: crate::context::ContextRoot {
            volume: file.volume().clone(),
            directory: String::new(),
        },
        generation,
        path: file.path().into(),
    };
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "source".into(),
        name: "read".into(),
        arguments: json!({"source":source}),
    };
    let narrow = RuntimeScope::new(
        Capabilities::new([file.read_capability(&mut None)?]),
        Limits::default(),
    )?;
    assert!(
        FileTool::Read
            .preflight(&narrow, &invocation, None)
            .is_err()
    );
    let granted = RuntimeScope::new(
        Capabilities::new([file.volume().directory_read_capability("", &mut None)?]),
        Limits::default(),
    )?;
    assert!(
        FileTool::Read
            .preflight(&granted, &invocation, None)?
            .0
            .is_none()
    );
    // A source declaration never grants execution without its admitted context.
    assert!(
        read()?
            .executor
            .authorize(Some(&granted), None, &invocation, None)
            .is_err()
    );
    Ok(())
}

#[test]
fn context_free_gate_rejects_even_valid_source_grants() -> Result<()> {
    let file = crate::tool::tests::file(b"exact")?;
    let missing = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let grants = Capabilities::new([file.read_capability(&mut None)?]);
    let granted = RuntimeScope::new(grants.clone(), Limits::default())?;
    let mut invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "preflight".into(),
        name: String::new(),
        arguments: Value::Null,
    };
    for (tool, arguments) in [
        (read()?, json!({"file":file})),
        (
            edit()?,
            json!({"file":file,"old_text":"exact","new_text":"changed"}),
        ),
        (
            patch_file(4096, 16)?,
            json!({"file":file,"diff":"@@\n-exact\n+changed\n"}),
        ),
    ] {
        invocation.name = tool.definition.name.clone();
        invocation.arguments = arguments;
        for scope in [None, Some(&missing), Some(&granted)] {
            assert!(matches!(
                tool.executor.authorize(scope, None, &invocation, None),
                Err(Error::Unauthorized(_))
            ));
        }
    }
    invocation.name = "read".into();
    invocation.arguments = json!({"file":file});
    let narrow = RuntimeScope::new(
        grants,
        Limits {
            render_bytes: 4,
            ..Limits::default()
        },
    )?;
    assert!(matches!(
        read()?
            .executor
            .authorize(Some(&narrow), None, &invocation, None),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[test]
fn publication_template_rejects_missing_scope_and_invalid_path_before_dispatch() -> Result<()> {
    let tool = write()?;
    let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "write".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"path":"../escape.txt","text":"body","media_type":"text/plain","display_name":"body"}),
    };
    assert!(matches!(
        tool.executor.authorize(None, None, &invocation, None),
        Err(Error::Unauthorized(_))
    ));
    assert!(
        tool.executor
            .authorize(Some(&scope), None, &invocation, None)
            .is_err()
    );
    Ok(())
}
