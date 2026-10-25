use super::*;
use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
use proptest::prelude::*;

struct PrivateBody {
    file: FileRef,
    bytes: Vec<u8>,
}

impl crate::conversation::ContentResidencyVerifier for PrivateBody {
    fn read_private_path<'a, 'io>(
        &'a self,
        _volume: &'a VolumeRef,
        _prefix: &'a str,
        _path: &'a str,
        _generation: Option<&'a GenerationRef>,
        _maximum: u64,
        access: crate::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<crate::conversation::ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            Ok((self.file.clone(), self.bytes.clone().into(), None))
        })
    }
}

#[tokio::test]
async fn lazy_read_body_and_result_have_independent_finite_bounds() -> Result<()> {
    for scalar in ["a", "\"", "\\", "\n"] {
        let text = scalar.repeat(4096);
        let template = file_fixture("file.txt", "text/plain")?;
        let file = FileRef::new(
            template.volume().clone(),
            "file.txt".into(),
            "generation-1".into(),
            crate::conversation::FileDescriptor::from_bytes(
                text.as_bytes(),
                "text/plain",
                &mut None,
            )?,
            "file.txt".into(),
            &mut None,
        )?;
        let source = crate::context::PinnedContextPath {
            root: crate::context::ContextRoot {
                volume: file.volume().clone(),
                directory: String::new(),
            },
            generation: GenerationRef::new(file.volume().provider(), b"generation", None, None)?.0,
            path: file.path().into(),
        };
        let expected = serde_json::json!({"file":file,"text":text});
        let envelope = serde_json::to_vec(&expected).unwrap().len() as u64;
        assert!(envelope > 4096);
        for (file_bytes, render_bytes, accepted) in [
            (4096, envelope, true),
            (4095, envelope, false),
            (4096, envelope - 1, false),
        ] {
            let scope = RuntimeScope::new(
                Capabilities::new([file.volume().directory_read_capability("", &mut None)?]),
                Limits {
                    file_bytes,
                    render_bytes,
                    ..Limits::default()
                },
            )?;
            let task = test_context(
                Bindings {
                    scope,
                    content: Some(ContentBindings {
                        reader: Arc::new(PrivateBody {
                            file: file.clone(),
                            bytes: text.as_bytes().to_vec(),
                        }),
                        writer: None,
                    }),
                    ..Bindings::local()
                }
                .build(None)?,
            )?;
            let invocation = crate::tool::ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "boundary".into(),
                name: "read".into(),
                arguments: serde_json::json!({"source":source}),
            };
            let context = ToolContext::new(
                Arc::new(task),
                invocation.operation_id,
                &invocation.call_id,
                None,
            )?;
            let result = crate::tool::files::read()?
                .executor
                .execute(invocation, Some(context))
                .await;
            assert_eq!(
                result.is_ok(),
                accepted,
                "{scalar:?} file={file_bytes} render={render_bytes}: {result:?}"
            );
            if let Ok(result) = result {
                assert_eq!(result.value, expected);
            }
        }
    }
    Ok(())
}

fn grants(path: &str) -> Result<Capabilities> {
    Ok(Capabilities::new([Capability {
        layers: vec![vec![Grant {
            actions: [Action::Read].into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: path.into(),
            },
        }]],
        expires_at: None,
    }
    .to_scope_label()
    .map_err(|error| Error::Invalid(error.to_string()))?]))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn ranged_callers_cannot_replace_the_tasks_scope(prefix in "[a-z]{1,8}") {
        tokio::runtime::Builder::new_current_thread().build()?.block_on(async {
            let narrow = grants(&format!("{prefix}/**"))?;
            let wide = grants("**")?;
            let scope = RuntimeScope::new(narrow, Limits::default())?;
            let context = test_context(Bindings { scope: scope.clone(), ..Bindings::local() }.build(None)?)?;
            let file = file_fixture("secrets/key", "text/plain")?;
            let result = context.read_bound_range(&file, acyclic_fs::ByteRange { offset: 0, length: 1 }, &scope, &wide, None).await;
            assert_eq!(result.unwrap_err().code(), crate::wire::ErrorCode::Unauthorized);
            let source = crate::context::PinnedContextPath {
                root: crate::context::ContextRoot {
                    volume: file.volume().clone(), directory: String::new(),
                },
                generation: GenerationRef::new(file.volume().provider(), b"generation", None, None)?.0,
                path: file.path().into(),
            };
            let replacement = RuntimeScope::new(wide.clone(), Limits::default())?;
            let result = context.read_pinned_source(&source, &replacement, 4096, None).await;
            assert_eq!(result.unwrap_err().code(), crate::wire::ErrorCode::Unauthorized);
            // This was the complete former guard: the backing reader received
            // the replacement grants even though they widened the task scope.
            assert!(read_granted_with_work(&wide, &file, &mut None)?);
            Ok::<_, Error>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
