use super::super::text_files::SearchResult;
use super::*;
use schema::ProjectionMode;

/// Replaceable model projection for the canonical file result shapes.
#[derive(Clone, Copy)]
pub enum FileProjection {
    /// Exact UTF-8 read result.
    Read(ProjectionMode),
    /// Published immutable reference.
    Publication(ProjectionMode),
    /// Literal matches and omissions.
    Search(ProjectionMode),
}

impl ToolProjection for FileProjection {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            work.remaining()?;
        }
        Ok(())
    }
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        match *self {
            Self::Read(mode) => {
                if invocation.arguments.get("source").is_some() {
                    return project_source(invocation, result, mode, work);
                }
                let text = decode::<String>(&result.value, work)?;
                if mode == schema::ProjectionMode::Full {
                    return schema::project_json(&text.0, work);
                }
                let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let owned_io = work
                    .map(crate::executor::PreparationIo::from_native_work)
                    .transpose()?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let io = owned_io.as_ref();
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let io: Option<&crate::executor::PreparationIo> = None;
                if let Some(range) = input.0.range {
                    if range.end.checked_sub(range.start) != Some(text.0.len() as u64)
                        || range.end > input.0.file.descriptor().byte_length()
                    {
                        return Err(crate::executor::with_history_io(io, |history| {
                            crate::store::guarded_boundary(
                                history,
                                crate::wire::ErrorCode::Invalid,
                                "read result does not match its requested interval",
                            )
                        }));
                    }
                } else {
                    crate::executor::with_history_io(io, |history| {
                        input.0.file.descriptor().verify(text.0.as_bytes(), history)
                    })?;
                }
                if let Some(range) = input.0.range {
                    return schema::summary_reference(
                        &input.0.file,
                        400,
                        format_args!(
                            "Read bytes {}..{}; {} source bytes omitted before and {} after. Selected text is retained in the canonical tool result and omitted from this model projection.",
                            range.start,
                            range.end,
                            range.start,
                            input.0.file.descriptor().byte_length() - range.end,
                        ),
                        work,
                    );
                }
                schema::summary_reference(
                    &input.0.file,
                    160,
                    format_args!(
                        "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                        text.0.len()
                    ),
                    work,
                )
            }
            Self::Publication(mode) => {
                let value = decode::<FileResult>(&result.value, work)?;
                match mode {
                    schema::ProjectionMode::Full => schema::project_json(&value.0, work),
                    schema::ProjectionMode::Reference => schema::project_reference(
                        &value.0.file,
                        "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.",
                        work,
                    ),
                }
            }
            Self::Search(mode) => {
                let value = decode::<SearchResult>(&result.value, work)?;
                if mode == ProjectionMode::Full {
                    return schema::project_json(&value.0, work);
                }
                schema::summary_reference(
                    &value.0.file,
                    480,
                    format_args!(
                        "Literal search completed: {} total matches, {} retained positions, {} positions omitted from the canonical result, {} byte comparisons. Query and retained positions are retained in the canonical tool result and omitted from this model projection.",
                        value.0.matches.total_matches,
                        value.0.matches.matches.len(),
                        value.0.matches.omitted_matches,
                        value.0.matches.work,
                    ),
                    work,
                )
            }
        }
    }
}

fn project_source(
    invocation: &ToolInvocation,
    result: &ToolResult,
    mode: ProjectionMode,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    let input = decode::<ReadSourceInput>(&invocation.arguments, work)?;
    let result = decode::<ReadSourceResult>(&result.value, work)?;
    if result.0.file.volume() != &input.0.source.root.volume
        || result.0.file.path() != input.0.source.path
    {
        return Err(super::executor::rejection(
            work,
            "source read result has another identity",
        ));
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let owned_io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = owned_io.as_ref();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let io: Option<&crate::executor::PreparationIo> = None;
    crate::executor::with_history_io(io, |history| {
        input.0.source.validate(history)?;
        result
            .0
            .file
            .descriptor()
            .verify(result.0.text.as_bytes(), history)
    })?;
    if mode == ProjectionMode::Full {
        return schema::project_json(&result.0, work);
    }
    schema::summary_reference(
        &result.0.file,
        160,
        format_args!(
            "Read {} UTF-8 bytes from the pinned private source. Exact text remains in the canonical tool result.",
            result.0.text.len()
        ),
        work,
    )
}
