//! Archive citations retain their owning task and original response-step offsets.
use crate::{TaskId, client::TaskFileViews, file_citations::FileCitations};
use std::{ops::Range, sync::Arc};

pub(super) fn archive(
    segments: &[Arc<str>],
    tasks: &[Option<TaskId>],
    views: &TaskFileViews,
) -> Vec<(Range<usize>, Arc<FileCitations>)> {
    let mut results = Vec::new();
    let mut base = 0;
    for (index, segment) in segments.iter().enumerate() {
        if base != 0 {
            base += 2;
        }
        if let Some(task) = tasks.get(index).and_then(|task| *task)
            && let Some(transcript) = views
                .transcripts
                .get(&format!("AGGREGATE_KIND_TASK:{task}"))
        {
            for snapshot in transcript.file_citations.values() {
                let (Ok(start), Ok(length)) = (
                    usize::try_from(snapshot.output_offset),
                    usize::try_from(snapshot.source_bytes),
                ) else {
                    continue;
                };
                if let Some(end) = start
                    .checked_add(length)
                    .filter(|&end| end <= segment.len())
                {
                    results.push((base + start..base + end, snapshot.clone()));
                }
            }
        }
        base += segment.len();
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_citations::FileCitation;
    use acyclic_markdown::directive::{Resolution, ResolutionError};
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn file_citations_archive_preserves_task_and_step_offsets(
            prefixes in prop::collection::vec("[a-z]{1,32}", 1..8),
        ) {
            let mut segments = Vec::new();
            let mut tasks = Vec::new();
            let mut views = TaskFileViews::default();
            for (index, prefix) in prefixes.iter().enumerate() {
                let task = TaskId::from_bytes((index as u128 + 1).to_be_bytes());
                let raw = format!("::file[file{index}.rs]");
                let snapshot = FileCitations {
                    output_offset: prefix.len() as u64,
                    source_bytes: raw.len() as u64,
                    entries: vec![FileCitation {
                        range: 0..raw.len(), raw: raw.clone(),
                        resolution: Resolution::Error(ResolutionError::Denied), read: None,
                    }],
                    ..Default::default()
                };
                let transcript = views.transcripts.entry(format!("AGGREGATE_KIND_TASK:{task}")).or_default();
                Arc::make_mut(&mut transcript.file_citations).insert(0, Arc::new(snapshot));
                segments.push(Arc::from(format!("{prefix}{raw}")));
                tasks.push(Some(task));
            }
            let source = segments.iter().map(|s: &Arc<str>| s.as_ref()).collect::<Vec<_>>().join("\n\n");
            let results = archive(&segments, &tasks, &views);
            prop_assert_eq!(results.len(), segments.len());
            for (range, snapshot) in results {
                prop_assert_eq!(snapshot.results(&source[range.clone()], range.start).len(), 1);
                // Restoring the old assumption that every response starts at
                // offset zero fails the same exact source-binding assertion.
                let broken = range.start - snapshot.output_offset as usize;
                prop_assert!(snapshot.results(&source[broken..broken + range.len()], broken).is_empty());
            }
        }
    }
}
