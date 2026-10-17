use super::*;

pub(in crate::local::children) struct Follow {
    pub(in crate::local::children) cursor: crate::task_volume::Cursor,
    idle: Option<(Query, crate::task_volume::ChangeStream)>,
}
impl Follow {
    pub(in crate::local::children) fn new(cursor: crate::task_volume::Cursor) -> Self {
        Self { cursor, idle: None }
    }
}

// Take the entire stream before polling it. Cancellation drops every pending
// read/permit; only a stream which returned a record can go back in the slot.
// Reusing that idle stream avoids revalidating its accepted anchor per record.
pub(in crate::local::children) async fn next(
    files: &Files,
    query: Query,
    follow: &mut Follow,
) -> Option<crate::task_volume::Result<Change>> {
    let mut changes = match follow.idle.take() {
        Some((previous, changes)) if previous == query => changes,
        _ => match open(files, query.clone(), follow.cursor.clone()).await {
            Ok(changes) => changes,
            Err(error) => return Some(Err(error)),
        },
    };
    let change = changes.next().await;
    if let Some(Ok(change)) = &change {
        follow.cursor = change.cursor.clone();
        follow.idle = Some((query, changes));
    }
    change
}

async fn open(
    files: &Files,
    query: Query,
    cursor: crate::task_volume::Cursor,
) -> crate::task_volume::Result<crate::task_volume::ChangeStream> {
    #[cfg(test)]
    crate::test_support::differential::record("child-follow-open", 1);
    files.inner.follow(query, cursor).await
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
