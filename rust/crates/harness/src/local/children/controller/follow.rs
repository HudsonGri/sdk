use super::*;

// Only the accepted cursor survives a cancelled select branch. An unfold stream
// can retain a pending read (and its publication permit) after Next is dropped.
// Keep that entire stream inside this future, before any subsequent file write.
pub(in crate::local::children) async fn next(
    files: &Files,
    query: Query,
    cursor: &mut crate::task_volume::Cursor,
) -> Option<crate::task_volume::Result<Change>> {
    let mut changes = match files.inner.follow(query, cursor.clone()).await {
        Ok(changes) => changes,
        Err(error) => return Some(Err(error)),
    };
    let change = changes.next().await;
    if let Some(Ok(change)) = &change {
        *cursor = change.cursor.clone();
    }
    change
}
