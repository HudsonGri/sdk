//! Previously admitted persistent subtrees need no repeated body validation.
use super::*;

impl Journal {
    pub(in crate::v1::memory) fn validate_bodies(
        &self,
        before: &State,
        next: &State,
        pending: Option<&PendingObject>,
    ) -> Result<(), Error> {
        validate_bodies(before, next, pending, self.limits.maximum_object_bytes)
    }
}

fn validate_bodies(
    before: &State,
    next: &State,
    pending: Option<&PendingObject>,
    maximum: u64,
) -> Result<(), Error> {
    visit_changed_bodies(before, next, pending, |body| {
        if body.len() as u64 > maximum {
            Err(QuotaExceeded.into())
        } else {
            Ok(())
        }
    })
}

// `before` was admitted by a previous transaction or bounded durable replay.
// Deferred puts stage their body separately and share both unchanged indexes.
// Other mutations retain the original complete-table validation.
fn visit_changed_bodies(
    before: &State,
    next: &State,
    pending: Option<&PendingObject>,
    mut visit: impl FnMut(&StoredBody) -> Result<(), Error>,
) -> Result<(), Error> {
    if !before.buckets.ptr_eq(&next.buckets) || !before.uploads.ptr_eq(&next.uploads) {
        for bucket in next.buckets.values() {
            for object in bucket.objects.values() {
                visit(&object.body)?;
            }
        }
        for upload in next.uploads.values() {
            for (_, body) in upload.parts.values() {
                visit(body)?;
            }
        }
    }
    if let Some(value) = pending.and_then(|change| change.value.as_ref()) {
        visit(&value.body)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
