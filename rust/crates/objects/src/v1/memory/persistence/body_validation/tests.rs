use super::*;
use proptest::prelude::*;

fn stored(length: usize) -> Stored {
    Stored {
        info: wire::ObjectInfo {
            size: length as u64,
            ..Default::default()
        }
        .into(),
        body: StoredBody::memory(OwnedBytes::from(vec![1; length])),
    }
}
fn bucket(count: usize) -> Bucket {
    let mut bucket = Bucket {
        info: wire::Bucket::default().into(),
        objects: OrdMap::new(),
    };
    for key in 0..count {
        bucket
            .objects
            .insert(format!("object{key}").into(), stored(16));
    }
    bucket
}
fn upload(length: usize) -> Upload {
    let mut parts = OrdMap::new();
    parts.insert(
        1,
        (wire::UploadedPart::default().into(), stored(length).body),
    );
    Upload {
        bucket: String::from("retained").into(),
        key: String::from("target").into(),
        metadata: None.into(),
        parts,
    }
}
fn seed(count: usize) -> State {
    let mut state = State::default();
    state
        .buckets
        .insert(String::from("retained").into(), bucket(count));
    state
        .uploads
        .insert(String::from("upload").into(), upload(16));
    state
}
fn visit_all(
    next: &State,
    pending: Option<&PendingObject>,
    mut visit: impl FnMut(&StoredBody) -> Result<(), Error>,
) -> Result<(), Error> {
    for bucket in next.buckets.values() {
        for stored in bucket.objects.values() {
            visit(&stored.body)?;
        }
    }
    for upload in next.uploads.values() {
        for (_, body) in upload.parts.values() {
            visit(body)?;
        }
    }
    if let Some(value) = pending.and_then(|change| change.value.as_ref()) {
        visit(&value.body)?;
    }
    Ok(())
}
fn pending(length: usize) -> PendingObject {
    PendingObject {
        bucket: String::from("retained").into(),
        key: String::from("pending").into(),
        value: Some(stored(length)),
    }
}
fn check_sparse(visits: usize) {
    assert_eq!(visits, 1, "pending publication revisited unchanged bodies");
}
fn count_pending(count: usize, broken: bool) -> usize {
    let before = seed(count);
    let next = before.clone();
    let pending = pending(16);
    let mut visits = 0;
    let visit = |_: &StoredBody| {
        visits += 1;
        Ok(())
    };
    if broken {
        visit_all(&next, Some(&pending), visit).unwrap();
    } else {
        visit_changed_bodies(&before, &next, Some(&pending), visit).unwrap();
    }
    visits
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn admitted_subtrees_and_changed_bodies_match_the_full_validation_oracle(
        count in 0usize..12, mode in 0u8..8, length in 0usize..33,
    ) {
        // Every retained body satisfies the immutable store's 16-byte bound.
        let before = seed(count);
        let mut next = before.clone();
        let mut pending_body = None;
        match mode {
            0 => { next.buckets.get_mut("retained").unwrap().objects.insert(String::from("new").into(), stored(length)); }
            1 => { let mut added = bucket(1); added.objects.insert(String::from("new").into(), stored(length)); next.buckets.insert(String::from("added").into(), added); }
            2 => { next.uploads.get_mut("upload").unwrap().parts.insert(1, (wire::UploadedPart::default().into(), stored(length).body)); }
            3 => { next.uploads.insert(String::from("added").into(), upload(length)); }
            4 => { pending_body = Some(pending(length)); }
            5 => { next.buckets.get_mut("retained").unwrap().objects.remove("object0"); }
            6 => { next.uploads.remove("upload"); }
            _ => { next.buckets.remove("retained"); }
        }
        let full = visit_all(&next, pending_body.as_ref(), |body| {
            if body.len() > 16 { Err(QuotaExceeded.into()) } else { Ok(()) }
        });
        let candidate = validate_bodies(&before, &next, pending_body.as_ref(), 16);
        prop_assert_eq!(candidate.map_err(|error| error.code), full.map_err(|error| error.code));
    }

    #[test]
    fn pending_body_validation_is_independent_of_retained_object_count(count in 0usize..257) {
        check_sparse(count_pending(count, false));
    }
}

#[test]
#[should_panic(expected = "pending publication revisited unchanged bodies")]
fn full_scan_control_fails_the_same_work_assertion() {
    check_sparse(count_pending(256, true));
}

#[test]
fn removals_and_every_new_body_path_preserve_the_bound() {
    let before = seed(3);
    for length in [16, 17] {
        let candidate = pending(length);
        assert_eq!(
            validate_bodies(&before, &before, Some(&candidate), 16).is_ok(),
            length == 16
        );
        let mut next = before.clone();
        next.uploads
            .insert(String::from("new").into(), upload(length));
        assert_eq!(
            validate_bodies(&before, &next, None, 16).is_ok(),
            length == 16
        );
    }
    let mut removal = pending(17);
    removal.value = None;
    assert!(validate_bodies(&before, &before, Some(&removal), 16).is_ok());
}
