use super::*;
use crate::test_meter::{Meter, trace};
use std::sync::atomic::Ordering;

#[test]
fn cloned_plain_paths_allocate_once_and_original_storage_fails_the_same_bound() {
    let text = "swarm/agent/task/files";
    let (_, [before, after]) = acyclic_test_support::differential(
        || {
            let path = BaselinePath::new(text).unwrap();
            let cloned = path.clone();
            (path.to_string(), cloned.parent().unwrap().to_string())
        },
        || {
            let path = StreamPath::new(text).unwrap();
            let cloned = path.clone();
            (path.to_string(), cloned.parent().unwrap().to_string())
        },
    );
    eprintln!("path_heap before={before:?} after={after:?}");
    // The two output Strings are identical measured work on both paths.
    let output_bytes = text.len() + text.rfind('/').unwrap();
    assert_eq!(after.count_total, 3);
    assert_eq!(before.count_total, 4);
    let text_allocation = std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::array::<u8>(text.len()).unwrap())
        .unwrap()
        .0
        .pad_to_align()
        .size();
    assert_eq!(after.bytes_total as usize, text_allocation + output_bytes);
    let bound = |heap: acyclic_test_support::AllocationInfo| {
        heap.count_total == 3 && heap.bytes_total as usize == text_allocation + output_bytes
    };
    assert!(bound(after));
    assert!(
        !bound(before),
        "original path clone allocation escaped the same bound"
    );
}

fn native_observation(
    old: bool,
    text: &str,
    memory_limit: u64,
    denied: bool,
) -> (Result<Vec<String>, String>, Vec<trace::Event>, [u64; 3]) {
    let meter = Arc::new(Meter {
        memory_limit,
        ..Meter::default()
    });
    meter.deny_allocation.store(denied, Ordering::SeqCst);
    trace::begin();
    let result = if old {
        BaselinePath::new_native(text, &meter.original()).map(|result| {
            result.map(|path| {
                let clone = path.clone();
                let parent = clone.parent();
                drop(path);
                let output = vec![
                    clone.to_string(),
                    parent.map_or_else(String::new, |path| path.to_string()),
                ];
                drop(clone);
                output
            })
        })
    } else {
        StreamPath::new_native(text, &meter.original()).map(|result| {
            result.map(|path| {
                let clone = path.clone();
                let parent = clone.parent();
                drop(path);
                let output = vec![
                    clone.to_string(),
                    parent.map_or_else(String::new, |path| path.to_string()),
                ];
                drop(clone);
                output
            })
        })
    };
    let result = match result {
        Err(error) => Err(format!("io:{:?}:{error}", error.kind())),
        Ok(Err(error)) => Err(format!("path:{error:?}")),
        Ok(Ok(paths)) => Ok(paths),
    };
    let events = trace::finish();
    let counters =
        [&meter.held, &meter.allocations, &meter.copied].map(|n| n.load(Ordering::SeqCst));
    (result, events, counters)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn native_paths_keep_full_bytes_exact_errors_ordered_admission_and_refunds(text in input(), memory_limit in 0u64..256, denied in any::<bool>()) {
        // Meter, storage, script and observer state are independent each time.
        let (_, [before, after]) = acyclic_test_support::differential(
            || native_observation(true, &text, memory_limit, denied),
            || native_observation(false, &text, memory_limit, denied),
        );
        prop_assert_eq!(before.count_total, after.count_total);
        prop_assert_eq!(before.bytes_total, after.bytes_total);
    }
    #[test]
    fn native_and_plain_paths_have_identical_key_semantics(text in "[a-z0-9]{1,12}(/[a-z0-9]{1,12}){0,5}") {
        let meter = Arc::new(Meter::default());
        let native = StreamPath::new_native(&text, &meter.original()).unwrap().unwrap();
        let plain = StreamPath::new(&text).unwrap();
        prop_assert_eq!(&native, &plain);
        prop_assert_eq!(native.cmp(&plain), std::cmp::Ordering::Equal);
        prop_assert_eq!(fingerprint(&native), fingerprint(&plain));
        let parent = native.parent();
        drop(native);
        if parent.is_some() { prop_assert!(meter.held.load(Ordering::SeqCst) > 0); }
        drop(parent);
        prop_assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
}
