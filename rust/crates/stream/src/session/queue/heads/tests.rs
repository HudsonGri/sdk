use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // A direct slot scan is the independent oracle for the byte-minimum tree.
    // Removal, reuse and resizing cannot hide a fitting head or change fairness.
    #[test]
    fn minimum_tree_matches_the_round_robin_scan(
        operations in prop::collection::vec((0u8..4, 0usize..32, prop::option::of(0usize..1024), 0u64..1024), 0..128),
    ) {
        let mut index = Heads::default();
        let mut slots: Vec<Option<(u64, Option<usize>)>> = Vec::new();
        let mut cursor = 0;
        let mut id = 0u64;
        let mut peak = 0;
        for (action, selected, cost, budget) in operations {
            match action {
                0 => {
                    let slot = index.insert(id).unwrap();
                    if slot == slots.len() { slots.push(None); }
                    prop_assert!(slots[slot].is_none(), "overwrote a live owner");
                    slots[slot] = Some((id,None)); id += 1;
                }
                1 | 2 if !slots.is_empty() => {
                    let slot = selected % slots.len();
                    if let Some((id,_)) = slots[slot] {
                        if action == 1 { index.set(slot,cost).unwrap(); slots[slot] = Some((id,cost)); }
                        else { index.remove(slot).unwrap(); slots[slot] = None; }
                    }
                }
                _ => {
                    let mut expected = None;
                    for slot in (cursor..slots.len()).chain(0..cursor) {
                        if let Some((id,Some(cost))) = slots[slot] && cost as u64 <= budget {
                            expected = Some(id); cursor = (slot + 1) % slots.len(); break;
                        }
                    }
                    prop_assert_eq!(index.next(budget), expected, "tree differs from byte eligibility and fair slot order");
                }
            }
            let live = slots.iter().filter(|slot| slot.is_some()).count();
            peak = peak.max(live);
            prop_assert_eq!(index.owners().collect::<Vec<_>>(), slots.iter().enumerate().filter_map(|(slot,value)| value.map(|(id,_)| (slot,id))).collect::<Vec<_>>());
            prop_assert!(slots.len() <= peak, "cancelled slots were not reused before growing");
        }
    }
}

#[test]
#[should_panic(expected = "tree differs from byte eligibility")]
fn negative_control_stale_parent_minimum_hides_a_fitting_head() {
    let mut index = Heads::default();
    let slot = index.insert(7).unwrap();
    index.insert(9).unwrap();
    index.set(slot, Some(11)).unwrap();
    // Lost parent update: the leaf is eligible but its ancestor stays disabled.
    index.minimum[1] = u64::MAX;
    assert_eq!(
        index.next(11),
        Some(7),
        "tree differs from byte eligibility"
    );
}
