use rustc_hir::def_id::LocalDefId;
use rustc_middle::mono::MonoItem;
use rustc_middle::ty::{self, Ty, TyCtxt};

// Generic helpers are judged for every instance in this test executable.
// No instance, incomplete substitution, or any unresolved instance remains a
// finding. In particular an unused `fn make<T: Default>()` is never clean.
pub(super) fn lock<'tcx>(
    tcx: TyCtxt<'tcx>,
    owner: LocalDefId,
    ty: Ty<'tcx>,
    default: bool,
) -> Option<bool> {
    let mut found = false;
    let mut owned = false;
    let partitions = tcx.collect_and_partition_mono_items(());
    for unit in partitions.codegen_units {
        for item in unit.items().keys() {
            let MonoItem::Fn(instance) = item else {
                continue;
            };
            if instance.def_id() != owner.to_def_id() {
                continue;
            }
            let ty = ty::EarlyBinder::bind(tcx, ty).instantiate(tcx, instance.args);
            owned |= crate::types::lock(tcx, owner, ty, default, 0)?;
            found = true;
        }
    }
    found.then_some(owned)
}
