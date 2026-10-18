use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_middle::mono::MonoItem;
use rustc_middle::ty::{self, Instance, Ty, TyCtxt};
use std::cell::OnceCell;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(super) struct Instances<'tcx> {
    compiled: OnceCell<HashMap<DefId, HashSet<Instance<'tcx>>>>,
}

impl<'tcx> Instances<'tcx> {
    fn compiled(&self, tcx: TyCtxt<'tcx>) -> &HashMap<DefId, HashSet<Instance<'tcx>>> {
        self.compiled.get_or_init(|| {
            let mut compiled = HashMap::<_, HashSet<_>>::new();
            for unit in tcx.collect_and_partition_mono_items(()).codegen_units {
                for item in unit.items().keys() {
                    let MonoItem::Fn(instance) = item else {
                        continue;
                    };
                    compiled
                        .entry(instance.def_id())
                        .or_default()
                        .insert(*instance);
                    // MIR inlining can remove a callee's standalone MonoItem.
                    // Its source scopes still carry the compiler-resolved instance,
                    // expressed in the enclosing body's generic parameters.
                    for scope in tcx.instance_mir(instance.def).source_scopes.iter() {
                        if let Some((inlined, _)) = scope.inlined {
                            let inlined =
                                instance.instantiate_mir(tcx, ty::EarlyBinder::bind(tcx, inlined));
                            compiled
                                .entry(inlined.def_id())
                                .or_default()
                                .insert(inlined);
                        }
                    }
                }
            }
            compiled
        })
    }

    // Generic helpers are judged for every instance in this test executable.
    // No instance, incomplete substitution, or any unresolved instance remains a
    // finding. In particular an unused `fn make<T: Default>()` is never clean.
    pub(super) fn lock(
        &self,
        tcx: TyCtxt<'tcx>,
        owner: LocalDefId,
        ty: Ty<'tcx>,
        default: bool,
    ) -> Option<bool> {
        let mut found = false;
        let mut owned = false;
        if let Some(instances) = self.compiled(tcx).get(&owner.to_def_id()) {
            for instance in instances {
                let ty = ty::EarlyBinder::bind(tcx, ty).instantiate(tcx, instance.args);
                owned |= crate::types::lock(tcx, owner, ty, default, 0)?;
                found = true;
            }
        }
        found.then_some(owned)
    }
}
