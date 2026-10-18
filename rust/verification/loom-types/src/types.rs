use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_middle::ty::{self, Ty, TyCtxt, TypingEnv};

pub(super) fn default_call(tcx: TyCtxt<'_>, method: DefId) -> bool {
    let symbol = tcx.item_name(method);
    let name = symbol.as_str();
    matches!(
        name,
        "default"
            | "from"
            | "into"
            | "try_from"
            | "try_into"
            | "unwrap_or_default"
            | "or_default"
            | "get_or_insert_default"
    )
}

pub(super) fn lock<'tcx>(
    tcx: TyCtxt<'tcx>,
    owner: LocalDefId,
    ty: ty::Unnormalized<TyCtxt<'tcx>, Ty<'tcx>>,
    default: bool,
    depth: usize,
) -> Option<bool> {
    if depth > 32 {
        return None;
    }
    let ty = tcx
        .try_normalize_erasing_regions(TypingEnv::post_analysis(tcx, owner), ty)
        .ok()?;
    match ty.kind() {
        ty::Adt(definition, arguments) => {
            let symbol = tcx.item_name(definition.did());
            let name = symbol.as_str();
            if matches!(
                name,
                "Mutex" | "RwLock" | "Condvar" | "SyncMutex" | "AsyncMutex"
            ) {
                return Some(true);
            }
            if !default {
                return Some(false);
            }
            let path = tcx.def_path_str(definition.did());
            if matches!(
                path.as_str(),
                "alloc::vec::Vec"
                    | "core::option::Option"
                    | "alloc::collections::vec_deque::VecDeque"
                    | "std::collections::hash::map::HashMap"
                    | "std::collections::hash::set::HashSet"
                    | "alloc::collections::btree::map::BTreeMap"
                    | "alloc::collections::btree::set::BTreeSet"
                    | "core::marker::PhantomData"
                    | "alloc::sync::Weak"
                    | "alloc::rc::Weak"
            ) {
                return Some(false);
            }
            any(arguments
                .types()
                .map(ty::Unnormalized::new_wip)
                .chain(
                    definition
                        .all_fields()
                        .map(|field| field.ty(tcx, arguments)),
                )
                .map(|ty| lock(tcx, owner, ty, true, depth + 1)))
        }
        ty::Tuple(elements) if default => any(elements
            .iter()
            .map(|ty| lock(tcx, owner, ty::Unnormalized::new_wip(ty), true, depth + 1))),
        ty::Array(element, length) if default => match length.try_to_target_usize(tcx) {
            Some(0) => Some(false),
            Some(_) => lock(
                tcx,
                owner,
                ty::Unnormalized::new_wip(*element),
                true,
                depth + 1,
            ),
            None => None,
        },
        ty::Param(_)
        | ty::Alias(..)
        | ty::Bound(..)
        | ty::Placeholder(_)
        | ty::Infer(_)
        | ty::Error(_) => None,
        _ => Some(false),
    }
}

fn any(values: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut result = Some(false);
    for value in values {
        match value {
            Some(true) => return Some(true),
            None => result = None,
            Some(false) => {}
        }
    }
    result
}
