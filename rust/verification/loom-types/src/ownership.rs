use crate::source;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{CRATE_DEF_ID, LocalDefId};
use rustc_middle::ty::{self, TyCtxt};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub(super) fn roots(
    tcx: TyCtxt<'_>,
    root: &Path,
    models: &BTreeSet<String>,
) -> BTreeMap<LocalDefId, String> {
    let mut roots = BTreeMap::new();
    for owner in tcx.hir_body_owners() {
        let Some(file) = source(tcx, root, tcx.def_span(owner)) else {
            continue;
        };
        if !models.contains(&file) {
            continue;
        }
        let mut parent = owner;
        while parent != CRATE_DEF_ID {
            let next = tcx.parent(parent.to_def_id()).expect_local();
            parent = next;
            if source(tcx, root, tcx.def_span(parent)).as_ref() != Some(&file) {
                break;
            }
        }
        roots.insert(parent, file);
    }
    roots
}

pub(super) fn model<'a>(
    tcx: TyCtxt<'_>,
    roots: &'a BTreeMap<LocalDefId, String>,
    mut owner: LocalDefId,
) -> Option<&'a String> {
    loop {
        if let Some(file) = roots.get(&owner) {
            return Some(file);
        }
        if owner == CRATE_DEF_ID {
            return None;
        }
        owner = tcx.parent(owner.to_def_id()).expect_local();
    }
}

pub(super) fn declarations(
    tcx: TyCtxt<'_>,
    roots: &BTreeMap<LocalDefId, String>,
) -> Vec<(String, String, bool)> {
    let mut found = Vec::new();
    for item in tcx.hir_crate_items(()).items() {
        let item = tcx.hir_item(item);
        let owner = item.owner_id.def_id;
        let Some(file) = model(tcx, roots, owner) else {
            continue;
        };
        let mut owned = false;
        match tcx.def_kind(owner) {
            DefKind::Struct | DefKind::Enum | DefKind::Union => {
                let definition = tcx.adt_def(owner);
                let arguments = ty::GenericArgs::identity_for_item(tcx, owner);
                owned = definition.all_fields().any(|field| {
                    crate::types::lock(tcx, owner, field.ty(tcx, arguments), false, 0) == Some(true)
                });
            }
            _ => {}
        }
        if let rustc_hir::ItemKind::Use(path, _) = item.kind {
            if let Some(Res::Def(DefKind::Struct | DefKind::TyAlias, id)) = path.res.type_ns {
                let ty = tcx.type_of(id).instantiate_identity();
                owned |= tcx.crate_name(id.krate).as_str() == "loom"
                    && crate::types::lock(tcx, owner, ty, false, 0) == Some(true);
            }
        }
        found.push((file.clone(), tcx.def_path_str(owner.to_def_id()), owned));
    }
    found
}
