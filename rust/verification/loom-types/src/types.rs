use rustc_hir::attrs::{AttributeKind, CfgEntry};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_middle::ty::{self, Ty, TyCtxt, TypingEnv};
use rustc_span::sym;
use std::path::Path;

type Roots = std::collections::HashMap<LocalDefId, String>;

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
            | "take"
    )
}

// A cfg that holds only in test builds (`test`, or a feature named for tests)
// removes its item from the production build.
fn test_cfg(entry: &CfgEntry, positive: bool) -> bool {
    match entry {
        CfgEntry::All(entries, _) | CfgEntry::Any(entries, _) => {
            entries.iter().any(|entry| test_cfg(entry, positive))
        }
        CfgEntry::Not(entry, _) => test_cfg(entry, !positive),
        CfgEntry::NameValue { name, value, .. } => {
            positive
                && (*name == sym::test
                    || (*name == sym::feature
                        && value.is_some_and(|v| v.as_str().contains("test"))))
        }
        CfgEntry::Bool(..) | CfgEntry::Version(..) => false,
    }
}

// Model or test code: a model file, a local file outside a `src` directory
// (integration tests and model crates), a test-support crate, or any item
// enclosed by a test-only cfg. Only definitions outside all of these belong to
// the production build.
fn test_code(tcx: TyCtxt<'_>, root: &Path, roots: &Roots, id: DefId) -> bool {
    if let Some(local) = id.as_local() {
        if crate::ownership::model(tcx, roots, local).is_some()
            || !crate::source(tcx, root, tcx.def_span(id))
                .is_some_and(|file| file.split('/').any(|part| part == "src"))
        {
            return true;
        }
    }
    if tcx.crate_name(id.krate).as_str().contains("test_support") {
        return true;
    }
    let mut item = Some(id);
    while let Some(id) = item {
        for attribute in rustc_hir::attrs::HasAttrs::get_attrs(id, &tcx) {
            if let rustc_hir::Attribute::Parsed(AttributeKind::CfgTrace(entries)) = attribute
                && entries.iter().any(|(entry, _)| test_cfg(entry, true))
            {
                return true;
            }
        }
        item = tcx.opt_parent(id);
    }
    false
}

// A production component's own Default/From construction is the production
// constructor. A constructing impl written in model or test code is not.
fn production<'tcx>(
    tcx: TyCtxt<'tcx>,
    root: &Path,
    roots: &Roots,
    id: DefId,
    ty: Ty<'tcx>,
) -> bool {
    !test_code(tcx, root, roots, id)
        && [sym::Default, sym::From, sym::TryFrom]
            .into_iter()
            .all(|name| {
                tcx.get_diagnostic_item(name).is_none_or(|constructor| {
                    tcx.non_blanket_impls_for_ty(constructor, ty)
                        .all(|implementation| !test_code(tcx, root, roots, implementation))
                })
            })
}

pub(super) fn lock<'tcx>(
    tcx: TyCtxt<'tcx>,
    owner: LocalDefId,
    roots: &Roots,
    root: &Path,
    ty: ty::Unnormalized<'tcx, Ty<'tcx>>,
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
            let id = definition.did();
            if tcx.is_diagnostic_item(sym::Option, id) {
                return Some(false);
            }
            // Default initializes physical fields, not arbitrary type arguments.
            // Owning pointer defaults additionally initialize their pointee.
            let pointee = (tcx.is_diagnostic_item(sym::Arc, id)
                || tcx.is_diagnostic_item(sym::Rc, id)
                || tcx.is_lang_item(id, rustc_hir::LangItem::OwnedBox))
            .then(|| arguments.types().next())
            .flatten();
            if let Some(pointee) = pointee {
                return lock(
                    tcx,
                    owner,
                    roots,
                    root,
                    ty::Unnormalized::new_wip(pointee),
                    true,
                    depth + 1,
                );
            }
            // Only model or test aggregates introduce model state. Initializing
            // a production component through its production Default implementation
            // does not turn its private locks into a substitute model lock.
            if production(tcx, root, roots, id, ty) {
                return Some(false);
            }
            any(definition.all_fields().map(|field| {
                lock(
                    tcx,
                    owner,
                    roots,
                    root,
                    field.ty(tcx, arguments),
                    true,
                    depth + 1,
                )
            }))
        }
        ty::Tuple(elements) if default => any(elements.iter().map(|ty| {
            lock(
                tcx,
                owner,
                roots,
                root,
                ty::Unnormalized::new_wip(ty),
                true,
                depth + 1,
            )
        })),
        ty::Array(element, length) if default => match length.try_to_target_usize(tcx) {
            Some(0) => Some(false),
            Some(_) => lock(
                tcx,
                owner,
                roots,
                root,
                ty::Unnormalized::new_wip(*element),
                true,
                depth + 1,
            ),
            None => None,
        },
        ty::Ref(_, element, _) if default => lock(
            tcx,
            owner,
            roots,
            root,
            ty::Unnormalized::new_wip(*element),
            true,
            depth + 1,
        ),
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
