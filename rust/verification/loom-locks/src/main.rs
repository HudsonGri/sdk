//! Reports which Rust sources construct or import a lock, for
//! `scripts/check-loom-models.mjs`. Sources are parsed, not pattern matched,
//! so comments, const-generic blocks, shifts inside array lengths, qualified
//! paths and nested generics cannot hide or fake a constructor. Macro bodies
//! are expanded into parsed statements first (see [`macros`]), and a
//! `macro_rules!` transcriber that constructs through a metavariable type, or
//! does not parse, makes the scan indeterminate (an error) rather than lock free.
//!
//! stdin: records `path\0source\0`. stdout: one `path\t1`, `path\t0` or
//! `path\t!error` line per record.
mod macros;

use macros::{Fragment, METAVARIABLE};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    process::ExitCode,
};
use syn::{
    Expr, ExprPath, Local, Pat, Path, Type, UseTree,
    visit::{self, Visit},
};

const LOCKS: [&str; 5] = ["Mutex", "RwLock", "Condvar", "SyncMutex", "AsyncMutex"];
const LOOM_LOCKS: [&str; 3] = ["Mutex", "RwLock", "Condvar"];
const CONSTRUCTORS: [&str; 2] = ["new", "default"];

fn main() -> ExitCode {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        return ExitCode::FAILURE;
    }
    let mut fields = input.split('\0');
    let mut output = String::new();
    while let (Some(path), Some(source)) = (fields.next(), fields.next()) {
        let verdict = match scan(source) {
            Ok(found) => u8::from(found).to_string(),
            Err(error) => format!("!{}", error.to_string().replace('\n', " ")),
        };
        output.push_str(&format!("{path}\t{verdict}\n"));
    }
    match std::io::stdout().write_all(output.as_bytes()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

/// Whether `source` constructs a lock or imports a Loom lock.
pub fn scan(source: &str) -> syn::Result<bool> {
    let file = syn::parse_file(source)?;
    let fragments = macros::fragments(&file)?;
    let mut names = Names::default();
    names.visit_file(&file);
    for fragment in &fragments {
        fragment
            .stmts
            .iter()
            .for_each(|stmt| names.visit_stmt(stmt));
    }
    let mut finder = Finder {
        locks: names.resolve(&[]),
        found: names.loom_import,
    };
    finder.visit_file(&file);
    for fragment in &fragments {
        fragment
            .stmts
            .iter()
            .for_each(|stmt| finder.visit_stmt(stmt));
    }
    if finder.found {
        return Ok(true);
    }
    // A transcriber constructing a metavariable type constructs whatever the
    // caller passes in: that is unknown here, not lock free.
    let mut templated = Finder {
        locks: names.resolve(&[METAVARIABLE]),
        found: false,
    };
    for Fragment { stmts, template } in &fragments {
        if *template {
            stmts.iter().for_each(|stmt| templated.visit_stmt(stmt));
        }
    }
    if templated.found {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "indeterminate: a macro_rules! transcriber constructs a metavariable type",
        ));
    }
    Ok(false)
}

/// Lock names, renames and type aliases visible anywhere in the file.
#[derive(Default)]
struct Names {
    aliases: Vec<(String, String)>,
    loom_import: bool,
}

impl Names {
    fn resolve(&self, extra: &[&str]) -> BTreeSet<String> {
        let mut locks: BTreeSet<String> =
            LOCKS.iter().chain(extra).map(ToString::to_string).collect();
        while let Some(alias) = self
            .aliases
            .iter()
            .find(|(target, alias)| locks.contains(target) && !locks.contains(alias))
        {
            locks.insert(alias.1.clone());
        }
        locks
    }

    fn walk(&mut self, tree: &UseTree, prefix: &mut Vec<String>) {
        let loom = prefix.first().is_some_and(|first| first == "loom")
            && prefix.get(1).is_some_and(|second| second == "sync");
        match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                self.walk(&path.tree, prefix);
                prefix.pop();
            }
            UseTree::Name(name) => {
                self.loom_import |= loom && LOOM_LOCKS.contains(&name.ident.to_string().as_str());
            }
            UseTree::Rename(rename) => {
                let target = rename.ident.to_string();
                self.loom_import |= loom && LOOM_LOCKS.contains(&target.as_str());
                self.aliases.push((target, rename.rename.to_string()));
            }
            UseTree::Glob(_) => self.loom_import |= loom,
            UseTree::Group(group) => {
                for tree in &group.items {
                    self.walk(tree, prefix);
                }
            }
        }
    }
}

impl Visit<'_> for Names {
    fn visit_item_use(&mut self, item: &syn::ItemUse) {
        self.walk(&item.tree, &mut Vec::new());
    }

    fn visit_item_type(&mut self, item: &syn::ItemType) {
        if let Some(target) = type_name(&item.ty) {
            self.aliases.push((target, item.ident.to_string()));
        }
        visit::visit_item_type(self, item);
    }
}

// The last segment of an unqualified type path.
fn type_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) if path.qself.is_none() => last(&path.path),
        Type::Paren(inner) => type_name(&inner.elem),
        Type::Group(inner) => type_name(&inner.elem),
        _ => None,
    }
}

fn last(path: &Path) -> Option<String> {
    path.segments
        .last()
        .map(|segment| segment.ident.to_string())
}

struct Finder {
    locks: BTreeSet<String>,
    found: bool,
}

impl Finder {
    fn is_lock(&self, ty: &Type) -> bool {
        type_name(ty).is_some_and(|name| self.locks.contains(&name))
    }

    // `Lock::new`, `Lock::<T>::default`, `<Lock<T>>::new` and
    // `<Lock<T> as Default>::default` name a lock constructor.
    fn constructor(&self, path: &ExprPath) -> bool {
        let segments = &path.path.segments;
        let Some(name) = segments.last().map(|segment| segment.ident.to_string()) else {
            return false;
        };
        if !CONSTRUCTORS.contains(&name.as_str()) {
            return false;
        }
        if let Some(qself) = &path.qself {
            return self.is_lock(&qself.ty);
        }
        segments.len() >= 2
            && self
                .locks
                .contains(&segments[segments.len() - 2].ident.to_string())
    }
}

impl Visit<'_> for Finder {
    fn visit_expr_path(&mut self, path: &ExprPath) {
        self.found |= self.constructor(path);
        visit::visit_expr_path(self, path);
    }

    // `let lock: Lock<T> = Default::default();`
    fn visit_local(&mut self, local: &Local) {
        if let Pat::Type(typed) = &local.pat
            && self.is_lock(&typed.ty)
            && let Some(init) = &local.init
            && let Expr::Call(call) = &*init.expr
            && let Expr::Path(function) = &*call.func
            && last(&function.path).is_some_and(|name| name == "default")
        {
            self.found = true;
        }
        visit::visit_local(self, local);
    }
}

#[cfg(test)]
mod tests;
