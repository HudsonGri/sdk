//! Reports which Rust sources construct or import a lock, for
//! `scripts/check-loom-models.mjs`. Sources are parsed, not pattern matched,
//! so comments, const-generic blocks, shifts inside array lengths, qualified
//! paths and nested generics cannot hide or fake a constructor.
//!
//! The scan is decided on types, not expression shapes ([`finder`]): a
//! binding declared as a lock, or a context-typed constructor
//! (`Default::default()`, `From::from`, `.into()`, ...) whose expected type
//! holds a lock, is a construction however it is wrapped, and one whose
//! expected type the file does not determine is indeterminate. Generated code
//! is excluded structurally ([`macros`]): only allow-listed macros and
//! attributes may appear, and their arguments are parsed with their grammar.
//!
//! stdin: records `path\0source\0`. stdout: one `path\t1`, `path\t0` or
//! `path\t!error` line per record.
mod finder;
mod macros;

use std::{
    collections::BTreeSet,
    io::{Read, Write},
    process::ExitCode,
};
use syn::{
    Stmt, Type, UseTree,
    visit::{self, Visit},
};

const LOCKS: [&str; 5] = ["Mutex", "RwLock", "Condvar", "SyncMutex", "AsyncMutex"];
const LOOM_LOCKS: [&str; 3] = ["Mutex", "RwLock", "Condvar"];

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

/// Whether `source` constructs a lock or imports a Loom lock; an error when
/// it uses generated code or a construction whose type it does not determine.
pub fn scan(source: &str) -> syn::Result<bool> {
    let file = syn::parse_file(source)?;
    let fragments = macros::fragments(&file)?;
    let stmts: Vec<&Stmt> = fragments.iter().flatten().collect();
    let mut names = Names::default();
    names.visit_file(&file);
    stmts.iter().for_each(|stmt| names.visit_stmt(stmt));
    if let Some(error) = names.error {
        return Err(error);
    }
    let found = finder::find(&file, &stmts, &names)?;
    Ok(found || names.loom_import)
}

/// Lock names, renames and type aliases visible anywhere in the file.
#[derive(Default)]
pub struct Names<'ast> {
    renames: Vec<(String, String)>,
    /// `type` items: name, generic parameter count and target.
    aliases: Vec<(String, usize, &'ast Type)>,
    loom_import: bool,
    error: Option<syn::Error>,
}

impl<'ast> Names<'ast> {
    /// Names that denote a lock: the lock types, their renames, and aliases
    /// whose target is one.
    pub fn locks(&self) -> BTreeSet<String> {
        let mut locks: BTreeSet<String> = LOCKS.iter().map(ToString::to_string).collect();
        loop {
            let renamed = self
                .renames
                .iter()
                .find(|(target, alias)| locks.contains(target) && !locks.contains(alias))
                .map(|(_, alias)| alias.clone());
            let aliased = self
                .aliases
                .iter()
                .find(|(alias, _, target)| {
                    !locks.contains(alias)
                        && finder::type_name(target).is_some_and(|name| locks.contains(&name))
                })
                .map(|(alias, _, _)| alias.clone());
            match renamed.or(aliased) {
                Some(name) => locks.insert(name),
                None => return locks,
            };
        }
    }

    /// The target and generic parameter count of the alias `name`.
    pub fn alias(&self, name: &str) -> Option<(usize, &'ast Type)> {
        self.aliases
            .iter()
            .find(|(alias, _, _)| alias == name)
            .map(|(_, count, target)| (*count, *target))
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
                let target = name.ident.to_string();
                self.loom_import |= loom && LOOM_LOCKS.contains(&target.as_str());
                self.bind(prefix, &target, &target);
            }
            UseTree::Rename(rename) => {
                let target = rename.ident.to_string();
                self.loom_import |= loom && LOOM_LOCKS.contains(&target.as_str());
                self.bind(prefix, &target, &rename.rename.to_string());
                self.renames.push((target, rename.rename.to_string()));
            }
            UseTree::Glob(_) => self.loom_import |= loom,
            UseTree::Group(group) => {
                for tree in &group.items {
                    self.walk(tree, prefix);
                }
            }
        }
    }

    fn bind(&mut self, prefix: &[String], target: &str, name: &str) {
        let mut path = prefix.to_vec();
        path.push(target.to_string());
        if macros::redirects(&path, name) && self.error.is_none() {
            self.error = Some(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "`use {} as {name}` rebinds an allow-listed macro name",
                    path.join("::")
                ),
            ));
        }
    }
}

impl<'ast> Visit<'ast> for Names<'ast> {
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        self.walk(&item.tree, &mut Vec::new());
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        self.aliases
            .push((item.ident.to_string(), item.generics.params.len(), &item.ty));
        visit::visit_item_type(self, item);
    }
}

#[cfg(test)]
mod properties;
#[cfg(test)]
mod tests;
