//! Reports which Rust sources construct or import a lock, for
//! `scripts/check-loom-models.mjs`. Sources are parsed, not pattern matched,
//! so comments, const-generic blocks, shifts inside array lengths, qualified
//! paths and nested generics cannot hide or fake a constructor.
//!
//! stdin: records `path\0source\0`. stdout: one `path\t1`, `path\t0` or
//! `path\t!error` line per record.
use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    process::ExitCode,
};
use syn::{
    Block, Expr, ExprPath, Local, Macro, Pat, Path, Token, Type, UseTree,
    parse::{ParseStream, Parser},
    punctuated::Punctuated,
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
    let mut names = Names::default();
    names.visit_file(&file);
    let mut finder = Finder {
        locks: names.resolve(),
        found: names.loom_import,
    };
    finder.visit_file(&file);
    Ok(finder.found)
}

/// Lock names, renames and type aliases visible anywhere in the file.
#[derive(Default)]
struct Names {
    aliases: Vec<(String, String)>,
    loom_import: bool,
}

impl Names {
    fn resolve(&self) -> BTreeSet<String> {
        let mut locks: BTreeSet<String> = LOCKS.iter().map(ToString::to_string).collect();
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

    // Macro bodies are tokens to syn: parse them as expressions or
    // statements, and scan the raw tokens when neither form fits.
    fn visit_tokens(&mut self, tokens: &TokenStream) {
        let exprs = Punctuated::<Expr, Token![,]>::parse_terminated.parse2(tokens.clone());
        if let Ok(exprs) = exprs {
            exprs.iter().for_each(|expr| self.visit_expr(expr));
            return;
        }
        let stmts = (|input: ParseStream<'_>| Block::parse_within(input)).parse2(tokens.clone());
        if let Ok(stmts) = stmts {
            stmts.iter().for_each(|stmt| self.visit_stmt(stmt));
            return;
        }
        self.found |= self.token_constructor(tokens);
    }

    // Token fallback: a lock identifier, an optional `::<..>` list balanced
    // at this token-tree level, then `::new(` or `::default(`. Parenthesised,
    // bracketed and braced groups are single trees, so shifts, array lengths
    // and const blocks inside them never count as generic delimiters.
    fn token_constructor(&self, tokens: &TokenStream) -> bool {
        let trees: Vec<TokenTree> = tokens.clone().into_iter().collect();
        for (index, tree) in trees.iter().enumerate() {
            if let TokenTree::Group(group) = tree
                && self.token_constructor(&group.stream())
            {
                return true;
            }
            let TokenTree::Ident(ident) = tree else {
                continue;
            };
            if !self.locks.contains(&ident.to_string()) {
                continue;
            }
            let mut at = index + 1;
            if path_sep(&trees, at) && punct(&trees, at + 2, '<') {
                let mut depth = 0_usize;
                at += 2;
                loop {
                    match trees.get(at) {
                        Some(TokenTree::Punct(p)) if p.as_char() == '<' => depth += 1,
                        Some(TokenTree::Punct(p)) if p.as_char() == '>' && !arrow(&trees, at) => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        None => break,
                        _ => {}
                    }
                    at += 1;
                }
                at += 1;
            }
            let named = matches!(trees.get(at + 2), Some(TokenTree::Ident(name))
                if CONSTRUCTORS.contains(&name.to_string().as_str()));
            let called = matches!(trees.get(at + 3), Some(TokenTree::Group(group))
                if group.delimiter() == Delimiter::Parenthesis);
            if path_sep(&trees, at) && named && called {
                return true;
            }
        }
        false
    }
}

fn punct(trees: &[TokenTree], at: usize, char: char) -> bool {
    matches!(trees.get(at), Some(TokenTree::Punct(p)) if p.as_char() == char)
}

fn path_sep(trees: &[TokenTree], at: usize) -> bool {
    matches!(trees.get(at), Some(TokenTree::Punct(p)) if p.as_char() == ':' && p.spacing() == Spacing::Joint)
        && punct(trees, at + 1, ':')
}

// The `>` of `->`.
fn arrow(trees: &[TokenTree], at: usize) -> bool {
    at > 0
        && matches!(&trees[at - 1], TokenTree::Punct(p) if p.as_char() == '-' && p.spacing() == Spacing::Joint)
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

    fn visit_macro(&mut self, mac: &Macro) {
        self.visit_tokens(&mac.tokens);
        visit::visit_macro(self, mac);
    }
}

#[cfg(test)]
mod tests;
