//! Macro bodies are tokens to syn. This turns every macro in a file into
//! parsed statements, so the alias and constructor analysis sees inside them:
//!
//! - A `macro_rules!` transcriber has its metavariables replaced by
//!   [`METAVARIABLE`] and each repetition expanded once, then must parse as
//!   statements, expressions, impl items or a type. A transcriber that does
//!   not parse makes the scan indeterminate rather than lock free.
//! - Any other macro parses whole as statements or expressions or, for a
//!   custom grammar, yields every statement and expression that parses from
//!   any token position, at every group depth.
use proc_macro2::{Delimiter, Group, Ident, Spacing, Span, TokenStream, TokenTree};
use std::collections::BTreeSet;
use syn::{
    Block, Expr, Item, Macro, Stmt, Token,
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
    visit::{self, Visit},
};

/// Stands in for every `macro_rules!` metavariable.
pub const METAVARIABLE: &str = "__loom_lock_scan_metavariable";

/// Statements parsed out of macro bodies; `template` marks those that came
/// from a `macro_rules!` transcriber.
pub struct Fragment {
    pub stmts: Vec<Stmt>,
    pub template: bool,
}

/// Expands every macro in `file`, including macros inside expansions.
pub fn fragments(file: &syn::File) -> syn::Result<Vec<Fragment>> {
    let mut collector = Collector::default();
    collector.visit_file(file);
    let mut next = 0;
    if let Some(error) = collector.error.take() {
        return Err(error);
    }
    while next < collector.fragments.len() {
        // Taken out while visited: the visit appends to the same list.
        let stmts = std::mem::take(&mut collector.fragments[next].stmts);
        collector.template = collector.fragments[next].template;
        stmts.iter().for_each(|stmt| collector.visit_stmt(stmt));
        collector.fragments[next].stmts = stmts;
        if let Some(error) = collector.error.take() {
            return Err(error);
        }
        next += 1;
    }
    Ok(collector.fragments)
}

#[derive(Default)]
struct Collector {
    fragments: Vec<Fragment>,
    expanded: BTreeSet<(bool, String)>,
    template: bool,
    error: Option<syn::Error>,
}

impl Collector {
    fn push(&mut self, stmts: Vec<Stmt>, template: bool) {
        self.fragments.push(Fragment { stmts, template });
    }

    fn rules(&mut self, tokens: &TokenStream) -> syn::Result<()> {
        let trees: Vec<TokenTree> = tokens.clone().into_iter().collect();
        let mut at = 0;
        while at < trees.len() {
            let rule = (
                &trees[at],
                trees.get(at + 1),
                trees.get(at + 2),
                trees.get(at + 3),
            );
            let (
                TokenTree::Group(_),
                Some(TokenTree::Punct(eq)),
                Some(TokenTree::Punct(gt)),
                Some(TokenTree::Group(body)),
            ) = rule
            else {
                return Err(indeterminate(
                    "a macro_rules! rule is not `(matcher) => {transcriber}`",
                ));
            };
            if eq.as_char() != '=' || eq.spacing() != Spacing::Joint || gt.as_char() != '>' {
                return Err(indeterminate(
                    "a macro_rules! rule is not `(matcher) => {transcriber}`",
                ));
            }
            let Some(stmts) = statements(&substitute(&body.stream())) else {
                return Err(indeterminate(
                    "a macro_rules! transcriber does not parse as statements, expressions, impl items or a type",
                ));
            };
            self.push(stmts, true);
            at += 4;
            if matches!(trees.get(at), Some(TokenTree::Punct(semi)) if semi.as_char() == ';') {
                at += 1;
            }
        }
        Ok(())
    }

    // Everything that parses from any position: a custom grammar can embed
    // statements and expressions anywhere. A start just after `::` is the
    // middle of a path (such as the turbofish of `Vec::<Lock<T>>::new`).
    fn positions(&mut self, tokens: &TokenStream) {
        let trees: Vec<TokenTree> = tokens.clone().into_iter().collect();
        for at in 0..trees.len() {
            if let TokenTree::Group(group) = &trees[at] {
                self.positions(&group.stream());
            }
            if at >= 2 && punct(&trees[at - 1], ':') && punct(&trees[at - 2], ':') {
                continue;
            }
            let rest: TokenStream = trees[at..].iter().cloned().collect();
            if let Some(stmt) = prefix::<Stmt>(rest.clone()) {
                self.push(vec![stmt], self.template);
            }
            if let Some(expr) = prefix::<Expr>(rest) {
                self.push(vec![Stmt::Expr(expr, None)], self.template);
            }
        }
    }
}

impl Visit<'_> for Collector {
    fn visit_macro(&mut self, mac: &Macro) {
        let path: Vec<String> = mac
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        let key = (self.template, format!("{}!{}", path.join("::"), mac.tokens));
        if self.error.is_some() || !self.expanded.insert(key) {
            return;
        }
        if mac.path.is_ident("macro_rules") {
            if let Err(error) = self.rules(&mac.tokens) {
                self.error = Some(error);
            }
        } else if let Some(stmts) = statements(&mac.tokens) {
            self.push(stmts, self.template);
        } else {
            self.positions(&mac.tokens);
        }
        visit::visit_macro(self, mac);
    }
}

fn indeterminate(message: &str) -> syn::Error {
    syn::Error::new(Span::call_site(), format!("indeterminate: {message}"))
}

fn punct(tree: &TokenTree, char: char) -> bool {
    matches!(tree, TokenTree::Punct(p) if p.as_char() == char)
}

fn prefix<T: Parse>(tokens: TokenStream) -> Option<T> {
    (|input: ParseStream<'_>| {
        let value = input.parse::<T>()?;
        input.parse::<TokenStream>()?;
        Ok(value)
    })
    .parse2(tokens)
    .ok()
}

/// Statements, comma-separated expressions, impl items or a type.
fn statements(tokens: &TokenStream) -> Option<Vec<Stmt>> {
    let block = (|input: ParseStream<'_>| Block::parse_within(input)).parse2(tokens.clone());
    if let Ok(stmts) = block {
        return Some(stmts);
    }
    let exprs = Punctuated::<Expr, Token![,]>::parse_terminated.parse2(tokens.clone());
    if let Ok(exprs) = exprs {
        return Some(
            exprs
                .into_iter()
                .map(|expr| Stmt::Expr(expr, None))
                .collect(),
        );
    }
    [
        format!("impl {METAVARIABLE} {{ {tokens} }}"),
        format!("type {METAVARIABLE} = {tokens};"),
    ]
    .iter()
    .find_map(|source| syn::parse_str::<Item>(source).ok())
    .map(|item| vec![Stmt::Item(item)])
}

/// `$name` becomes [`METAVARIABLE`], `$crate` becomes `crate`, and
/// `$( .. ) sep? op` becomes its contents once.
fn substitute(tokens: &TokenStream) -> TokenStream {
    let trees: Vec<TokenTree> = tokens.clone().into_iter().collect();
    let mut output = Vec::new();
    let mut at = 0;
    while at < trees.len() {
        match (&trees[at], trees.get(at + 1)) {
            (TokenTree::Punct(dollar), Some(TokenTree::Ident(name))) if dollar.as_char() == '$' => {
                let name = if name == "crate" {
                    "crate"
                } else {
                    METAVARIABLE
                };
                output.push(TokenTree::Ident(Ident::new(name, Span::call_site())));
                at += 2;
            }
            (TokenTree::Punct(dollar), Some(TokenTree::Group(group)))
                if dollar.as_char() == '$' && group.delimiter() == Delimiter::Parenthesis =>
            {
                output.extend(substitute(&group.stream()));
                at += 2;
                let operator = |tree: Option<&TokenTree>| {
                    tree.is_some_and(|tree| ['*', '+', '?'].iter().any(|char| punct(tree, *char)))
                };
                if operator(trees.get(at)) {
                    at += 1;
                } else if operator(trees.get(at + 1)) {
                    at += 2;
                }
            }
            (TokenTree::Group(group), _) => {
                let mut copy = Group::new(group.delimiter(), substitute(&group.stream()));
                copy.set_span(group.span());
                output.push(TokenTree::Group(copy));
                at += 1;
            }
            (tree, _) => {
                output.push(tree.clone());
                at += 1;
            }
        }
    }
    output.into_iter().collect()
}
