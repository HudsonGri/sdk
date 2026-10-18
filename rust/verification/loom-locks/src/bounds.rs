//! Rust token trees discard comments and preserve groups and literals.
//! Search executable token positions, including macro input, rather than text.
use proc_macro2::{TokenStream, TokenTree};
use syn::{
    Expr,
    visit::{self, Visit},
};

pub fn bounded(source: &str) -> Result<bool, proc_macro2::LexError> {
    let stream: TokenStream = source.parse()?;
    let Ok(file) = syn::parse2::<syn::File>(stream.clone()) else {
        // Invalid Rust cannot execute; token analysis still catches caps in
        // fragments while the lock classifier reports the syntax failure.
        return Ok(tokens(stream));
    };
    let mut check = Bounds(false);
    check.visit_file(&file);
    Ok(check.0)
}

struct Bounds(bool);

fn field(expr: &Expr) -> bool {
    matches!(expr, Expr::Field(value) if matches!(&value.member, syn::Member::Named(name) if crate::name(name) == "preemption_bound"))
}

fn none(expr: &Expr) -> bool {
    match expr {
        Expr::Path(path) => path.qself.is_none() && path.path.is_ident("None"),
        Expr::Paren(inner) => none(&inner.expr),
        Expr::Group(inner) => none(&inner.expr),
        _ => false,
    }
}

impl<'ast> Visit<'ast> for Bounds {
    fn visit_expr(&mut self, expr: &'ast Expr) {
        if let Expr::Assign(value) = expr
            && field(&value.left)
        {
            self.0 |= !none(&value.right);
            self.visit_expr(&value.right);
            return;
        }
        if field(expr) {
            // Reads or indirect mutation leave the cap unresolved.
            self.0 = true;
        }
        if let Expr::Call(call) = expr
            && let Expr::Path(path) = &*call.func
            && let Some(last) = path.path.segments.last()
        {
            match crate::name(&last.ident).as_str() {
                "check_with_bound" => self.0 |= !call.args.first().is_some_and(none),
                "assert_negative_control" => self.0 = true,
                _ => {}
            }
        }
        visit::visit_expr(self, expr);
    }

    fn visit_field_value(&mut self, value: &'ast syn::FieldValue) {
        if matches!(&value.member, syn::Member::Named(name) if crate::name(name) == "preemption_bound")
        {
            self.0 |= !none(&value.expr);
        }
        visit::visit_field_value(self, value);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        self.0 |= tokens(value.tokens.clone());
    }
}

fn ident(token: Option<&TokenTree>, expected: &str) -> bool {
    matches!(token, Some(TokenTree::Ident(value)) if value.to_string().trim_start_matches("r#") == expected)
}

fn tokens(stream: TokenStream) -> bool {
    let input: Vec<_> = stream.into_iter().collect();
    for (index, token) in input.iter().enumerate() {
        if ident(Some(token), "preemption_bound")
            && matches!(input.get(index + 1), Some(TokenTree::Punct(p)) if matches!(p.as_char(), '=' | ':'))
            && !ident(input.get(index + 2), "None")
        {
            return true;
        }
        if !ident(index.checked_sub(1).and_then(|i| input.get(i)), "fn")
            && (ident(Some(token), "check_with_bound")
                || ident(Some(token), "assert_negative_control"))
            && let Some(TokenTree::Group(arguments)) = input.get(index + 1)
            && arguments.delimiter() == proc_macro2::Delimiter::Parenthesis
            && !ident(arguments.stream().into_iter().next().as_ref(), "None")
        {
            return true;
        }
        if let TokenTree::Group(group) = token
            && tokens(group.stream())
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn comments_and_whitespace_do_not_change_bounds(
            gaps in prop::collection::vec(prop_oneof![Just(" "), Just("\n\t"), Just("/* cap */"), Just("/* a /* b */ c */"), Just("// cap\n")], 10),
            raw in any::<bool>(),
        ) {
            let field = if raw { "r#preemption_bound" } else { "preemption_bound" };
            let source = format!("fn f() {{ builder{} .{}{}{}={}Some{}({}2{}); }}", gaps[0], gaps[1], field, gaps[2], gaps[3], gaps[4], gaps[5], gaps[6]);
            prop_assert_eq!(bounded(&source).ok(), Some(true));
            let unbounded = format!("fn f() {{ builder{} .{}{}{}={}None{}; }}", gaps[0], gaps[1], field, gaps[2], gaps[3], gaps[4]);
            prop_assert_eq!(bounded(&unbounded).ok(), Some(false));
            let parsed = syn::parse_file(&source).map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(bounded(&quote::quote!(#parsed).to_string()).ok(), Some(true));
            let call = format!("fn f() {{ check_with_bound{}({}Some{}(2), run); }}", gaps[7], gaps[8], gaps[9]);
            prop_assert_eq!(bounded(&call).ok(), Some(true));
            let unbounded_call = format!("fn f() {{ check_with_bound{}({}None{}, run); }}", gaps[7], gaps[8], gaps[9]);
            prop_assert_eq!(bounded(&unbounded_call).ok(), Some(false));
        }
    }

    #[test]
    fn literals_comments_and_definitions_are_not_bounds() {
        assert_eq!(bounded("fn check_with_bound(_: Option<usize>) {} fn f() { let s = \"preemption_bound = Some(2)\"; /* preemption_bound = Some(2) */ }").ok(), Some(false));
    }
}
