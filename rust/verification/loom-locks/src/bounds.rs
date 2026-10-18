//! Rust token trees discard comments and preserve groups and literals.
//! Search executable token positions, including macro input, rather than text.
use proc_macro2::{TokenStream, TokenTree};

pub fn bounded(source: &str) -> Result<bool, proc_macro2::LexError> {
    Ok(tokens(source.parse()?))
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
            prop_assert_eq!(bounded(&source.replace("Some", "None")).ok(), Some(false));
            let call = format!("fn f() {{ check_with_bound{}({}Some{}(2), run); }}", gaps[7], gaps[8], gaps[9]);
            prop_assert_eq!(bounded(&call).ok(), Some(true));
            prop_assert_eq!(bounded(&call.replace("Some", "None")).ok(), Some(false));
        }
    }

    #[test]
    fn literals_comments_and_definitions_are_not_bounds() {
        assert_eq!(bounded("fn check_with_bound(_: Option<usize>) {} fn f() { let s = \"preemption_bound = Some(2)\"; /* preemption_bound = Some(2) */ }").ok(), Some(false));
    }
}
