//! Discover model markers in Rust tokens; comments and literals are data.
use proc_macro2::{TokenStream, TokenTree};

pub fn present(source: &str) -> Result<bool, proc_macro2::LexError> {
    let source = source.strip_prefix('\u{feff}').unwrap_or(source);
    let source = if source.starts_with("#!") && !source.starts_with("#![") {
        source.split_once('\n').map_or("", |(_, rest)| rest)
    } else {
        source
    };
    Ok(tokens(source.parse()?))
}

fn tokens(stream: TokenStream) -> bool {
    let input: Vec<_> = stream.into_iter().collect();
    for (index, token) in input.iter().enumerate() {
        if let TokenTree::Ident(ident) = token {
            let name = crate::name(ident);
            if matches!(name.as_str(), "preemption_bound" | "check_with_bound") {
                return true;
            }
            if name == "loom"
                && matches!(input.get(index + 1), Some(TokenTree::Punct(p)) if p.as_char() == ':')
                && matches!(input.get(index + 2), Some(TokenTree::Punct(p)) if p.as_char() == ':')
                && matches!(input.get(index + 3), Some(TokenTree::Ident(i)) if crate::name(i) == "model")
            {
                return true;
            }
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
        fn raw_identifiers_comments_and_macro_wrappers_preserve_discovery(
            gap in prop_oneof![Just(" "), Just("\n"), Just("/* marker */"), Just("// marker\n")],
            raw in any::<bool>(),
            nested in 0usize..5,
        ) {
            let (module, function) = if raw { ("r#loom", "r#model") } else { ("loom", "model") };
            let mut code = format!("{module}{gap}::{gap}{function}{gap}(|| {{}});");
            for _ in 0..nested { code = format!("wrap! {{ {code} }}"); }
            prop_assert_eq!(present(&code).ok(), Some(true));
            let data = format!("/* {code} */ fn plain() {{ let s = {code:?}; }}");
            prop_assert_eq!(present(&data).ok(), Some(false));
        }
    }
}
