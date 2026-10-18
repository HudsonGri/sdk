use syn::{Meta, Token, parse::Parser, punctuated::Punctuated, visit::Visit};

fn contains_test_support(meta: &Meta) -> bool {
    match meta {
        Meta::NameValue(value) if value.path.is_ident("feature") => {
            matches!(&value.value, syn::Expr::Lit(value)
                if matches!(&value.lit, syn::Lit::Str(value) if value.value() == "test-support"))
        }
        Meta::List(list) => Punctuated::<Meta, Token![,]>::parse_terminated
            .parse2(list.tokens.clone())
            .expect("valid cfg predicate")
            .iter()
            .any(contains_test_support),
        _ => false,
    }
}

#[derive(Default)]
struct FeaturePolicy {
    violations: usize,
}
impl<'ast> Visit<'ast> for FeaturePolicy {
    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        if (attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
            && contains_test_support(&attr.meta)
        {
            self.violations += 1;
        }
        syn::visit::visit_attribute(self, attr);
    }
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        if mac.path.is_ident("cfg") {
            let meta: Meta = syn::parse2(mac.tokens.clone()).expect("valid cfg expression");
            if contains_test_support(&meta) {
                self.violations += 1;
            }
        }
        syn::visit::visit_macro(self, mac);
    }
}
fn violations(source: &str) -> usize {
    let ast = syn::parse_file(source).expect("valid Rust source");
    let mut policy = FeaturePolicy::default();
    policy.visit_file(&ast);
    policy.violations
}

#[test]
fn runtime_configuration_is_independent_of_test_support() {
    for source in [
        include_str!("../../config.rs"),
        include_str!("../../../native_environment.rs"),
    ] {
        assert_eq!(violations(source), 0);
    }
}

#[test]
fn negative_controls_reject_feature_attributes_and_expressions() {
    for source in [
        r#"#[cfg(any(test, feature = "test-support"))] fn resolve() {}"#,
        r#"fn resolve() { #[cfg(feature = "test-support")] let _state = (); }"#,
        r##"#[cfg_attr(feature = r#"test-support"#, inline)] fn resolve() {}"##,
        r#"fn resolve() { let _temporary = cfg!(all(feature = "test-support", unix)); }"#,
    ] {
        assert_eq!(violations(source), 1);
    }
    assert_eq!(
        violations(r#"#[cfg(feature = "filesystem-local")] fn resolve() {}"#),
        0
    );
}
