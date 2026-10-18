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

/// Every definition of the named functions: its violations (attributes, signature and body) and its body.
struct Functions<'a> {
    names: &'a [&'a str],
    found: Vec<(String, usize, syn::Block)>,
}
impl Functions<'_> {
    fn record(
        &mut self,
        name: &syn::Ident,
        item: impl FnOnce(&mut FeaturePolicy),
        block: &syn::Block,
    ) {
        if let Some(name) = self.names.iter().find(|candidate| *name == **candidate) {
            let mut policy = FeaturePolicy::default();
            item(&mut policy);
            self.found
                .push(((*name).to_owned(), policy.violations, block.clone()));
        }
    }
}
impl<'ast> Visit<'ast> for Functions<'_> {
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.record(
            &item.sig.ident,
            |policy| policy.visit_item_fn(item),
            &item.block,
        );
        syn::visit::visit_item_fn(self, item);
    }
    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.record(
            &item.sig.ident,
            |policy| policy.visit_impl_item_fn(item),
            &item.block,
        );
        syn::visit::visit_impl_item_fn(self, item);
    }
}
fn functions<'a>(source: &str, names: &'a [&'a str]) -> Vec<(String, usize, syn::Block)> {
    let mut functions = Functions {
        names,
        found: Vec::new(),
    };
    functions.visit_file(&syn::parse_file(source).expect("valid Rust source"));
    for name in names {
        assert!(
            functions.found.iter().any(|(found, ..)| found == name),
            "runtime function `{name}` left the policy scope"
        );
    }
    functions.found
}

/// The state, settings and environment resolution path of `LocalHarness::open` and `bind`. Whole files are scanned;
/// files that also expose feature-gated test accessors are scanned per runtime function.
const RUNTIME_FILES: [&str; 5] = [
    include_str!("../../config.rs"),
    include_str!("../../owner.rs"),
    include_str!("../../settings.rs"),
    include_str!("../../bind.rs"),
    include_str!("../../../native_environment.rs"),
];
const RUNTIME_FUNCTIONS: [(&str, &[&str]); 2] = [
    (include_str!("../../mod.rs"), &["open"]),
    (
        include_str!("../../../native_settings.rs"),
        &["new", "configured_codex_home", "codex_directory"],
    ),
];

#[test]
fn runtime_configuration_is_independent_of_test_support() {
    for source in RUNTIME_FILES {
        assert_eq!(violations(source), 0);
    }
    for (source, names) in RUNTIME_FUNCTIONS {
        for (name, violations, _) in functions(source, names) {
            assert_eq!(violations, 0, "`{name}` depends on test-support");
        }
    }
}

/// Negative control for the scope: a violation planted in every scanned file and in the body of every scanned
/// function definition is detected.
#[test]
fn planted_violations_in_every_runtime_unit_are_rejected() {
    for source in RUNTIME_FILES {
        let planted = format!("{source}\n#[cfg(feature = \"test-support\")]\nfn planted() {{}}\n");
        assert_eq!(violations(&planted), 1);
    }
    let statement: syn::Stmt =
        syn::parse_str(r#"#[cfg(feature = "test-support")] let _planted = ();"#)
            .expect("statement");
    for (source, names) in RUNTIME_FUNCTIONS {
        for (name, _, mut block) in functions(source, names) {
            block.stmts.insert(0, statement.clone());
            let mut policy = FeaturePolicy::default();
            policy.visit_block(&block);
            assert_eq!(
                policy.violations, 1,
                "planted violation in `{name}` was missed"
            );
        }
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
