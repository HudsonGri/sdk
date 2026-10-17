use super::scan;

fn constructs(body: &str) -> bool {
    scan(&format!("fn model() {{ {body} }}")).unwrap_or_else(|error| panic!("{body}: {error}"))
}

#[test]
fn every_lock_constructor_spelling_is_found() {
    for body in [
        "let a = SyncMutex::new(0);",
        "let a = crate::sync::RwLock::default();",
        "let a = SyncMutex::<Vec<u8>>::new(Vec::new());",
        "let a = Mutex::<BTreeMap<u8, Vec<Option<u8>>>>::default();",
        "let a = SyncMutex::<()>::new(());",
        "let a = SyncMutex::<(u8, Vec<u8>)>::new((0, Vec::new()));",
        "let a = Mutex::<[u8; 4]>::default();",
        "let a = Mutex::<Box<dyn Fn() -> u8>>::new(Box::new(|| 0));",
        // The five forms that escaped the text matcher.
        "let a = SyncMutex::<[u8; { 4 }]>::new([0; 4]);",
        "let a = SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]);",
        "let a = SyncMutex::<Vec</* > */ u8>>::new(Vec::new());",
        "let a = <SyncMutex<()>>::new(());",
        "let a = SyncMutex /* x */ ::<u8>::new(0);",
        "let a = <SyncMutex<u8> as Default>::default();",
        "let a: SyncMutex<u8> = Default::default();",
        "let a = Arc::new(loom::sync::Condvar::new());",
        "let a = vec![SyncMutex::<u8>::new(0)];",
        // A closure argument, as in a Loom model entry.
        "run(|| { let a = SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]); });",
        "my_macro!(x => SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]));",
    ] {
        assert!(constructs(body), "missed {body}");
    }
}

#[test]
fn lock_types_inside_other_constructors_are_not_locks() {
    for body in [
        "let a = Vec::<SyncMutex::<Vec<u8>>>::new();",
        "let a = Option::<SyncMutex::<Vec<u8>>>::default();",
        "let a = Vec::<SyncMutex<u8>>::new();",
        "let a: Option<SyncMutex<u8>> = Default::default();",
        "// SyncMutex::new(0)\nlet a = 1;",
        "let a = \"SyncMutex::new(0)\";",
        "let a = production::Component::new();",
        "my_macro!(x => Vec::<SyncMutex<u8>>::new());",
        "my_macro!(x => Option::<SyncMutex<u8>>::default(), y);",
    ] {
        assert!(!constructs(body), "flagged {body}");
    }
    let identity =
        "const _: fn(crate::sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;";
    assert_eq!(scan(identity).ok(), Some(false));
}

#[test]
fn aliases_and_loom_imports_are_resolved() {
    let renamed = "use crate::sync::SyncMutex as DataMutex;\nfn m() { DataMutex::new(0); }";
    assert_eq!(scan(renamed).ok(), Some(true));
    let chained =
        "type Lock<T> = crate::sync::Mutex<T>;\ntype Cell<T> = Lock<T>;\nfn m() { Cell::new(0); }";
    assert_eq!(scan(chained).ok(), Some(true));
    let unrelated = "use crate::cell::Cell as Slot;\nfn m() { Slot::new(0); }";
    assert_eq!(scan(unrelated).ok(), Some(false));
    let imported = "use loom::{sync::Mutex as DataMutex, thread};";
    assert_eq!(scan(imported).ok(), Some(true));
    let threads = "use loom::{sync::Arc, thread};";
    assert_eq!(scan(threads).ok(), Some(false));
}

// The four transcriber forms that escaped the token fallback, each compiling
// with rustc, plus alias and import forms inside transcribers.
#[test]
fn macro_rules_transcribers_are_analysed_like_source() {
    for body in [
        "macro_rules! m { () => { <SyncMutex<()>>::new(()) }; }",
        "macro_rules! m { () => { let a: SyncMutex<u8> = Default::default(); }; }",
        "macro_rules! m { () => { use crate::sync::SyncMutex as DataMutex; DataMutex::new(0) }; }",
        "macro_rules! m { () => { type Lock = crate::sync::SyncMutex<u8>; Lock::new(0) }; }",
        "macro_rules! m { ($e:expr) => { <SyncMutex<u8> as Default>::default() }; }",
    ] {
        assert!(constructs(body), "missed {body}");
    }
    let outside =
        "use crate::sync::SyncMutex as DataMutex;\nmacro_rules! m { () => { DataMutex::new(0) }; }";
    assert_eq!(scan(outside).ok(), Some(true));
}

#[test]
fn unknowable_transcribers_are_indeterminate_not_lock_free() {
    for body in [
        "macro_rules! m { ($t:ty) => { <$t>::new(()) }; }",
        "macro_rules! m { ($t:ident) => { $t::default() }; }",
        "macro_rules! m { ($t:ty) => { let a: $t = Default::default(); }; }",
        "macro_rules! m { ($t:ty) => { type Lock = $t; Lock::new(0) }; }",
        "macro_rules! m { () => { a b c }; }",
        "macro_rules! m { () => { }; garbage }",
    ] {
        let verdict = scan(&format!("fn model() {{ {body} }}"));
        assert!(
            verdict
                .as_ref()
                .is_err_and(|error| error.to_string().starts_with("indeterminate")),
            "{body}: {:?}",
            verdict.map_err(|error| error.to_string())
        );
    }
}

// Every lock construction, in every macro wrapping, is found; every
// non-lock construction in the same wrappings is not. The grammar is small
// enough to enumerate exhaustively instead of sampling it.
#[test]
fn generated_constructions_in_every_macro_wrapping() {
    let constructions = [
        "let _a = SyncMutex::new(0);",
        "let _a = crate::sync::RwLock::<u8>::default();",
        "let _a = SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]);",
        "let _a = <SyncMutex<()>>::new(());",
        "let _a = <SyncMutex<u8> as Default>::default();",
        "let _a: SyncMutex<u8> = Default::default();",
        "use crate::sync::SyncMutex as DataMutex; let _a = DataMutex::new(0);",
        "type Lock = crate::sync::Mutex<u8>; let _a = Lock::new(0);",
        "let _a = std::sync::Arc::new(loom::sync::Condvar::new());",
    ];
    let others = [
        "let _a = Vec::<SyncMutex<u8>>::new();",
        "let _a: Option<SyncMutex<u8>> = Default::default();",
        "let _a = Option::<SyncMutex<u8>>::default();",
        "let _a = \"SyncMutex::new(0)\";",
        "use crate::cell::Cell as Slot; let _a = Slot::new(0);",
    ];
    let wrappings = [
        "macro_rules! m { () => { WRAPPED }; }",
        "macro_rules! m { ($e:expr) => { let _x = $e; WRAPPED }; }",
        "macro_rules! m { ($($e:expr),*) => { $( let _x = $e; )* WRAPPED }; }",
        "macro_rules! m { ($($e:expr),*) => { $( WRAPPED let _x = $e; )* }; }",
        "macro_rules! m { ($($e:expr);+) => { $( WRAPPED let _x = $e; );+ }; }",
        "macro_rules! m { (a) => {}; (b) => {{ WRAPPED }}; }",
        "macro_rules! m { () => { vec![{ WRAPPED }] }; }",
        "macro_rules! m { () => { fn inner() { WRAPPED } }; }",
        "macro_rules! m { () => { impl Probe { fn inner(&self) { WRAPPED } } }; }",
        "macro_rules! m { () => { #[test] fn inner() { WRAPPED } }; }",
        "macro_rules! outer { () => { macro_rules! inner { () => { WRAPPED }; } }; }",
        "my_macro!(x => { WRAPPED });",
        "my_macro! { key: value; WRAPPED }",
    ];
    for wrapping in wrappings {
        for construction in constructions {
            let body = wrapping.replace("WRAPPED", construction);
            assert_eq!(
                scan(&format!("fn model() {{ {body} }}")).ok(),
                Some(true),
                "missed {body}"
            );
            assert_eq!(
                scan(&body).ok().or(Some(true)),
                Some(true),
                "item-level {body}"
            );
        }
        for construction in others {
            let body = wrapping.replace("WRAPPED", construction);
            assert_eq!(
                scan(&format!("fn model() {{ {body} }}")).ok(),
                Some(false),
                "flagged {body}"
            );
        }
    }
}
