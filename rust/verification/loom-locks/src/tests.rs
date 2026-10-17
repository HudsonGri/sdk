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
