//! Generated constructions: a lock spelled through any chain of type aliases
//! and renamed imports, made by any constructor form, under any nesting of
//! transparent wrappers, is found; the same program over a non-lock type is
//! not, in a file that writes a lock type so nothing is left unresolved. Cases are generated structurally and rendered to source, so proptest
//! shrinks a miss to its smallest chain, form and wrapping. Failures persist
//! to the checked-in `proptest-regressions/properties.txt` seeds.
use super::scan;
use proptest::prelude::*;

const PREFIXES: [&str; 5] = [
    "",
    "crate::sync::",
    "std::sync::",
    "loom::sync::",
    "tokio::sync::",
];
const LOCKS: [&str; 5] = ["Mutex", "RwLock", "Condvar", "SyncMutex", "AsyncMutex"];

#[derive(Clone, Debug)]
enum Link {
    /// `type A<T> = Current<T>;`
    Alias,
    /// `use Current as R;`
    Rename,
}

#[derive(Clone, Debug)]
enum Wrap {
    Paren,
    Block,
    Unsafe,
    If,
    Match,
    Statement,
}

#[derive(Clone, Debug)]
enum Form {
    New,
    QualifiedDefault,
    TraitDefault,
    /// `let lock: T = init;` with each context-typed initializer.
    Typed(Init),
    /// The tail of `fn make() -> T`.
    Returned(Init),
    /// An argument of `fn take(_: T)`.
    Argument(Init),
    /// A field of `struct Holder { lock: T }`.
    Field(Init),
    /// The first element of a `(T, u8)` binding.
    Tuple(Init),
    /// `let shared: Arc<T> = init;`: `Default` of an `Arc` makes its lock.
    Shared,
}

#[derive(Clone, Copy, Debug)]
enum Init {
    Default,
    From,
    Into,
    MethodInto,
}

#[derive(Clone, Debug)]
struct Case {
    prefix: usize,
    lock: usize,
    chain: Vec<Link>,
    form: Form,
    wraps: Vec<Wrap>,
}

fn init() -> impl Strategy<Value = Init> {
    prop_oneof![
        Just(Init::Default),
        Just(Init::From),
        Just(Init::Into),
        Just(Init::MethodInto),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    let link = prop_oneof![Just(Link::Alias), Just(Link::Rename)];
    let wrap = prop_oneof![
        Just(Wrap::Paren),
        Just(Wrap::Block),
        Just(Wrap::Unsafe),
        Just(Wrap::If),
        Just(Wrap::Match),
        Just(Wrap::Statement),
    ];
    let form = prop_oneof![
        Just(Form::New),
        Just(Form::QualifiedDefault),
        Just(Form::TraitDefault),
        init().prop_map(Form::Typed),
        init().prop_map(Form::Returned),
        init().prop_map(Form::Argument),
        init().prop_map(Form::Field),
        init().prop_map(Form::Tuple),
        Just(Form::Shared),
    ];
    (
        0..PREFIXES.len(),
        0..LOCKS.len(),
        prop::collection::vec(link, 0..4),
        form,
        prop::collection::vec(wrap, 0..4),
    )
        .prop_map(|(prefix, lock, chain, form, wraps)| Case {
            prefix,
            lock,
            chain,
            form,
            wraps,
        })
}

impl Case {
    /// The program over `name` (a lock, or a non-lock stand-in).
    fn render(&self, name: &str) -> String {
        let mut items = String::new();
        let mut current = format!("{}{name}", PREFIXES[self.prefix]);
        for (index, link) in self.chain.iter().enumerate() {
            match link {
                Link::Alias => {
                    items.push_str(&format!("type Alias{index}<T> = {current}<T>;\n"));
                    current = format!("Alias{index}");
                }
                Link::Rename => {
                    let path = if current.contains("::") {
                        current.clone()
                    } else {
                        format!("self::{current}")
                    };
                    items.push_str(&format!("use {path} as Renamed{index};\n"));
                    current = format!("Renamed{index}");
                }
            }
        }
        let ty = format!("{current}<u8>");
        let wrap = |value: &str| {
            self.wraps
                .iter()
                .fold(value.to_string(), |value, wrap| match wrap {
                    Wrap::Paren => format!("({value})"),
                    Wrap::Block => format!("{{ {value} }}"),
                    Wrap::Unsafe => format!("unsafe {{ {value} }}"),
                    Wrap::If => format!("if ready() {{ {value} }} else {{ {value} }}"),
                    Wrap::Match => format!("match 0 {{ 0 => {value}, _ => {value} }}"),
                    Wrap::Statement => format!("{{ let _unrelated = 1; {value} }}"),
                })
        };
        let made = |init: Init| {
            wrap(match init {
                Init::Default => "Default::default()",
                Init::From => "From::from(0)",
                Init::Into => "Into::into(0)",
                Init::MethodInto => "0.into()",
            })
        };
        let body = match &self.form {
            Form::New => format!("let _lock = {};", wrap(&format!("{current}::new(0)"))),
            Form::QualifiedDefault => {
                format!("let _lock = {};", wrap(&format!("<{ty}>::default()")))
            }
            Form::TraitDefault => {
                format!(
                    "let _lock = {};",
                    wrap(&format!("<{ty} as Default>::default()"))
                )
            }
            Form::Typed(init) => format!("let _lock: {ty} = {};", made(*init)),
            Form::Returned(init) => {
                items.push_str(&format!("fn make() -> {ty} {{ {} }}\n", made(*init)));
                "let _lock = make();".into()
            }
            Form::Argument(init) => {
                items.push_str(&format!("fn take(_lock: {ty}) {{}}\n"));
                format!("take({});", made(*init))
            }
            Form::Field(init) => {
                items.push_str(&format!("struct Holder {{ lock: {ty} }}\n"));
                format!("let _holder = Holder {{ lock: {} }};", made(*init))
            }
            Form::Tuple(init) => {
                format!(
                    "let _pair: ({ty}, u8) = {};",
                    wrap(&format!("({}, 0)", made(*init)))
                )
            }
            Form::Shared => {
                format!(
                    "let _shared: std::sync::Arc<{ty}> = {};",
                    made(Init::Default)
                )
            }
        };
        format!("{items}fn model() {{ {body} }}\n")
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn every_generated_construction_is_found(case in case()) {
        let source = case.render(LOCKS[case.lock]);
        let verdict = scan(&source).map_err(|error| error.to_string());
        prop_assert_eq!(verdict, Ok(true), "missed:\n{}", source);
    }

    #[test]
    fn the_same_programs_over_a_non_lock_construct_nothing(case in case()) {
        let source = case.render("Cell");
        let verdict = scan(&source).map_err(|error| error.to_string());
        prop_assert_eq!(verdict, Ok(false), "flagged:\n{}", source);
    }
}
