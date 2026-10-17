//! Lock construction, decided on types. A lock value comes into existence
//! only through
//!
//! - an associated function named on a lock type (`Lock::new`,
//!   `<Lock<T>>::default`, `Self::new` in an impl for a lock, ...), or
//! - a context-typed constructor whose type is inferred from where its value
//!   goes: `Default::default()`, `From::from`, `Into::into`, `TryFrom`,
//!   `TryInto`, `.into()`, `.try_into()`, `<_>::default()` and associated
//!   constructors of a generic parameter.
//!
//! The first is found by name. For the second, the expected type is carried
//! from its context through parentheses, blocks, `unsafe`, `if`, `match`,
//! tuples, arrays and references: typed `let`s, statics, consts, function
//! returns and arguments of functions in the file, closures with a return
//! type, and fields of structs in the file. An expected type holding a lock
//! is a construction; one the file does not determine is indeterminate when
//! the file writes a lock type by value anywhere (an annotation, a turbofish,
//! a field, `Arc<Lock<T>>`), since that type can direct an inference to a
//! lock. A file that writes none can only receive a lock type from a
//! signature in another file, which is where a per-file scan ends, as it does
//! for aliases reached through `use super::*`.
//! Independently, a binding, static, const or field declared with a lock type
//! is a construction whatever its initializer.
use crate::{Names, macros::indeterminate};
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use syn::{
    Block, Expr, ExprPath, Fields, FnArg, GenericParam, Generics, Local, Pat, Path, ReturnType,
    Signature, Stmt, Type,
    visit::{self, Visit},
};

/// Containers whose `Default` holds no value, so a lock argument is not
/// constructed by it.
const EMPTY: [&str; 9] = [
    "Option",
    "Vec",
    "VecDeque",
    "HashMap",
    "BTreeMap",
    "HashSet",
    "BTreeSet",
    "PhantomData",
    "Weak",
];
const TRAITS: [&str; 5] = ["Default", "From", "Into", "TryFrom", "TryInto"];
const INFERRED: [&str; 5] = ["default", "from", "into", "try_from", "try_into"];
const GENERIC: [&str; 4] = ["new", "default", "from", "try_from"];
/// Methods whose result type is inferred from where it goes, or which make
/// a `Default` value of a type parameter.
const METHODS: [&str; 5] = [
    "into",
    "try_into",
    "unwrap_or_default",
    "or_default",
    "get_or_insert_default",
];

/// Whether `file` and the macro statements `stmts` construct a lock.
pub fn find<'ast>(
    file: &'ast syn::File,
    stmts: &[&'ast Stmt],
    names: &Names<'ast>,
) -> syn::Result<bool> {
    let mut items = Items::default();
    items.visit_file(file);
    stmts.iter().for_each(|stmt| items.visit_stmt(stmt));
    let mut finder = Finder {
        names,
        locks: names.locks(),
        traits: TRAITS
            .iter()
            .map(ToString::to_string)
            .chain(
                names
                    .renames
                    .iter()
                    .filter(|(target, _)| TRAITS.contains(&target.as_str()))
                    .map(|(_, alias)| alias.clone()),
            )
            .collect(),
        items,
        found: false,
        mentions: false,
        unresolved: Vec::new(),
        expected: HashMap::new(),
        callees: HashSet::new(),
        generics: Vec::new(),
        returns: Vec::new(),
        selves: Vec::new(),
    };
    finder.visit_file(file);
    stmts.iter().for_each(|stmt| finder.visit_stmt(stmt));
    match finder.unresolved.first() {
        Some(_) if finder.found || !finder.mentions => Ok(finder.found),
        Some(expr) => Err(indeterminate(&format!(
            "the type `{expr}` constructs is inferred from context the scan cannot resolve; name the type"
        ))),
        None => Ok(finder.found),
    }
}

/// The last segment of an unqualified type path.
pub fn type_name(ty: &Type) -> Option<String> {
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

fn key(expr: &Expr) -> *const Expr {
    expr
}

/// Free functions, associated functions and structs defined in the file. A
/// name defined twice is ambiguous and resolves to nothing.
#[derive(Default)]
struct Items<'ast> {
    functions: BTreeMap<String, Option<&'ast Signature>>,
    structs: BTreeMap<String, Option<&'ast Fields>>,
    owner: Vec<Option<String>>,
}

impl<'ast> Items<'ast> {
    fn function(&mut self, name: String, signature: &'ast Signature) {
        self.functions
            .entry(name)
            .and_modify(|seen| *seen = None)
            .or_insert(Some(signature));
    }
}

impl<'ast> Visit<'ast> for Items<'ast> {
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.function(item.sig.ident.to_string(), &item.sig);
        visit::visit_item_fn(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        self.owner.push(type_name(&item.self_ty));
        visit::visit_item_impl(self, item);
        self.owner.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if let Some(Some(owner)) = self.owner.last() {
            self.function(format!("{owner}::{}", item.sig.ident), &item.sig);
        }
        visit::visit_impl_item_fn(self, item);
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        self.structs
            .entry(item.ident.to_string())
            .and_modify(|seen| *seen = None)
            .or_insert(Some(&item.fields));
        visit::visit_item_struct(self, item);
    }
}

#[derive(Clone, Copy)]
enum Expect<'ast> {
    Type(&'ast Type),
    Known(bool),
}

struct Finder<'a, 'ast> {
    names: &'a Names<'ast>,
    locks: BTreeSet<String>,
    /// The conversion traits and their renames.
    traits: BTreeSet<String>,
    items: Items<'ast>,
    found: bool,
    /// Whether the file writes a lock type by value.
    mentions: bool,
    unresolved: Vec<String>,
    expected: HashMap<*const Expr, Expect<'ast>>,
    callees: HashSet<*const Expr>,
    generics: Vec<String>,
    returns: Vec<Option<Expect<'ast>>>,
    selves: Vec<&'ast Type>,
}

impl<'ast> Finder<'_, 'ast> {
    fn generic(&self, name: &str) -> bool {
        self.generics.iter().any(|generic| generic == name)
    }

    /// Whether `ty` is itself a lock; `None` when it is a generic parameter,
    /// `_` or `impl Trait`.
    fn is_lock(&self, ty: &Type) -> Option<bool> {
        match ty {
            Type::Paren(inner) => self.is_lock(&inner.elem),
            Type::Group(inner) => self.is_lock(&inner.elem),
            Type::Infer(_) | Type::ImplTrait(_) | Type::Macro(_) | Type::Verbatim(_) => None,
            Type::Path(path) if path.qself.is_some() => None,
            Type::Path(path) => {
                let name = last(&path.path)?;
                if path.path.segments.len() == 1 && self.generic(&name) {
                    None
                } else if path.path.is_ident("Self") {
                    self.selves.last().and_then(|ty| self.is_lock(ty))
                } else {
                    Some(self.locks.contains(&name))
                }
            }
            _ => Some(false),
        }
    }

    /// Whether a value of type `ty` made by `Default` holds a lock by value:
    /// a lock, a tuple or array of one, an alias of one, or a type with a lock
    /// argument other than an [`EMPTY`] container.
    fn holds(&self, ty: &Type, depth: usize) -> Option<bool> {
        if depth > 16 {
            return None;
        }
        match ty {
            Type::Paren(inner) => self.holds(&inner.elem, depth),
            Type::Group(inner) => self.holds(&inner.elem, depth),
            Type::Tuple(tuple) => any(tuple.elems.iter().map(|ty| self.holds(ty, depth + 1))),
            Type::Array(array) => self.holds(&array.elem, depth + 1),
            Type::Path(path) if path.qself.is_none() => {
                if self.is_lock(ty)? {
                    return Some(true);
                }
                let segment = path.path.segments.last()?;
                let name = segment.ident.to_string();
                let arguments: Vec<&Type> = match &segment.arguments {
                    syn::PathArguments::AngleBracketed(angle) => angle
                        .args
                        .iter()
                        .filter_map(|arg| match arg {
                            syn::GenericArgument::Type(ty) => Some(ty),
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                let argued = any(arguments.iter().map(|ty| self.holds(ty, depth + 1)));
                let aliased = if path.path.segments.len() == 1
                    && let Some((_, target)) = self.names.alias(&name)
                {
                    self.holds(target, depth + 1)
                } else {
                    Some(false)
                };
                if EMPTY.contains(&name.as_str()) {
                    aliased
                } else {
                    any([argued, aliased].into_iter())
                }
            }
            _ => self.is_lock(ty),
        }
    }

    fn assign(&mut self, expr: &'ast Expr, expect: Option<Expect<'ast>>) {
        let element = |index: usize| match expect {
            Some(Expect::Type(Type::Tuple(tuple))) => {
                tuple.elems.iter().nth(index).map(Expect::Type)
            }
            Some(Expect::Type(Type::Array(array))) => Some(Expect::Type(&array.elem)),
            _ => None,
        };
        match expr {
            Expr::Paren(inner) => self.assign(&inner.expr, expect),
            Expr::Group(inner) => self.assign(&inner.expr, expect),
            Expr::Block(block) => self.tail(&block.block, expect),
            Expr::Unsafe(block) => self.tail(&block.block, expect),
            Expr::If(branch) => {
                self.tail(&branch.then_branch, expect);
                if let Some((_, otherwise)) = &branch.else_branch {
                    self.assign(otherwise, expect);
                }
            }
            Expr::Match(choice) => {
                for arm in &choice.arms {
                    self.assign(&arm.body, expect);
                }
            }
            Expr::Tuple(tuple) => {
                for (index, elem) in tuple.elems.iter().enumerate() {
                    self.assign(elem, element(index));
                }
            }
            Expr::Array(array) => {
                for elem in &array.elems {
                    self.assign(elem, element(0));
                }
            }
            Expr::Repeat(repeat) => self.assign(&repeat.expr, element(0)),
            Expr::Reference(reference) => {
                let inner = match expect {
                    Some(Expect::Type(Type::Reference(ty))) => Some(Expect::Type(&ty.elem)),
                    _ => None,
                };
                self.assign(&reference.expr, inner);
            }
            _ => {
                if let Some(expect) = expect {
                    self.expected.insert(key(expr), expect);
                }
            }
        }
    }

    fn tail(&mut self, block: &'ast Block, expect: Option<Expect<'ast>>) {
        if let Some(Stmt::Expr(expr, None)) = block.stmts.last() {
            self.assign(expr, expect);
        }
    }

    fn output(output: &'ast ReturnType) -> Expect<'ast> {
        match output {
            ReturnType::Default => Expect::Known(false),
            ReturnType::Type(_, ty) => Expect::Type(ty),
        }
    }

    /// Visits a function body with its generics and return type in scope.
    fn function(
        &mut self,
        signature: &'ast Signature,
        block: &'ast Block,
        visit: impl FnOnce(&mut Self),
    ) {
        let scope = self.generics.len();
        self.push(&signature.generics);
        let output = Self::output(&signature.output);
        self.tail(block, Some(output));
        self.returns.push(Some(output));
        visit(self);
        self.returns.pop();
        self.generics.truncate(scope);
    }

    fn push(&mut self, generics: &Generics) {
        for param in &generics.params {
            if let GenericParam::Type(param) = param {
                self.generics.push(param.ident.to_string());
            }
        }
    }

    fn judge(&mut self, expr: &'ast Expr) {
        let verdict = match self.expected.get(&key(expr)) {
            Some(Expect::Known(lock)) => Some(*lock),
            Some(Expect::Type(ty)) => self.holds(ty, 0),
            None => None,
        };
        match verdict {
            Some(lock) => self.found |= lock,
            None => self.unresolved.push(expr.to_token_stream().to_string()),
        }
    }

    // A context-typed constructor path: `Default::default`, `From::from`,
    // `<_>::default`, `T::new` for a generic `T`, ...
    fn inferred(&self, path: &ExprPath) -> bool {
        let segments = &path.path.segments;
        let Some(name) = segments.last().map(|segment| segment.ident.to_string()) else {
            return false;
        };
        if let Some(qself) = &path.qself {
            return INFERRED.contains(&name.as_str()) && self.is_lock(&qself.ty).is_none();
        }
        let Some(owner) = segments.len().checked_sub(2).map(|at| &segments[at]) else {
            return false;
        };
        let owner_name = owner.ident.to_string();
        (INFERRED.contains(&name.as_str()) && self.traits.contains(&owner_name))
            || (name == "take" && owner_name == "mem")
            || (segments.len() == 2
                && self.generic(&owner_name)
                && GENERIC.contains(&name.as_str()))
    }

    // A constructor named on a lock: `Lock::new`, `<Lock<T>>::new`,
    // `<Lock<T> as Default>::default`, `Self::new` in an impl for a lock, and
    // `default` of a type holding a lock, such as `Arc::<Lock<T>>::default`.
    fn constructor(&self, path: &ExprPath) -> bool {
        let segments = &path.path.segments;
        let Some(name) = segments.last().map(|segment| segment.ident.to_string()) else {
            return false;
        };
        if !GENERIC.contains(&name.as_str()) {
            return false;
        }
        if let Some(qself) = &path.qself {
            return if name == "default" {
                self.holds(&qself.ty, 0) == Some(true)
            } else {
                self.is_lock(&qself.ty) == Some(true)
            };
        }
        let Some(owner) = segments.len().checked_sub(2).map(|at| &segments[at]) else {
            return false;
        };
        let owner_type = Type::Path(syn::TypePath {
            qself: None,
            path: Path {
                leading_colon: None,
                segments: std::iter::once(owner.clone()).collect(),
            },
        });
        if name == "default" {
            self.holds(&owner_type, 0) == Some(true)
        } else {
            self.is_lock(&owner_type) == Some(true)
        }
    }

    fn signature(&self, func: &Expr) -> Option<&'ast Signature> {
        let Expr::Path(path) = func else { return None };
        let segments: Vec<String> = path
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        let name = match segments.as_slice() {
            [name] => name.clone(),
            [owner, name] if owner == "Self" => {
                format!("{}::{name}", type_name(self.selves.last()?)?)
            }
            [owner, name] => format!("{owner}::{name}"),
            _ => return None,
        };
        self.items.functions.get(&name).copied().flatten()
    }

    fn fields(&self, path: &Path) -> Option<&'ast Fields> {
        let name = (path.segments.len() == 1).then(|| last(path))??;
        self.items.structs.get(&name).copied().flatten()
    }

    fn field_types(&self, fields: &'ast Fields) -> Vec<(Option<String>, &'ast Type)> {
        fields
            .iter()
            .map(|field| (field.ident.as_ref().map(ToString::to_string), &field.ty))
            .collect()
    }
}

/// `const _: fn(A) -> B = |lock| lock;`, which asserts at compile time that
/// `A` is `B` and constructs nothing; its types are not written values.
fn identity(item: &syn::ItemConst) -> bool {
    let Expr::Closure(closure) = &*item.expr else {
        return false;
    };
    let parameter = match closure.inputs.iter().collect::<Vec<_>>().as_slice() {
        [Pat::Ident(parameter)] => parameter.ident.to_string(),
        _ => return false,
    };
    item.ident == "_"
        && matches!(*item.ty, Type::BareFn(_))
        && matches!(&*closure.body, Expr::Path(body) if body.path.is_ident(&parameter))
}

fn any(verdicts: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut known = Some(false);
    for verdict in verdicts {
        match verdict {
            Some(true) => return Some(true),
            None => known = None,
            Some(false) => {}
        }
    }
    known
}

impl<'ast> Visit<'ast> for Finder<'_, 'ast> {
    // A lock behind a reference or pointer is borrowed, never made here.
    fn visit_type(&mut self, ty: &'ast Type) {
        match ty {
            Type::Reference(_) | Type::Ptr(_) => return,
            Type::Path(_) => self.mentions |= self.is_lock(ty) == Some(true),
            _ => {}
        }
        visit::visit_type(self, ty);
    }

    fn visit_expr(&mut self, expr: &'ast Expr) {
        match expr {
            Expr::Call(call) => {
                if let Expr::Path(path) = &*call.func {
                    self.callees.insert(key(&call.func));
                    if self.inferred(path) {
                        self.judge(expr);
                    }
                }
                let inputs: Vec<Option<Expect<'ast>>> =
                    if let Some(signature) = self.signature(&call.func) {
                        signature
                            .inputs
                            .iter()
                            .map(|input| match input {
                                FnArg::Typed(typed) => Some(Expect::Type(&typed.ty)),
                                FnArg::Receiver(_) => None,
                            })
                            .collect()
                    } else if let Expr::Path(path) = &*call.func
                        && let Some(fields) = self.fields(&path.path)
                    {
                        self.field_types(fields)
                            .into_iter()
                            .map(|(_, ty)| Some(Expect::Type(ty)))
                            .collect()
                    } else {
                        Vec::new()
                    };
                if inputs.len() == call.args.len() {
                    for (arg, expect) in call.args.iter().zip(inputs) {
                        self.assign(arg, expect);
                    }
                }
            }
            Expr::MethodCall(call)
                if METHODS.contains(&call.method.to_string().as_str())
                    && call.turbofish.is_none()
                    && call.args.is_empty() =>
            {
                self.judge(expr);
            }
            Expr::Path(path) => {
                self.found |= self.constructor(path);
                if !self.callees.contains(&key(expr)) && self.inferred(path) {
                    self.judge(expr);
                }
            }
            Expr::Struct(literal) => {
                if let Some(fields) = self.fields(&literal.path) {
                    let types = self.field_types(fields);
                    for value in &literal.fields {
                        let member = match &value.member {
                            syn::Member::Named(name) => types
                                .iter()
                                .find(|(field, _)| field.as_deref() == Some(&name.to_string()))
                                .map(|(_, ty)| *ty),
                            syn::Member::Unnamed(index) => {
                                types.get(index.index as usize).map(|(_, ty)| *ty)
                            }
                        };
                        self.assign(&value.expr, member.map(Expect::Type));
                    }
                }
                if let Some(rest) = &literal.rest {
                    let ty = Type::Path(syn::TypePath {
                        qself: None,
                        path: literal.path.clone(),
                    });
                    let holds = self.holds(&ty, 0);
                    self.assign(rest, holds.map(Expect::Known));
                }
            }
            Expr::Return(returned) => {
                if let Some(value) = &returned.expr {
                    let expect = self.returns.last().copied().flatten();
                    self.assign(value, expect);
                }
            }
            Expr::Closure(closure) => {
                let output = match &closure.output {
                    ReturnType::Type(_, ty) => Some(Expect::Type(ty)),
                    ReturnType::Default => None,
                };
                self.assign(&closure.body, output);
                self.returns.push(output);
                visit::visit_expr(self, expr);
                self.returns.pop();
                return;
            }
            Expr::Async(_) => {
                self.returns.push(None);
                visit::visit_expr(self, expr);
                self.returns.pop();
                return;
            }
            _ => {}
        }
        visit::visit_expr(self, expr);
    }

    // `let lock: Lock<T> = ..;` declares a lock whatever initializes it.
    fn visit_local(&mut self, local: &'ast Local) {
        let ty = match &local.pat {
            Pat::Type(typed) => Some(&*typed.ty),
            _ => None,
        };
        if let Some(ty) = ty {
            self.found |= self.is_lock(ty) == Some(true);
        }
        if let Some(init) = &local.init {
            self.assign(&init.expr, ty.map(Expect::Type));
        }
        visit::visit_local(self, local);
    }

    fn visit_item_static(&mut self, item: &'ast syn::ItemStatic) {
        self.found |= self.is_lock(&item.ty) == Some(true);
        self.assign(&item.expr, Some(Expect::Type(&item.ty)));
        visit::visit_item_static(self, item);
    }

    fn visit_item_const(&mut self, item: &'ast syn::ItemConst) {
        if identity(item) {
            return;
        }
        self.found |= self.is_lock(&item.ty) == Some(true);
        self.assign(&item.expr, Some(Expect::Type(&item.ty)));
        visit::visit_item_const(self, item);
    }

    fn visit_impl_item_const(&mut self, item: &'ast syn::ImplItemConst) {
        self.found |= self.is_lock(&item.ty) == Some(true);
        self.assign(&item.expr, Some(Expect::Type(&item.ty)));
        visit::visit_impl_item_const(self, item);
    }

    // A field holding a lock: the type owns one, and a derived `Default`
    // constructs it.
    fn visit_field(&mut self, field: &'ast syn::Field) {
        self.found |= self.is_lock(&field.ty) == Some(true);
        visit::visit_field(self, field);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.function(&item.sig, &item.block, |finder| {
            visit::visit_item_fn(finder, item);
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.function(&item.sig, &item.block, |finder| {
            visit::visit_impl_item_fn(finder, item);
        });
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        let scope = self.generics.len();
        self.push(&item.sig.generics);
        if let Some(block) = &item.default {
            self.tail(block, Some(Self::output(&item.sig.output)));
        }
        self.returns.push(Some(Self::output(&item.sig.output)));
        visit::visit_trait_item_fn(self, item);
        self.returns.pop();
        self.generics.truncate(scope);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let scope = self.generics.len();
        self.push(&item.generics);
        self.selves.push(&item.self_ty);
        visit::visit_item_impl(self, item);
        self.selves.pop();
        self.generics.truncate(scope);
    }

    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        let scope = self.generics.len();
        self.push(&item.generics);
        // `Self` in a trait is any implementor.
        self.generics.push("Self".into());
        visit::visit_item_trait(self, item);
        self.generics.truncate(scope);
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        let scope = self.generics.len();
        self.push(&item.generics);
        visit::visit_item_struct(self, item);
        self.generics.truncate(scope);
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        let scope = self.generics.len();
        self.push(&item.generics);
        visit::visit_item_enum(self, item);
        self.generics.truncate(scope);
    }
}
