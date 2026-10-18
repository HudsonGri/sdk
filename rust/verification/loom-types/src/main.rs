#![feature(rustc_private)]

extern crate rustc_driver;
extern crate rustc_hir;
extern crate rustc_interface;
extern crate rustc_middle;
extern crate rustc_span;

mod ownership;
mod replay;
mod types;

use rustc_driver::{Callbacks, Compilation};
use rustc_hir::intravisit::{self, Visitor};
use rustc_interface::interface::Compiler;
use rustc_middle::ty::{TyCtxt, TypeckResults};
use rustc_span::{FileName, Span};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn source(tcx: TyCtxt<'_>, root: &Path, span: Span) -> Option<String> {
    let FileName::Real(file) = tcx
        .sess
        .source_map()
        .span_to_filename(span.source_callsite())
    else {
        return None;
    };
    let file = file.local_path()?;
    let file = if file.is_absolute() {
        file.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(file)
    };
    Some(
        file.canonicalize()
            .ok()?
            .strip_prefix(root)
            .ok()?
            .to_string_lossy()
            .replace('\\', "/"),
    )
}

#[derive(Default)]
struct Check {
    root: PathBuf,
    models: BTreeSet<String>,
    report: Option<PathBuf>,
}

impl Callbacks for Check {
    fn after_analysis<'tcx>(&mut self, _: &Compiler, tcx: TyCtxt<'tcx>) -> Compilation {
        if self.models.is_empty() {
            return Compilation::Continue;
        }
        let mut verdicts: BTreeMap<(String, String), Result<bool, String>> = BTreeMap::new();
        let roots = ownership::roots(tcx, &self.root, &self.models);
        for (file, item, owned) in ownership::declarations(tcx, &roots) {
            verdicts.insert((file, item), Ok(owned));
        }
        for owner in tcx.hir_body_owners() {
            let Some(file) = ownership::model(tcx, &roots, owner) else {
                continue;
            };
            let body = tcx.hir_body_owned_by(owner);
            let mut check = Body {
                tcx,
                types: tcx.typeck(owner),
                owner,
                verdict: verdicts
                    .entry((file.clone(), tcx.def_path_str(owner.to_def_id())))
                    .or_insert(Ok(false)),
            };
            check.visit_body(body);
        }
        let mut output = String::new();
        for ((file, body), verdict) in verdicts {
            let verdict = match verdict {
                Ok(owned) => u8::from(owned).to_string(),
                Err(error) => format!("!{}", error.replace(['\n', '\t'], " ")),
            };
            output.push_str(&format!("{file}\t{verdict}\t{body}\n"));
        }
        if let Some(path) = &self.report {
            std::fs::write(path, output).expect("write compiler lock verdicts");
        } else {
            print!("{output}");
        }
        Compilation::Continue
    }
}

struct Body<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    types: &'tcx TypeckResults<'tcx>,
    owner: rustc_hir::def_id::LocalDefId,
    verdict: &'a mut Result<bool, String>,
}

impl<'tcx> Body<'_, 'tcx> {
    fn judge(&mut self, ty: rustc_middle::ty::Ty<'tcx>, default: bool) {
        let found = types::lock(
            self.tcx,
            self.owner,
            rustc_middle::ty::Unnormalized::new_wip(ty),
            default,
            0,
        );
        match found {
            Some(true) => {
                if let Ok(owned) = self.verdict {
                    *owned = true;
                }
            }
            Some(false) => {}
            None => {
                *self.verdict = Err(format!("indeterminate: compiler could not normalize {ty}"))
            }
        }
    }
}

impl<'tcx> Visitor<'tcx> for Body<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx rustc_hir::Expr<'tcx>) {
        use rustc_hir::ExprKind;
        match expression.kind {
            ExprKind::Call(callee, _) => {
                let default = match self.types.expr_ty(callee).kind() {
                    rustc_middle::ty::FnDef(id, _) => types::default_call(self.tcx, *id),
                    _ => false,
                };
                self.judge(self.types.expr_ty(expression), default);
            }
            ExprKind::MethodCall(..) => {
                let default = self
                    .types
                    .type_dependent_def_id(expression.hir_id)
                    .is_some_and(|id| types::default_call(self.tcx, id));
                self.judge(self.types.expr_ty(expression), default);
            }
            ExprKind::Struct(..) => self.judge(self.types.expr_ty(expression), false),
            _ => {}
        }
        intravisit::walk_expr(self, expression);
    }
}

fn main() -> std::process::ExitCode {
    let mut args: Vec<_> = std::env::args().collect();
    let direct = args.iter().any(|arg| arg == "--loom-direct");
    args.retain(|arg| arg != "--loom-direct");
    if args.get(1).is_some_and(|arg| {
        Path::new(arg)
            .file_stem()
            .is_some_and(|name| name == "rustc")
    }) {
        if !args
            .iter()
            .any(|arg| arg == "--sysroot" || arg.starts_with("--sysroot="))
        {
            let output = std::process::Command::new(&args[1])
                .args(["--print", "sysroot"])
                .output()
                .expect("query wrapped compiler sysroot");
            assert!(
                output.status.success(),
                "wrapped compiler could not report its sysroot"
            );
            args.extend([
                "--sysroot".to_string(),
                String::from_utf8(output.stdout)
                    .expect("compiler sysroot is UTF-8")
                    .trim()
                    .to_string(),
            ]);
        }
        args.remove(1);
    }
    let mut check = Check::default();
    if (direct || args.iter().any(|arg| arg == "--test"))
        && let Ok(root) = std::env::var("LOOM_MODEL_ROOT")
    {
        check.root = PathBuf::from(root)
            .canonicalize()
            .expect("resolve model source root");
        check.models =
            std::fs::read_to_string(std::env::var("LOOM_MODEL_FILES").expect("model list"))
                .expect("read model list")
                .lines()
                .map(str::to_owned)
                .collect();
        let directory =
            PathBuf::from(std::env::var("LOOM_MODEL_REPORTS").expect("report directory"));
        check.report = Some(directory.join(format!("{}.tsv", std::process::id())));
        if let Some(directory) = std::env::var_os("LOOM_MODEL_REPLAY") {
            replay::record(Path::new(&directory), &args).expect("record compiler invocation");
        }
    }
    rustc_driver::catch_with_exit_code(|| rustc_driver::run_compiler(&args, &mut check))
}
