#!/usr/bin/env python3
"""Plan a PR gate without building: metadata + local dependency/source-reference edges."""
import argparse
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tomllib


def rust_references(source, text, target_roots):
    """Resolve literal Rust file edges, including nested inline modules.

    Tokenization excludes comments and string contents from Rust syntax. An
    imported file creates one edge, never an edge to all of its siblings.
    Cargo supplies crate roots; non-root foo.rs owns ordinary modules in foo/.
    """
    token = re.compile(r'''//[^\n]*|/\*|\*/|r(?P<hash>\#*)".*?"(?P=hash)|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\\n])'|\b[A-Za-z_][A-Za-z_0-9]*\b|[^\s]''', re.S)
    tokens=[]; depth=0
    for match in token.finditer(text):
        value=match[0]
        if value=='/*':depth+=1
        elif value=='*/' and depth:depth-=1
        elif not depth and not value.startswith('//'):tokens.append(value)

    def literal(value):
        if value.startswith('r') and '"' in value:return value[value.index('"')+1:value.rindex('"')]
        if value.startswith('"'):
            try:return json.loads(value)
            except ValueError:return None
        return None

    references=[]
    module_dir=source.parent if source in target_roots or source.name=='mod.rs' else source.with_suffix('')

    def scan(index, ordinary, attribute, nested=False):
        path=None
        while index<len(tokens):
            value=tokens[index]
            if value=='}':return index+1
            if tokens[index:index+4]==['#','[','path','='] and index+5<len(tokens) and tokens[index+5]==']':
                path=literal(tokens[index+4]);index+=6;continue
            if value=='mod' and index+2<len(tokens) and re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',tokens[index+1]):
                name,end=tokens[index+1:index+3]
                if end==';':
                    candidates=[attribute/path] if path is not None else [ordinary/(name+'.rs'),ordinary/name/'mod.rs']
                    references.extend((candidate,'source') for candidate in candidates)
                    path=None;index+=3;continue
                if end=='{':
                    directory=attribute/path if path is not None else ordinary/name
                    index=scan(index+3,directory,directory,True);path=None;continue
            if value=='{':
                # Other Rust blocks do not change module ownership. Scan them
                # too so cfg/macros cannot hide literal module/include edges.
                index=scan(index+1,ordinary,attribute,nested);continue
            if value in ('include','include_str','include_bytes') and tokens[index+1:index+3]==['!','(']:
                suffix=tokens[index+3:]
                kind='source' if value=='include' else 'data'
                if suffix:
                    file=literal(suffix[0])
                    if file is not None:references.append((source.parent/file,kind))
                    elif suffix[:7]==['concat','!','(','env','!','(','"CARGO_MANIFEST_DIR"'] and len(suffix)>9 and suffix[7:9]==[')',',']:
                        file=literal(suffix[9])
                        if file is not None:references.append((file,'manifest-'+kind))
            if value==';':path=None
            index+=1
        return index
    scan(0,module_dir,source.parent)
    return references


def run(root, *args):
    return subprocess.check_output(args, cwd=root)


def plan(root, base, head="HEAD"):
    root = Path(root).resolve()
    # --no-renames deliberately includes both sides of a rename and deleted files.
    changed = sorted(filter(None, run(root, "git", "diff", "--no-renames", "--name-only", "-z", base, head).decode().split("\0")))
    metadata = json.loads(run(root, "cargo", "metadata", "--offline", "--no-deps", "--format-version", "1"))
    member_ids = set(metadata["workspace_members"])
    packages = {p["name"]: p for p in metadata["packages"] if p["id"] in member_ids}
    directories = {name: Path(p["manifest_path"]).resolve().parent for name, p in packages.items()}
    build_scripts = {Path(t["src_path"]).resolve() for p in packages.values() for t in p["targets"] if "custom-build" in t["kind"]}
    ordered = sorted(directories, key=lambda n: len(directories[n].parts), reverse=True)

    def owner(path):
        return next((n for n in ordered if path.is_relative_to(directories[n])), None)

    reverse = {n: set() for n in packages}
    reference_users = {}
    file_users = {}
    edges = []

    def reference(user, path, kind, source=None):
        path = path.resolve()
        reference_users.setdefault(path, set()).add(user)
        if source is not None:
            file_users.setdefault(path, set()).add(source)
        dependency = owner(path)
        if dependency and dependency != user and kind != "data":
            if kind in ("dependency", "target"):
                reverse[dependency].add(user)
            edges.append([dependency, user, kind, path.relative_to(root).as_posix()])

    # Metadata includes optional, target-specific, dev and build dependencies,
    # including renamed dependencies. Do not use a host-only resolved graph.
    for user, p in packages.items():
        for dep in p["dependencies"]:
            if dep.get("path"):
                reference(user, Path(dep["path"]) / "Cargo.toml", "dependency")
        for target in p["targets"]:
            reference(user, Path(target["src_path"]), "target")

    tracked = list(filter(None, run(root, "git", "ls-files", "-z").decode().split("\0")))
    # Keep source references at file granularity, independent of Cargo edges.
    target_roots={Path(t['src_path']).resolve() for p in packages.values() for t in p['targets']}
    for rel in tracked:
        if not rel.endswith(".rs") or not (root / rel).is_file():
            continue
        source = (root / rel).resolve()
        user = owner(source)
        if not user:
            continue
        text = source.read_text(encoding="utf-8", errors="replace")
        for path,kind in rust_references(source,text,target_roots):
            if kind.startswith('manifest-'):
                path=directories[user]/path.lstrip('/\\');kind=kind.removeprefix('manifest-')
            reference(user,path,kind,source)

    reasons = []
    direct = set()
    for rel in changed:
        path = (root / rel).resolve()
        parts = PurePosixPath(rel).parts
        name = parts[-1]
        if name == "Cargo.lock" or ".cargo" in parts or name.startswith(("rust-toolchain", "rustfmt-toolchain")):
            reasons.append(rel)
        elif name == "Cargo.toml":
            # Deleted manifests and workspace-level manifests cannot be narrowed.
            if not path.exists() or "workspace" in tomllib.loads(path.read_text(encoding="utf-8")):
                reasons.append(rel)
        if name == "build.rs" or rel.startswith(("scripts/", "build/", "proto/")) or (name.startswith("build") and path.suffix in (".sh", ".ps1", ".mjs", ".js", ".py", ".cmd", ".bat")):
            reasons.append(rel)
        for p in packages.values():
            if any("custom-build" in t["kind"] and Path(t["src_path"]).resolve() == path for t in p["targets"]):
                reasons.append(rel)
        selected_owner = owner(path)
        if selected_owner:
            direct.add(selected_owner)
        direct.update(reference_users.get(path, ()))
        if not selected_owner and path not in reference_users and rel not in reasons:
            reasons.append("unmapped: " + rel)

    # Follow transitive includes even if intermediate files are in the source
    # crate and only the final includer belongs to the fuzz/test crate.
    source_pending = [(root / rel).resolve() for rel in changed]
    source_seen = set(source_pending)
    while source_pending:
        path = source_pending.pop()
        if path in build_scripts:
            reasons.append("build-script input: " + path.relative_to(root).as_posix())
        users = set(file_users.get(path, ()))
        for source in users - source_seen:
            source_seen.add(source)
            source_pending.append(source)
            if owner(source):
                direct.add(owner(source))

    selected = set(direct)
    pending = list(direct)
    while pending:
        for user in reverse[pending.pop()] - selected:
            selected.add(user)
            pending.append(user)

    mode = "full" if reasons else "affected"
    tests = sorted(selected)
    if mode == "full":
        # Cargo/build inputs invalidate narrowing: cover every workspace member.
        tests = sorted(packages)
    return {"mode": mode, "base": base, "head": run(root, "git", "rev-parse", head).decode().strip(),
            "changed": changed, "direct": sorted(direct), "tests": tests,
            "clippy": sorted(packages) if mode == "full" else sorted(selected),
            "full_reasons": sorted(set(reasons)), "source_edges": sorted({tuple(e) for e in edges if e[2] != "dependency"})}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkout")
    parser.add_argument("base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = plan(args.checkout, args.base, args.head)
    output = Path(args.output)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    for kind in ("tests", "clippy"):
        output.with_name(".gate-" + kind + "-packages").write_text("".join(n + "\n" for n in result[kind]), encoding="utf-8", newline="\n")
    output.with_name(".gate-scope-mode").write_text(result["mode"] + "\n", encoding="utf-8", newline="\n")
    print("GATE_SCOPE " + json.dumps({k: result[k] for k in ("mode", "base", "head", "changed", "direct", "tests", "clippy", "full_reasons")}))
