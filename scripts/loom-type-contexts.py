"""Resolve model constructors in their actual Cargo test compilation contexts."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import urllib.request

TOOLCHAIN = "nightly-2026-07-01"
ROOT = Path(__file__).resolve().parent.parent
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
HARNESS = "terminal,tui,filesystem-local,native-execution,grpc,test-support,search-http,model-checking"
QUERY = "--cfg acyclic_loom"
STREAM = QUERY + " --cfg acyclic_stream_loom"
FILESYSTEM = QUERY + " --cfg acyclic_fs_loom"
HARNESS_FLAGS = FILESYSTEM + " --cfg acyclic_harness_loom"


def context(name, selection, flags, prefix, check=False):
    return name, selection, flags, prefix, check


CONTEXTS = [
    context("native", ["-p", "acyclic-native-runtime", "--features", "test-support", "--lib"],
            QUERY + " --cfg acyclic_native_runtime_loom", "rust/crates/native-runtime/src/testing/"),
    context("apple", ["-p", "acyclic-native-runtime", "--features", "test-support", "--lib", "--tests",
                      "--target", "aarch64-apple-darwin"], QUERY + " --cfg acyclic_native_runtime_loom",
            "rust/crates/native-runtime/src/apple_model.rs", check=True),
    context("session", ["--manifest-path", "rust/crates/stream/models/Cargo.toml", "--test", "session_model"],
            STREAM, "rust/crates/stream/models/"),
    context("publication", ["--manifest-path", "rust/crates/stream/publication-models/Cargo.toml", "--test", "publication"],
            STREAM, "rust/crates/stream/publication-models/"),
    context("stream", ["-p", "acyclic-stream", "--no-default-features", "--features", "grpc,local", "--lib"],
            STREAM, "rust/crates/stream/src/"),
    context("filesystem", ["-p", "acyclic-fs", "--features", "model-checking,fuzzing", "--lib"],
            FILESYSTEM, "rust/crates/filesystem/src/"),
    context("filesystem-fuzz", ["-p", "acyclic-fs-fuzz", "--test", "model"],
            FILESYSTEM, "rust/crates/filesystem/fuzz/tests/"),
    context("actors", ["-p", "acyclic-actors", "--lib"], "--cfg loom", "rust/crates/actors/src/"),
    context("timer", ["-p", "acyclic-actors", "--test", "timer_loom"], QUERY, "rust/crates/actors/tests/"),
    context("harness", ["-p", "acyclic-harness", "--no-default-features", "--features", HARNESS, "--lib"],
            HARNESS_FLAGS, "rust/crates/harness/src/"),
    context("query", ["-p", "acyclic-harness", "--no-default-features", "--features", HARNESS, "--test", "query-model"],
            HARNESS_FLAGS, "rust/crates/harness/tests/query_model.rs"),
    context("accounting", ["-p", "acyclic-harness", "--no-default-features", "--features", HARNESS, "--test", "memory-accounting-model"],
            HARNESS_FLAGS, "rust/crates/harness/tests/memory_accounting_model.rs"),
    context("providers", ["-p", "acyclic-harness-providers", "--features",
                          ",".join("acyclic-harness/" + feature for feature in HARNESS.split(",")),
                          "--test", "verification_models"],
            HARNESS_FLAGS, "rust/crates/harness-providers/tests/"),
]


def run(arguments, environment, **options):
    return subprocess.run(arguments, env=environment, check=True, **options)


def compiler():
    host = subprocess.check_output(["rustc", "+" + TOOLCHAIN, "-vV"], text=True).split("host: ")[1].splitlines()[0]
    sysroot = Path(subprocess.check_output(["rustc", "+" + TOOLCHAIN, "--print", "sysroot"], text=True).strip())
    directory = TARGET / ("loom-rustc-dev-" + TOOLCHAIN.removeprefix("nightly-"))
    name = "rustc-dev-nightly-" + host
    libraries = directory / name / "rustc-dev/lib/rustlib" / host / "lib"
    if not libraries.is_dir():
        directory.mkdir(parents=True, exist_ok=True)
        archive = directory / (name + ".tar.xz")
        url = "https://static.rust-lang.org/dist/" + TOOLCHAIN.removeprefix("nightly-") + "/" + archive.name
        try:
            urllib.request.urlretrieve(url, archive)
            expected = urllib.request.urlopen(url + ".sha256").read().decode().split()[0]
            with archive.open("rb") as source:
                if hashlib.file_digest(source, "sha256").hexdigest() != expected:
                    raise ValueError("compiler component checksum mismatch")
            with tarfile.open(archive) as source:
                source.extractall(directory, filter="data")
        finally:
            archive.unlink(missing_ok=True)
    environment = dict(os.environ, RUSTFLAGS="-Lall=" + str(libraries) + " -Lnative=" + str(sysroot / "lib"))
    environment.pop("CARGO_ENCODED_RUSTFLAGS", None)
    environment.pop("RUSTC_WORKSPACE_WRAPPER", None)
    run(["cargo", "+" + TOOLCHAIN, "build", "--locked", "--manifest-path",
         str(ROOT / "rust/verification/loom-types/Cargo.toml"), "--target-dir", str(TARGET / "loom-types")],
        environment, stdout=sys.stderr)
    environment["LD_LIBRARY_PATH"] = str(sysroot / "lib")
    environment["DYLD_LIBRARY_PATH"] = str(sysroot / "lib")
    environment["RUSTC_WORKSPACE_WRAPPER"] = str(TARGET / "loom-types/debug/loom-type-scan")
    environment.pop("RUSTC_WRAPPER", None)
    return environment


def replay(executable, commands, directory, environment):
    candidates = []
    for command in commands.glob("*.args"):
        fields = command.read_text().split("\0")
        suffix = next((arg.split("=", 1)[1] for arg in fields if arg.startswith("extra-filename=")), None)
        if suffix and Path(executable).stem.endswith(suffix):
            candidates.append((command, fields))
    if len(candidates) != 1:
        raise ValueError("missing or ambiguous compiler invocation for " + executable)
    command, (cwd, *arguments) = candidates[0]
    metadata = directory / "metadata"
    metadata.mkdir(exist_ok=True)
    for index, argument in enumerate(arguments):
        if argument.startswith("--emit="):
            arguments[index] = "--emit=metadata"
        if argument == "--out-dir":
            arguments[index + 1] = str(metadata)
    restored = dict(environment)
    for record in command.with_suffix(".env").read_text().split("\0"):
        if record:
            key, value = record.split("=", 1)
            restored[key] = value
    run(arguments, restored, cwd=cwd, stdout=sys.stderr)


def analyze(name, selection, flags, models, environment, check=False):
    directory = TARGET / ("loom-context-" + name)
    reports, commands = directory / "reports", directory / "commands"
    reports.mkdir(parents=True, exist_ok=True)
    commands.mkdir(exist_ok=True)
    for report in reports.glob("*.tsv"):
        report.unlink()
    model_file = directory / "models"
    model_file.write_text("\n".join(models) + "\n")
    environment = dict(environment, LOOM_MODEL_ROOT=str(ROOT), LOOM_MODEL_FILES=str(model_file),
                       LOOM_MODEL_REPORTS=str(reports), LOOM_MODEL_REPLAY=str(commands), RUSTFLAGS=flags)
    action = ["check"] if check else ["test", "--no-run"]
    process = subprocess.Popen(["cargo", "+" + TOOLCHAIN, *action, "--locked", *selection,
                                "--message-format=json"],
                               cwd=ROOT, env=environment, stdout=subprocess.PIPE, text=True)
    executables = []
    for line in process.stdout:
        try:
            event = json.loads(line)
        except ValueError:
            print(line, end="", file=sys.stderr)
            continue
        if event.get("reason") == "compiler-message":
            print(event["message"].get("rendered", ""), end="", file=sys.stderr)
        if event.get("reason") == "compiler-artifact" and event.get("profile", {}).get("test"):
            files = [event["executable"]] if event.get("executable") else event.get("filenames", [])
            executables.extend(files)
    if process.wait():
        raise ValueError("model compilation failed: " + name)
    if not list(reports.glob("*.tsv")):
        for executable in executables:
            replay(executable, commands, directory, environment)
    rows = {model: {} for model in models}
    for report in reports.glob("*.tsv"):
        output = report.read_bytes().decode("utf-8")
        if not output.endswith("\n") or "\r" in output or "\0" in output:
            raise ValueError("incomplete compiler response")
        for line in output.splitlines():
            fields = line.split("\t")
            if len(fields) != 3:
                raise ValueError("invalid compiler response row")
            model, verdict, body = fields
            if model not in rows or not body or body in rows[model]:
                raise ValueError("unexpected or duplicate compiler response row: " + line)
            if verdict not in ["0", "1"] and not verdict.startswith("!indeterminate:"):
                raise ValueError("invalid compiler verdict: " + verdict)
            rows[model][body] = verdict
    result = {}
    for model, bodies in rows.items():
        if not bodies:
            result[model] = "!indeterminate: model was not compiled in " + name
        else:
            unknown = [body + ": " + verdict[1:] for body, verdict in bodies.items() if verdict.startswith("!")]
            result[model] = "!" + "; ".join(unknown) if unknown else str(int("1" in bodies.values()))
    return result


def main():
    models = json.load(sys.stdin)
    if not isinstance(models, list) or any(not isinstance(path, str) or any(c in path for c in "\r\n\t\0") for path in models):
        raise ValueError("invalid model paths")
    if len(set(models)) != len(models):
        raise ValueError("duplicate model paths")
    environment = compiler()
    result = {}
    for name, selection, flags, prefix, check in CONTEXTS:
        selected = [path for path in models if path.startswith(prefix)]
        if selected:
            if check:
                run(["rustup", "target", "add", "--toolchain", TOOLCHAIN, "aarch64-apple-darwin"],
                    environment, stdout=sys.stderr)
            result.update(analyze(name, selection, flags, selected, environment, check))
            print(name + ": " + str(len(selected)) + " model sources analyzed", file=sys.stderr, flush=True)
            for model in selected:
                if result[model].startswith("!"):
                    print(model + ": " + result[model], file=sys.stderr, flush=True)
    for model in models:
        result.setdefault(model, "!indeterminate: no compilation context for " + model)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
