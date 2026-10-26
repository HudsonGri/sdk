#!/usr/bin/env python3
"""One Windows fixture and fleet lease for libtest and shipping commands.

Compilation and artifact admission belong to win-test.sh. Process containment
belongs to the admitted external process host; this module never owns a Job or
uses PID-based tree killing. OpenAI Codex <noreply@openai.com>.
"""

import codecs
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def admitted_host(source, executable, receipt):
    """Bind the cross-built host to its source and producer receipt."""
    executable = Path(executable)
    receipt = json.loads(Path(receipt).read_text(encoding="utf-8"))
    if not executable.is_absolute() or not executable.is_file():
        raise ValueError("E2E_PROCESS_HOST must name an absolute cross-built executable")
    if receipt["exit"] != 0 or receipt["executable_sha256"] != digest(executable):
        raise ValueError("process host executable differs from its successful build receipt")
    sources = receipt["source"]
    entry = "rust/crates/native-runtime/src/bin/process_host.rs"
    if receipt["target"] != "x86_64-pc-windows-msvc" or entry not in sources:
        raise ValueError("process host needs the shared native-runtime MSVC build receipt")
    source = Path(source).resolve()
    for name, expected in sources.items():
        path = source / name
        if not path.resolve().is_relative_to(source) or path.is_symlink() or digest(path) != expected:
            raise ValueError("process host needs a fresh bb2 build for this source: " + name)
    return executable.resolve()


class Commands:
    """Exclusive fixture per command; the lease outlives its process host."""

    def __init__(self, records, source, host, binary=None):
        self.records = Path(records).resolve()
        self.source = Path(source).resolve()
        if not self.records.is_relative_to(Path("D:/lane-scratch").resolve()):
            raise ValueError("Windows runner records must stay under D:/lane-scratch")
        self.host = Path(host).resolve()
        self.binary = None if binary is None else Path(binary).resolve()
        self.session = None
        self.acquired = False
        self.lease_wait_seconds = 0
        self.results = []

    def __enter__(self):
        if os.name != "nt":
            raise OSError("this runtime adapter requires Windows")
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
        # The record creator owns admission before READY, through final drain.
        # A caller death closes stdin; it cannot release the host's lease early.
        print("WIN_TEST_WAITING_FOR_GLOBAL_MUTEX", flush=True)
        self.session = subprocess.Popen(
            [str(self.host), "--fleet-session", "D:/lane-scratch/windows/process-custody"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            encoding="utf-8", errors="strict",
        )
        try:
            ready = json.loads(self.session.stdout.readline())
            if ready.get("ready") is not True:
                raise RuntimeError("process host did not fence and acquire admission")
            self.lease_wait_seconds = ready["lease_wait_seconds"]
            self.acquired = True
            return self
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_):
        if self.session is not None:
            self.session.stdin.close()
            # Consume final output so a host draining after interruption cannot
            # block on its control pipe. Mutex release belongs to that host.
            for _ in self.session.stdout:
                pass
            rc = self.session.wait()
            self.session.stdout.close()
            self.session = None
            self.acquired = False
            if rc:
                raise RuntimeError("process host drain failed: " + str(rc))

    def _environment(self):
        identity = uuid.uuid4().hex
        fixture = self.records / "profiles" / identity
        fixture.mkdir(parents=True)
        home = fixture / "home"
        codex = home / ".codex-fixture"
        temp = self.records / "tmp" / identity
        for folder in (codex, temp, home / "AppData/Local", home / "AppData/Roaming"):
            folder.mkdir(parents=True)
        env = os.environ.copy()
        for key in list(env):
            upper = key.upper()
            if upper.endswith("API_KEY") or "AUTH_TOKEN" in upper or upper in (
                "OPENAI_ACCESS_TOKEN", "CODEX_ACCESS_TOKEN", "RUST_MIN_STACK"
            ):
                del env[key]
        env.update(
            HOME=str(home), USERPROFILE=str(home), CODEX_HOME=str(codex),
            TEMP=str(temp), TMP=str(temp), LOCALAPPDATA=str(home / "AppData/Local"),
            APPDATA=str(home / "AppData/Roaming"), HOMEDRIVE=home.drive,
            HOMEPATH=str(home)[len(home.drive):], RUST_TEST_THREADS="4",
            ACYCLIC_TEST_SOURCE_ROOT=str(self.source), GIT_CONFIG_NOSYSTEM="1",
            GIT_CONFIG_GLOBAL=str(home / ".gitconfig"), E2E_PROCESS_HOST=str(self.host),
            PYTHONDONTWRITEBYTECODE="1",
        )
        if self.binary is not None:
            env["ACYCLIC_TEST_BINARY"] = str(self.binary)
        else:
            env.pop("ACYCLIC_TEST_BINARY", None)
        return identity, env

    def run(self, argv, *, cwd, name, sha256, libtest=False):
        """Run admitted argv. Only the libtest adapter adds libtest arguments."""
        if not self.acquired:
            raise RuntimeError("Windows fleet lease is not held")
        argv = [str(value) for value in argv]
        executable = Path(argv[0])
        cwd = Path(cwd).resolve()
        if not executable.is_absolute() or digest(executable) != sha256:
            raise ValueError("command executable differs from its admission")
        if not cwd.is_relative_to(self.source) or not cwd.is_dir():
            raise ValueError("command cwd must be inside the admitted source")
        if libtest:
            if any(value.startswith("--test-threads") for value in argv[1:]):
                raise ValueError("the runner owns --test-threads=4")
            argv.append("--test-threads=4")
        identity, env = self._environment()
        start = time.monotonic()
        summaries = []
        log_path = self.records / (identity + ".log")
        packet = dict(argv=argv, cwd=str(cwd), env=env, log=str(log_path))
        self.session.stdin.write(json.dumps(packet) + "\n")
        self.session.stdin.flush()
        pending = ["", ""]
        decoders = [codecs.getincrementaldecoder("utf-8")("replace") for _ in range(2)]
        try:
            while True:
                line = self.session.stdout.readline()
                if not line:
                    raise RuntimeError("process host exited before physical drain")
                event = json.loads(line)
                if "exit" in event:
                    rc = event["exit"]
                    break
                stream = event["stream"]
                pending[stream] += decoders[stream].decode(bytes(event["output"]))
                while "\n" in pending[stream]:
                    line, pending[stream] = pending[stream].split("\n", 1)
                    print(line, flush=True)
                    if "test result:" in line:
                        summaries.append(line.strip())
            for stream, decoder in enumerate(decoders):
                remaining = pending[stream] + decoder.decode(b"", final=True)
                if remaining:
                    print(remaining, end="", flush=True)
        except BaseException:
            self.__exit__()
            raise
        elapsed = time.monotonic() - start
        verification_error = None
        if libtest and rc == 0 and "--list" not in argv:
            if not summaries or not any(re.search(r"\b[1-9][0-9]* passed\b", value) for value in summaries):
                verification_error = "libtest selected ZERO passing tests: " + repr(argv)
        result = dict(
            target=name, executable=str(executable), argv=argv,
            exit=1 if verification_error else rc, process_exit=rc,
            seconds=round(elapsed, 6), under_10s=elapsed < 10,
            lease_wait_seconds=self.lease_wait_seconds,
            summaries=summaries, sha256=sha256, fixture=identity,
            log=str(log_path), process_host_sha256=digest(self.host),
            verification_error=verification_error,
        )
        # Each completed command owns one immutable structured receipt. The
        # invocation summary below is only a view: reusing a producer/records
        # directory must never erase an earlier invocation's evidence.
        receipts = self.records / "receipts"
        receipts.mkdir(exist_ok=True)
        with (receipts / (identity + ".json")).open("x", encoding="utf-8") as receipt:
            json.dump(result, receipt, indent=2)
        self.results.append(result)
        (self.records / "results.json").write_text(json.dumps(self.results, indent=2), encoding="utf-8")
        print("WIN_TEST_RESULT", json.dumps(result), flush=True)
        if verification_error:
            raise RuntimeError(verification_error)
        return rc if 0 <= rc < 256 else 1


def run_tests(records, manifest, runtime):
    """The libtest adapter over the same generic command custody API."""
    source = records / "source"
    fixtures = [row for row in manifest["artifacts"] if not row["test"]]
    if len(fixtures) > 1:
        raise ValueError("ambiguous normal executable fixture")
    binary = records / "bin" / fixtures[0]["name"] if fixtures else None
    if "process_host" in manifest:
        host = admitted_host(source, records / "bin" / manifest["process_host"]["name"],
                             records / "host-receipt.json")
    else:
        host = admitted_host(source, os.environ["E2E_PROCESS_HOST"], os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"])
    os.environ["E2E_PROCESS_HOST"] = str(host)
    if "process_host" in manifest:
        os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"] = str(records / "host-receipt.json")
    with Commands(records, source, host, binary) as commands:
        for row in manifest["artifacts"]:
            if row["test"]:
                rc = commands.run(
                    [str((records / "bin" / row["name"]).resolve()), *runtime],
                    cwd=source / manifest["crate_dir"], name=row["target"],
                    sha256=row["sha256"], libtest=True,
                )
                if rc:
                    return rc
    if not commands.results:
        raise RuntimeError("no admitted test executables")
    print("WIN_TEST_COMPLETE", len(commands.results), "suites;", records / "results.json", flush=True)
    return 0


def main():
    """Run a JSON command plan through this same API (including Node drivers)."""
    if len(sys.argv) != 2:
        raise ValueError("usage: windows-runner.py COMMAND_PLAN.json")
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    source = Path(plan["source"]).resolve()
    records = Path(plan["records"]).resolve()
    rows = plan["commands"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("command plan must contain at least one command")
    host = admitted_host(source, os.environ["E2E_PROCESS_HOST"], os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"])
    records.mkdir(parents=True)
    with Commands(records, source, host, plan.get("binary")) as commands:
        for row in rows:
            rc = commands.run(
                row["argv"], cwd=row["cwd"], name=row["name"],
                sha256=row["sha256"], libtest=row.get("libtest", False),
            )
            if rc:
                return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
