#!/usr/bin/env python3
"""Shared Windows export admission, including resuming a completed producer.

This module owns archive membership, digest and disk-capacity admission. It
does not build, select Cargo artifacts, own processes or run product tests.
OpenAI Codex <noreply@openai.com>.
"""

import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import subprocess
import tarfile
import uuid


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def relative(name):
    path = PurePosixPath(name)
    if (not path.parts or path.is_absolute() or ".." in path.parts
            or "\\" in name or ":" in name or path.as_posix() != name):
        raise ValueError("Unsafe archive/source path: " + name)
    return path


def require_capacity(directory, needed):
    """Reject an insufficient destination before fetch or extraction starts."""
    free = shutil.disk_usage(directory).free
    reserve = 64 * 1024 * 1024
    if needed + reserve > free:
        raise OSError(f"Insufficient export capacity: need {needed} bytes plus {reserve} reserve; have {free}")


def fetch_export(records, build_log):
    """Fetch a completed job's exact source-relative export, without rebuilding."""
    records = Path(records)
    matches = re.findall(r"^WIN_TEST_ARCHIVE ([0-9a-f]{64}) (\.bb-win-test-[a-f0-9]+\.tar\.gz)\s*$",
                         build_log, re.MULTILINE)
    if len(matches) != 1:
        raise ValueError("Build receipt must name exactly one successful Windows export")
    expected, name = matches[0]
    archive = records / name
    saved = records / "artifacts.tar.gz"
    for candidate in (archive, saved):
        if candidate.is_file() and digest(candidate) == expected:
            return candidate, expected
    sources = re.findall(r"; src (wt-sdk/[A-Za-z0-9_.-]+) target ", build_log)
    if len(sources) != 1:
        raise ValueError("Missing exact build-slot source receipt for immutable export")
    remote = "/mnt/bb/" + sources[0] + "/" + name
    inspect = ("import json,pathlib,sys,tarfile; p=pathlib.Path(sys.argv[1]); "
               "t=tarfile.open(p); print(json.dumps(dict(bytes=p.stat().st_size,"
               "expanded_bytes=sum(m.size for m in t if m.isfile()))))")
    command = ["python3", "-c", inspect, remote]
    transport = ["wsl.exe", "-d", "Ubuntu", "-e"]
    config = ["-F", "/home/var/.ssh/buildbox/config", "-o", "BatchMode=yes"]
    sizes = json.loads(subprocess.check_output(
        [*transport, "ssh", *config, "bb2", shlex.join(command)], text=True))
    require_capacity(records, sizes["bytes"] + sizes["expanded_bytes"])
    incoming = records / ("incoming-" + uuid.uuid4().hex + ".tar.gz")
    incoming.touch(exist_ok=False)
    target = "/mnt/" + incoming.drive[0].lower() + incoming.as_posix()[2:]
    # A failed transfer is retained at its exclusive path. A later --consume
    # uses this same immutable producer; it cannot rerun Cargo or overwrite it.
    subprocess.run([*transport, "scp", *config, "bb2:" + remote, target], check=True)
    if incoming.stat().st_size != sizes["bytes"] or digest(incoming) != expected:
        raise ValueError("Fetched archive SHA-256/size mismatch: " + str(incoming))
    if archive.exists():
        failed = records / "failed-transfers" / uuid.uuid4().hex
        failed.mkdir(parents=True)
        archive.rename(failed / archive.name)
        (failed / "receipt.json").write_text(json.dumps({
            "path": archive.name, "retained_sha256": digest(failed / archive.name),
            "expected_sha256": expected,
        }, indent=2), encoding="utf-8")
    incoming.rename(archive)
    return archive, expected


def destination(records, name):
    path = records.joinpath(*relative(name).parts)
    # A retained partial extraction must never redirect subsequent admission.
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError("Archive destination traverses a link: " + str(part))
        if part == records:
            break
    return path


def admit_archive(records, archive, expected_sha256):
    """Verify the whole immutable export before writing, then resume its files.

    Complete matching files are reused. Incomplete files retain their original
    bytes in failed-transfers; only an admitted replacement becomes executable.
    The archive is retained so a completed build is never repeated for a fetch.
    """
    records, archive = Path(records), Path(archive)
    if not re.fullmatch("[0-9a-f]{64}", expected_sha256) or digest(archive) != expected_sha256:
        raise ValueError("Fetched archive SHA-256 mismatch")
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        names = [member.name for member in members]
        if len(names) != len({name.casefold() for name in names}) or names.count("manifest.json") != 1:
            raise ValueError("Duplicate member or missing export manifest")
        for member in members:
            relative(member.name)
            if not member.isfile():
                raise ValueError("Unexpected non-file archive member: " + member.name)
        manifest = json.load(tar.extractfile("manifest.json"))
        allowed = {"bin/" + row["name"]: row["sha256"] for row in manifest["artifacts"]}
        allowed.update({"bin/" + name: row["sha256"] for name, row in manifest["dlls"].items()})
        allowed.update({"source/" + name: value for name, value in manifest["source"].items()})
        if not set(allowed).issubset(names):
            raise ValueError("Incomplete artifact/fixture fetch")
        planned, needed, reused = [], 0, 0
        for member in members:
            name = member.name
            if name != "manifest.json" and name not in allowed and not re.fullmatch(
                    r"logs/\.bb-win-test-[a-f0-9]+-[A-Za-z0-9_-]+\.jsonl", name):
                raise ValueError("Unexpected archive member: " + name)
            with tar.extractfile(member) as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if name in allowed and actual != allowed[name]:
                raise ValueError("Fetched file SHA-256 mismatch: " + name)
            path = destination(records, name)
            if path.exists() and not path.is_file():
                raise ValueError("Archive destination is not a regular file: " + name)
            complete = path.is_file() and path.stat().st_size == member.size and digest(path) == actual
            if complete:
                reused += 1
            else:
                needed += member.size
            planned.append((member, path, actual, complete))
        require_capacity(records, needed)
        for member, path, actual, complete in planned:
            if complete:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                failed = records / "failed-transfers" / uuid.uuid4().hex
                failed.mkdir(parents=True)
                path.rename(failed / path.name)
                (failed / "receipt.json").write_text(json.dumps({
                    "path": member.name, "retained_sha256": digest(failed / path.name),
                    "expected_sha256": actual, "archive_sha256": expected_sha256,
                }, indent=2), encoding="utf-8")
            # Exclusive creation cannot silently replace another consumer's file.
            with tar.extractfile(member) as src, path.open("xb") as output:
                shutil.copyfileobj(src, output)
            if digest(path) != actual:
                raise ValueError("Admitted file changed while extracting: " + member.name)
    print("WIN_TEST_FETCH_VERIFIED", len(manifest["artifacts"]), "executables,",
          len(manifest["source"]), "source/fixture files;", reused, "retained files", flush=True)
    return manifest
