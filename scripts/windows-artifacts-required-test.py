#!/usr/bin/env python3
"""Selected-input admission controls mutated from a captured real export.

Usage: SCRIPT ARCHIVE RECORDS [ACTUAL_OLD_API]. RECORDS must be a new directory.
No Cargo, provider, host, or executable bytes are fabricated by this fixture.
"""

import hashlib
import inspect
import io
import json
from pathlib import Path
import sys
import tarfile
import time
import types


def main():
    archive, records = Path(sys.argv[1]), Path(sys.argv[2])
    records.mkdir(parents=True)
    path = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).with_name("windows-artifacts.py")
    api = types.ModuleType("captured_export_admission")
    api.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), api.__dict__)
    with tarfile.open(archive) as tar:
        manifest = json.load(tar.extractfile("manifest.json"))
        executable = manifest["artifacts"][0]
        source_name = "scripts/graphcoder-e2e/artifacts.mjs"
        source_other = "scripts/windows-runner.py"
        replacements = {"source/" + source_name: tar.extractfile("source/" + source_other).read(),
                        "bin/" + executable["name"]: tar.extractfile("bin/" + executable["name"]).read(4096)}
    requirements = {"source/" + source_name: manifest["source"][source_name],
                    "bin/" + executable["name"]: executable["sha256"]}
    observations = []
    failed = []
    started = time.monotonic()
    for selected in requirements:
        for change in ("missing", "stale"):
            name = ("source" if selected.startswith("source/") else "companion") + "-" + change
            mutant = records / (name + ".tar.gz")
            changed = json.loads(json.dumps(manifest))
            if selected.startswith("source/"):
                if change == "missing":
                    del changed["source"][source_name]
                else:
                    changed["source"][source_name] = hashlib.sha256(replacements[selected]).hexdigest()
            elif change == "missing":
                changed["artifacts"] = [row for row in changed["artifacts"] if "bin/" + row["name"] != selected]
            else:
                changed["artifacts"][0]["sha256"] = hashlib.sha256(replacements[selected]).hexdigest()
            with tarfile.open(archive) as original, tarfile.open(mutant, "w:gz", compresslevel=1) as output:
                for member in original:
                    if member.name == selected and change == "missing":
                        continue
                    if member.name == "manifest.json":
                        data = json.dumps(changed).encode()
                    elif member.name == selected and change == "stale":
                        data = replacements[selected]
                    else:
                        output.addfile(member, original.extractfile(member))
                        continue
                    entry = tarfile.TarInfo(member.name)
                    entry.size = len(data)
                    output.addfile(entry, io.BytesIO(data))
            destination = records / name
            destination.mkdir()
            # The unchanged oracle requires rejection BEFORE capacity admission
            # or payload writes. The actual previous API reaches this barrier.
            original_capacity = api.require_capacity
            def payload_barrier(*args, **kwargs):
                raise AssertionError("selected inputs reached payload admission before missing/stale rejection")
            api.require_capacity = payload_barrier
            options = {"required_members": requirements} if "required_members" in inspect.signature(api.admit_archive).parameters else {}
            before = time.monotonic()
            try:
                api.admit_archive(destination, mutant, api.digest(mutant), **options)
            except ValueError as error:
                assert "step=selected-input-admission" in str(error) and selected in str(error), error
                observations.append(dict(case=name, rejected=True, seconds=time.monotonic() - before))
            except AssertionError as error:
                failed.append(dict(case=name, error=str(error), seconds=time.monotonic() - before))
            else:
                failed.append(dict(case=name, error="selected incomplete export was admitted"))
            finally:
                api.require_capacity = original_capacity
            assert list(destination.iterdir()) == [], "rejection wrote fixture bytes"
    result = dict(archive_sha256=api.digest(archive), api_sha256=api.digest(path),
                  requirements=requirements, observations=observations, failed=failed,
                  seconds=time.monotonic() - started)
    (records / "qualification.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    assert not failed, failed
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise RuntimeError("Export step=selected-input-controls failed: " + str(error)
                           + "; recovery=python3 scripts/windows-artifacts-required-test.py "
                           "CAPTURED_EXPORT D:/lane-scratch/windows/NEW_RECORDS") from error
