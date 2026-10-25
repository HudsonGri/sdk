#!/usr/bin/env python3
"""Exercise the real Windows runner with the installed Python, without builds."""

import importlib.util
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def load(path):
    spec = importlib.util.spec_from_file_location("windows_runner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    if os.name != "nt":
        raise OSError("Windows runner qualification requires real Windows")
    source = Path(__file__).resolve().parent.parent
    module_path = Path(sys.argv[2]) if len(sys.argv) > 2 else source / "scripts/windows-runner.py"
    module = load(module_path)
    records = Path(sys.argv[1]).resolve()
    records.mkdir(parents=True)
    host = module.admitted_host(
        source, os.environ["E2E_PROCESS_HOST"], os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"]
    )
    python = str(Path(sys.executable).resolve())
    checksum = module.digest(python)
    probe = (
        "import os,pathlib,sys; "
        "home=pathlib.Path(os.environ['HOME']); "
        "marker=home/'state'; assert not marker.exists(), 'retained fixture state'; "
        "marker.write_text('owned'); "
        "assert home==pathlib.Path(os.environ['USERPROFILE']); "
        "assert pathlib.Path(os.environ['CODEX_HOME']).is_relative_to(home); "
        "assert os.environ['RUST_TEST_THREADS']=='4'; "
        "assert 'RUST_MIN_STACK' not in os.environ; "
        "assert not any(k.upper().endswith('API_KEY') or 'AUTH_TOKEN' in k.upper() for k in os.environ); "
        "assert '--test-threads=4' not in sys.argv; print('generic command passed')"
    )
    start = time.monotonic()
    old_key = os.environ.get("RUNNER_PROBE_API_KEY")
    os.environ["RUNNER_PROBE_API_KEY"] = "fixture-only-no-secret"
    try:
        with module.Commands(records, source, host) as commands:
            for label in ("shipping-first", "shipping-second"):
                assert commands.run([python, "-c", probe], cwd=source, name=label, sha256=checksum) == 0
            libtest = "import sys; assert sys.argv.count('--test-threads=4')==1; print('test result: ok. 1 passed; 0 failed;')"
            assert commands.run([python, "-c", libtest], cwd=source, name="libtest-adapter", sha256=checksum, libtest=True) == 0
            lease = (
                "import ctypes; k=ctypes.WinDLL('kernel32',use_last_error=True); "
                "k.OpenMutexW.argtypes=[ctypes.c_uint32,ctypes.c_bool,ctypes.c_wchar_p]; "
                "k.OpenMutexW.restype=ctypes.c_void_p; "
                "k.WaitForSingleObject.argtypes=[ctypes.c_void_p,ctypes.c_uint32]; "
                "k.CloseHandle.argtypes=[ctypes.c_void_p]; "
                "h=k.OpenMutexW(0x100000,False,'Global\\\\AcyclicWindowsTestRun'); assert h; "
                "assert k.WaitForSingleObject(h,0)==0x102, 'fleet lease escaped'; "
                "assert k.CloseHandle(h); print('fleet exclusion passed')"
            )
            assert commands.run([python, "-c", lease], cwd=source, name="cross-command-fleet-exclusion", sha256=checksum) == 0
            # Force the same identity: the second call must reject before a child.
            fixed = module.uuid.uuid4()
            original_uuid = module.uuid.uuid4
            module.uuid.uuid4 = lambda: fixed
            try:
                assert commands.run([python, "-c", probe], cwd=source, name="exclusive-first", sha256=checksum) == 0
                count = len(commands.results)
                try:
                    commands.run([python, "-c", probe], cwd=source, name="exclusive-collision", sha256=checksum)
                except FileExistsError:
                    pass
                else:
                    raise AssertionError("reused a mutable fixture identity")
                assert len(commands.results) == count
            finally:
                module.uuid.uuid4 = original_uuid
            try:
                commands.run([python, "-c", "print('no libtest results')"], cwd=source, name="zero-tests", sha256=checksum, libtest=True)
            except RuntimeError as error:
                assert "ZERO" in str(error)
            else:
                raise AssertionError("accepted missing libtest cases")
            count = len(commands.results)
            try:
                commands.run([python, "-c", probe], cwd=source, name="wrong-artifact", sha256="0" * 64)
            except ValueError:
                pass
            else:
                raise AssertionError("ran an unadmitted executable")
            assert len(commands.results) == count
            # Observe physical death through handles opened while both processes
            # are live, rather than looking up potentially recycled PIDs later.
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_bool, ctypes.c_uint32]
            kernel.OpenProcess.restype = ctypes.c_void_p
            kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            handles = []
            def interrupt(*values, **kwargs):
                line = str(values[0]) if values else ""
                if line.startswith("DRAIN_READY "):
                    for pid in json.loads(line.removeprefix("DRAIN_READY ")):
                        handle = kernel.OpenProcess(0x100000, False, pid)
                        assert handle, ctypes.get_last_error()
                        handles.append(handle)
                    raise RuntimeError("fixture interruption")
                print(*values, **kwargs)
            module.print = interrupt
            tree = (
                "import json,os,subprocess,sys,threading; "
                "child=subprocess.Popen([sys.executable,'-c','import threading; threading.Event().wait()']); "
                "print('DRAIN_READY '+json.dumps([os.getpid(),child.pid]),flush=True); "
                "threading.Event().wait()"
            )
            try:
                try:
                    commands.run([python, "-c", tree], cwd=source, name="interruption-drain", sha256=checksum)
                except RuntimeError as error:
                    assert str(error) == "fixture interruption"
                else:
                    raise AssertionError("interruption was lost")
                assert len(handles) == 2
                for handle in handles:
                    assert kernel.WaitForSingleObject(handle, 0) == 0, "released before physical process death"
            finally:
                del module.print
                for handle in handles:
                    assert kernel.CloseHandle(handle)
            assert commands.run([python, "-c", probe], cwd=source, name="after-drain", sha256=checksum) == 0
        assert len({result["fixture"] for result in commands.results}) == len(commands.results)
        plan = dict(source=str(source), records=str(records / "cli"), commands=[dict(
            argv=[python, "-c", probe], cwd=str(source), name="shipping-plan", sha256=checksum,
        )])
        plan_path = records / "command-plan.json"
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        assert subprocess.call([python, "-X", "utf8", str(module_path), str(plan_path)]) == 0
    finally:
        if old_key is None:
            os.environ.pop("RUNNER_PROBE_API_KEY", None)
        else:
            os.environ["RUNNER_PROBE_API_KEY"] = old_key
    seconds = time.monotonic() - start
    result = dict(exit=0, seconds=seconds, under_10s=seconds < 10, commands=len(commands.results), runner_sha256=module.digest(module_path))
    (records / "qualification.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    assert result["under_10s"], result
    print(json.dumps(result))


if __name__ == "__main__":
    main()
