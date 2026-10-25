#!/usr/bin/env python3
"""Exercise the shared native host protocol on each real operating system."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid


def start(host, root):
    process = subprocess.Popen([host, "--session", str(root)], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, text=True, encoding="utf-8")
    assert json.loads(process.stdout.readline())["ready"] is True
    return process


def submit(process, root, mode, group):
    packet = dict(argv=[sys.executable, str(Path(__file__).parent / "graphcoder-e2e/tree-fixture.py"),
        mode, *(["--separate-group"] if group else [])], log=str(root / (uuid.uuid4().hex + ".log")))
    process.stdin.write(json.dumps(packet) + "\n")
    process.stdin.flush()


def output(process):
    pending = b""
    while True:
        row = json.loads(process.stdout.readline())
        assert "exit" not in row, row
        if row["stream"] == 0:
            pending += bytes(row["output"])
            if b"\n" in pending:
                return json.loads(pending.split(b"\n", 1)[0])["pids"]


class Generations:
    """Retain Windows kernel generations while the fixture is still live."""

    def __init__(self, pids):
        self.pids = pids
        self.handles = []
        self.queue = None
        if sys.platform == "darwin":
            import select
            self.queue = select.kqueue()
            self.watched = set()
            for pid in pids:
                try:
                    self.queue.control([select.kevent(pid, filter=select.KQ_FILTER_PROC,
                        flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                        fflags=select.KQ_NOTE_EXIT)], 0)
                    self.watched.add(pid)
                except ProcessLookupError:
                    pass
        if os.name == "nt":
            import ctypes
            self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            self.kernel.OpenProcess.restype = ctypes.c_void_p
            self.kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
            self.kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
            self.kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            for pid in pids:
                handle = self.kernel.OpenProcess(0x100000, False, pid)
                if handle:
                    self.handles.append((pid, handle))
                else:
                    assert ctypes.get_last_error() == 87, (pid, ctypes.get_last_error())

    def dead(self):
        try:
            if self.queue is not None:
                import select
                # NOTE_EXIT proves the retained generation cannot act, even
                # while launchd still owns its zombie's eventual reap.
                events = self.queue.control(None, len(self.pids), 0)
                exited = {event.ident for event in events
                          if event.fflags & select.KQ_NOTE_EXIT and not event.flags & select.KQ_EV_ERROR}
                assert self.watched <= exited, f"live descendants {self.watched - exited}"
                return
            for pid, handle in self.handles:
                assert self.kernel.WaitForSingleObject(handle, 0) == 0, f"live descendant {pid}"
            if os.name != "nt":
                dead(self.pids)
        finally:
            if self.queue is not None:
                self.queue.close()
            for _, handle in self.handles:
                self.kernel.CloseHandle(handle)
            self.handles = []


def dead(pids):
    for pid in pids:
        if os.name != "nt":
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            if sys.platform == "linux" and Path(f"/proc/{pid}/stat").exists():
                assert Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] in ("Z", "X"), f"live descendant {pid}"
            else:
                raise AssertionError(f"surviving descendant {pid}")


def finish(process):
    process.stdin.close()
    for _ in process.stdout:
        pass
    assert process.wait() == 0
    process.stdout.close()


def main():
    host = str(Path(sys.argv[1]).resolve())
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="native-process-host-") as directory:
        root = Path(directory)
        for group in (False, True):
            print(json.dumps(dict(phase="normal", separate_group=group)), flush=True)
            # Normal root exit drains before the next session command.
            process = start(host, root / ("normal" + str(group)))
            try:
                submit(process, root, "exit", group)
                generations = Generations(output(process))
                for line in process.stdout:
                    row = json.loads(line)
                    if "exit" in row:
                        assert row["exit"] == 0
                        break
                generations.dead()
            finally:
                finish(process)
            # EOF cancels while keeping admission until physical tree drain.
            custody = root / ("cancel" + str(group))
            print(json.dumps(dict(phase="cancel", separate_group=group)), flush=True)
            process = start(host, custody)
            submit(process, root, "wait", group)
            generations = Generations(output(process))
            finish(process)
            generations.dead()
            # Abrupt host death releases its lease; a successor must fence
            # durable records before READY or before accepting any command.
            custody = root / ("abrupt" + str(group))
            print(json.dumps(dict(phase="abrupt", separate_group=group)), flush=True)
            process = start(host, custody)
            submit(process, root, "wait", group)
            generations = Generations(output(process))
            process.kill()
            process.wait()
            process.stdin.close()
            process.stdout.close()
            successor = start(host, custody)
            try:
                generations.dead()
            finally:
                finish(successor)
        # One malformed control command fails without starting any process.
        process = start(host, root / "malformed")
        process.stdin.write('{"argv":[]}\n')
        process.stdin.flush()
        process.stdin.close()
        process.stdout.read()
        assert process.wait() == 125
    seconds = time.monotonic() - started
    assert seconds < 10, seconds
    print(json.dumps(dict(exit=0, cases=7, seconds=seconds)))


if __name__ == "__main__":
    main()
