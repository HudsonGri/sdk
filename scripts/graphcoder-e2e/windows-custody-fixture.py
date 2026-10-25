"""Hold real kernel process teardown at EXIT_PROCESS_DEBUG_EVENT.

The unchanged oracle rejects an external host whose output EOF precedes the
death of its admitted root. No sleeps, polling, compilation or synthetic exit
state: ContinueDebugEvent is the only operation that releases physical death.
"""
import ctypes
from ctypes import wintypes as W
import json
import msvcrt
import os
from pathlib import Path
import subprocess
import socket
import sys
import tempfile
import uuid


kernel = ctypes.WinDLL("kernel32", use_last_error=True)


def api(name, args, result=W.BOOL):
    function = getattr(kernel, name)
    function.argtypes = args
    function.restype = result
    return function


open_process = api("OpenProcess", [W.DWORD, W.BOOL, W.DWORD], W.HANDLE)
wait = api("WaitForSingleObject", [W.HANDLE, W.DWORD], W.DWORD)
close = api("CloseHandle", [W.HANDLE])
create_event = api("CreateEventW", [ctypes.c_void_p, W.BOOL, W.BOOL, W.LPCWSTR], W.HANDLE)
open_event = api("OpenEventW", [W.DWORD, W.BOOL, W.LPCWSTR], W.HANDLE)
set_event = api("SetEvent", [W.HANDLE])
attach = api("DebugActiveProcess", [W.DWORD])
detach = api("DebugActiveProcessStop", [W.DWORD])
continue_debug = api("ContinueDebugEvent", [W.DWORD, W.DWORD, W.DWORD])
peek = api("PeekNamedPipe", [W.HANDLE, ctypes.c_void_p, W.DWORD, ctypes.c_void_p, ctypes.POINTER(W.DWORD), ctypes.c_void_p])
terminate = api("TerminateProcess", [W.HANDLE, W.UINT])


class EventData(ctypes.Union):
    _fields_ = [("alignment", ctypes.c_uint64), ("bytes", ctypes.c_ubyte * 160)]


class DebugEvent(ctypes.Structure):
    _fields_ = [("code", W.DWORD), ("pid", W.DWORD), ("tid", W.DWORD), ("data", EventData)]


next_debug = api("WaitForDebugEvent", [ctypes.POINTER(DebugEvent), W.DWORD])


def require(value):
    if not value:
        raise ctypes.WinError(ctypes.get_last_error())
    return value


def child():
    event_name = "Local\\acyclic-custody-proof-" + uuid.uuid4().hex
    event = require(create_event(None, True, False, event_name))
    print(json.dumps(dict(pid=os.getpid(), event=event_name,
                         admission=os.environ["ACYCLIC_PROCESS_CUSTODY_ADMISSION"])), flush=True)
    # Do not let the debuggee itself preserve the transport's output lease.
    with open(os.devnull, "wb") as empty:
        os.dup2(empty.fileno(), 1)
        os.dup2(empty.fileno(), 2)
    try:
        assert wait(event, 0xFFFFFFFF) == 0
    finally:
        close(event)


def advance(event):
    # Debugger-owned image/DLL file handles have no automatic close.
    if event.code in (3, 6):
        file = ctypes.c_void_p.from_buffer(event.data).value
        if file:
            close(file)
    require(continue_debug(event.pid, event.tid, 0x10002))


def probe(host):
    assert Path(host).is_absolute() and Path(host).is_file()
    scope = tempfile.TemporaryDirectory(prefix="physical-custody-")
    process = subprocess.Popen([host, "--session", scope.name],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    root = event_handle = None
    attached = False
    pending = None
    collected = False
    try:
        assert json.loads(process.stdout.readline())["ready"] is True
        # Session children own separate capture pipes. They never inherit the
        # host transport being measured, so only the custodian can preserve it.
        packet = dict(argv=[sys.executable, "-X", "utf8", str(Path(__file__).resolve()), "child"],
                      log=str(Path(scope.name) / "child.log"))
        process.stdin.write((json.dumps(packet) + "\n").encode("utf-8"))
        process.stdin.flush()
        pending_output = b""
        while b"\n" not in pending_output:
            output = json.loads(process.stdout.readline())
            assert "exit" not in output, output
            if output["stream"] == 0:
                pending_output += bytes(output["output"])
        ready = json.loads(pending_output.split(b"\n", 1)[0])
        root = require(open_process(0x100000 | 1, False, ready["pid"]))
        event_handle = require(open_event(2, False, ready["event"]))
        require(attach(ready["pid"]))
        attached = True
        # The attach breakpoint orders all initialization before the exit request.
        while True:
            event = DebugEvent()
            require(next_debug(ctypes.byref(event), 0xFFFFFFFF))
            breakpoint = event.code == 1 and int.from_bytes(bytes(event.data.bytes[:4]), "little") == 0x80000003
            advance(event)
            if breakpoint:
                break
        require(set_event(event_handle))
        while True:
            event = DebugEvent()
            require(next_debug(ctypes.byref(event), 0xFFFFFFFF))
            if event.code == 5:
                pending = event
                break
            advance(event)
        before = wait(root, 0)
        assert before == 258, ("debug event must hold real physical death", before)
        process.kill()
        owner_exit = process.wait()
        # Each reply crosses the custodian's event loop. Two requests prove
        # that it survives the retirement check following the first reply;
        # a logical-active-count-only custodian exits at that check while the
        # debugger still holds real process death. No scheduling delay orders it.
        address, port = ready["admission"].rsplit(":", 1)
        for _ in range(2):
            with socket.create_connection((address, int(port))) as admission:
                admission.sendall(ready["pid"].to_bytes(4, "little"))
                assert admission.recv(1) == b"\x01", "custodian retired before physical process death"
        available = W.DWORD()
        ctypes.set_last_error(0)
        held = bool(peek(msvcrt.get_osfhandle(process.stdout.fileno()), None, 0, None, ctypes.byref(available), None))
        pipe_error = ctypes.get_last_error()
        row = dict(owner_exit=owner_exit, root_wait_before_continue=before,
                   output_lease_held=held, output_bytes=available.value, pipe_error=pipe_error)
        advance(pending)
        pending = None
        attached = False
        assert wait(root, 0xFFFFFFFF) == 0
        process.communicate()
        collected = True
        row["root_wait_after_continue"] = wait(root, 0)
        print(json.dumps(row), flush=True)
        assert held, "output EOF was published while an admitted process remained physically alive"
    finally:
        if pending is not None:
            advance(pending)
            attached = False
        if attached:
            detach(ready["pid"])
        if root:
            if wait(root, 0) != 0:
                terminate(root, 1)
                wait(root, 0xFFFFFFFF)
            close(root)
        if event_handle:
            close(event_handle)
        if process.poll() is None:
            process.kill()
        if not collected:
            process.communicate()
        scope.cleanup()


if __name__ == "__main__":
    if sys.argv[1:] == ["child"]:
        child()
    else:
        assert len(sys.argv) == 2
        probe(sys.argv[1])
