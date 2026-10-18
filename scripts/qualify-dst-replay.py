#!/usr/bin/env python3
"""Run every DST test and the cross-process controls within one nine-second bound."""
import re
import subprocess
import sys
import time

executable, = sys.argv[1:]
second_test = "digest::cross_process_seeded_replay_digest"
started = time.monotonic()


def run_pair():
    commands = [
        ["--skip", second_test, "--skip", "sources::", "--skip", "manifest::",
         "--test-threads=1", "--nocapture"],
        [second_test, "sources::", "manifest::", "--test-threads=1", "--nocapture"],
    ]
    processes = []
    try:
        # Independent processes overlap their physical I/O; each replay still
        # polls its own original tasks on one seeded thread. Neither waits for
        # the other's tests before starting, and both share the same deadline.
        for arguments in commands:
            processes.append(subprocess.Popen([executable, *arguments],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        outputs = []
        for process in processes:
            remaining = max(0, 9 - (time.monotonic() - started))
            output, error = process.communicate(timeout=remaining)
            sys.stdout.buffer.write(output)
            sys.stderr.buffer.write(error)
            sys.stdout.flush()
            sys.stderr.flush()
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, process.args)
            outputs.append(output)
        return outputs
    except subprocess.TimeoutExpired as error:
        sys.stdout.buffer.write(error.stdout or b"")
        sys.stderr.buffer.write(error.stderr or b"")
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
        for process in processes:
            process.wait()
            process.stdout.close()
            process.stderr.close()


def equal(left, right):
    if left != right:
        raise AssertionError(f"cross-process trace diverged: {left!r} != {right!r}")


def digest(output, label):
    values = re.findall(rb"^" + label + rb"=([0-9a-f]{64})$", output, re.MULTILINE)
    if len(values) != 1:
        sys.exit(f"Expected exactly one {label!r}, found {len(values)}")
    return values[0]


# The two command filters partition all tests: one has the turn/process/cut
# scenarios, the other has the replay peer, erasure control and source scans.
# No test is omitted or repeated, including tests added in future modules.
first, second = run_pair()
for output in (first, second):
    if not re.search(rb"test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;", output):
        sys.exit("Each process must execute its selected tests without ignored cases")
equal(digest(first, b"DST_REPLAY_DIGEST"), digest(second, b"DST_REPLAY_DIGEST"))
print("Cross-process seeded trace: identical digest bytes", flush=True)

# These digests add one host-random startup value, cached for the entire process.
# The same-process comparison passes, but this exact comparison must fail.
try:
    equal(digest(first, b"DST_STARTUP_MUTANT_DIGEST"),
          digest(second, b"DST_STARTUP_MUTANT_DIGEST"))
except AssertionError as error:
    print(f"Startup-entropy negative control caught: {error}", flush=True)
else:
    sys.exit("Startup-entropy negative control escaped the cross-process oracle")
print(f"DST suite and cross-process proof elapsed: {time.monotonic() - started:.3f}s",
      flush=True)
