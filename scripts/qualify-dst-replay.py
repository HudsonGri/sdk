#!/usr/bin/env python3
"""Run every DST test and the cross-process controls within one nine-second bound."""
import re
import subprocess
import sys
import time

executable, = sys.argv[1:]
second_test = "digest::cross_process_seeded_replay_digest"
started = time.monotonic()


def run(*arguments):
    remaining = 9 - (time.monotonic() - started)
    if remaining <= 0:
        sys.exit("DST exceeded its shared nine-second bound")
    try:
        result = subprocess.run([executable, *arguments], capture_output=True,
                                timeout=remaining)
    except subprocess.TimeoutExpired as error:
        sys.stdout.buffer.write(error.stdout or b"")
        sys.stderr.buffer.write(error.stderr or b"")
        raise
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    sys.stdout.flush()
    sys.stderr.flush()
    result.check_returncode()
    return result.stdout


def equal(left, right):
    if left != right:
        raise AssertionError(f"cross-process trace diverged: {left!r} != {right!r}")


def digest(output, label):
    values = re.findall(rb"^" + label + rb"=([0-9a-f]{64})$", output, re.MULTILINE)
    if len(values) != 1:
        sys.exit(f"Expected exactly one {label!r}, found {len(values)}")
    return values[0]


# The first process runs every test except the one-world replay used by the
# second. The aggregate still covers every test; there is no duplicate world.
first = run("--skip", second_test, "--test-threads=1", "--nocapture")
second = run(second_test, "--exact", "--test-threads=1", "--nocapture")
if b"1 passed; 0 failed" not in second:
    sys.exit("The second process did not execute its replay")
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
