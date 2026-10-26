#!/usr/bin/env python3
"""One short-diagnostic decoder and warning ratchet for every gate adapter."""
import argparse
from collections import Counter
from hashlib import sha256
import contextlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sys
import subprocess
import tempfile

SHORT = re.compile(r'^(.+?):[0-9]+:[0-9]+: (warning|error)(\[[^\]]+\])?: (.+)$')
ERROR = re.compile(r'^error(?:\[[^\]]+\])?: .+')


def diagnostics(text):
    warnings, errors = Counter(), set()
    for line in text.splitlines():
        match = SHORT.match(line)
        if match:
            path, severity, code, message = match.groups()
            if severity == 'warning':
                warnings[path + ': ' + message] += 1
            else:
                errors.add(path + ': error' + (code or '') + ': ' + message)
        elif ERROR.match(line) and not line.startswith('error: could not compile '):
            errors.add(line)
    return warnings, errors


def emit(text, status):
    if status not in (0, 101):
        raise ValueError(f'Clippy executor did not complete a Cargo outcome (rc={status})')
    warnings, errors = diagnostics(text)
    lines = [f'GATE_CLIPPY_FINDING {count} {finding}\n' for finding, count in sorted(warnings.items())]
    lines += ['GATE_CLIPPY_ERROR ' + error + '\n' for error in sorted(errors)]
    if status != 0 and not errors:
        # A compilation summary alone cannot qualify decoded diagnostics.
        raise ValueError(f'Clippy failed without usable compiler diagnostics (rc={status})')
    block = ''.join(lines)
    # The receipt follows the entire block, not Cargo's earlier exit marker.
    # A successful Cargo process cannot prove its diagnostics reached the log.
    sys.stdout.write(block)
    print(f'GATE_CLIPPY_COMPLETE rc={status} records={len(lines)} sha256={sha256(block.encode()).hexdigest()}')
    sys.stdout.flush()


def complete(text):
    statuses = re.findall(r'^GATE_CLIPPY rc=(-?[0-9]+)$', text, re.M)
    receipts = re.findall(r'^GATE_CLIPPY_COMPLETE rc=(-?[0-9]+) records=([0-9]+) sha256=([0-9a-f]{64})$', text, re.M)
    if len(statuses) != 1 or int(statuses[0]) not in (0, 101) or len(receipts) != 1:
        return False
    status, count, digest = receipts[0]
    records = [line + '\n' for line in text.splitlines() if line.startswith(('GATE_CLIPPY_FINDING ', 'GATE_CLIPPY_ERROR '))]
    return (status == statuses[0] and int(count) == len(records)
            and sha256(''.join(records).encode()).hexdigest() == digest)


def payload_output(text):
    """Consume the producer's allocation; asking the allocator again is wrong."""
    directories = re.findall(r'^GATE_OUTPUT_DIRECTORY (.+)$', text, re.M)
    if len(directories) == 1:
        output = Path(directories[0])
        if not output.is_absolute() or not output.is_dir():
            raise ValueError('reviewed payload output directory is unavailable: ' + str(output))
        return output
    if directories or Path('scripts/gate-target-cache.py').is_file():
        raise ValueError('reviewed payload has no unique invocation-owned output directory')
    # Historical reviewed runners retained their diagnostics in cwd.
    return Path.cwd()


@contextlib.contextmanager
def payload_environment():
    """Give Bun private pipe descriptions, never the shell's inherited ones.

    Bun leaves O_NONBLOCK on inherited pipes when it exits. Ordinary shell,
    sed and Python writers that follow it cannot safely use that description.
    Two blocking relays isolate its stdout/stderr without changing reviewed
    payload code, output bytes, compiler flags or formatter policy. Node
    restores its flags; no unrelated tool is substituted here.
    """
    env = dict(os.environ)
    bun = shutil.which('bun', path=env.get('PATH'))
    if bun is None:
        yield env
        return
    with tempfile.TemporaryDirectory(prefix='gate-bun-stdio.') as directory:
        shim = Path(directory) / 'bun'
        shim.write_text('''#!/bin/bash
exec {gate_out}> >(cat)
gate_out_pid=$!
exec {gate_err}> >(cat >&2)
gate_err_pid=$!
''' + shlex.quote(bun) + ''' "$@" >&"$gate_out" 2>&"$gate_err"
gate_rc=$?
exec {gate_out}>&-
exec {gate_err}>&-
wait "$gate_out_pid"; gate_out_rc=$?
wait "$gate_err_pid"; gate_err_rc=$?
if [ "$gate_out_rc" != 0 ] || [ "$gate_err_rc" != 0 ]; then
  printf 'error: GATE_RUNNER_INFRA Bun stdio relay step failed: stdout_rc=%s stderr_rc=%s; recovery command: ' "$gate_out_rc" "$gate_err_rc" >&2
  printf '%q ' ''' + shlex.quote(bun) + ''' "$@" >&2
  printf '\\n' >&2
  exit 224
fi
exit "$gate_rc"
''')
        shim.chmod(0o700)
        env['PATH'] = directory + os.pathsep + env.get('PATH', '')
        yield env


def run_payload(command):
    try:
        with payload_environment() as env:
            return run_owned_payload(command, env)
    except OSError as error:
        print(f'error: GATE_RUNNER_INFRA Bun stdio isolation step failed: {error}; recovery command: '
              + shlex.join([sys.executable, str(Path(__file__)), 'run', *command]))
        print('GATE_DONE rc=224', flush=True)
        return 224


def run_owned_payload(command, env):
    """Bridge reviewed older payloads without trusting their partial summaries.

    Read their retained Cargo diagnostics before the private job retires. This
    is the same decoder as both new producers, never a second lint policy.
    """
    try:
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    except OSError as error:
        print(f'error: GATE_RUNNER_INFRA payload start step failed: {error}; recovery command: '
              + shlex.join([sys.executable, str(Path(__file__)), 'run', *command]))
        print('GATE_DONE rc=224', flush=True)
        return 224
    lines = []
    for line in child.stdout:
        lines.append(line)
        if not line.startswith((b'GATE_DONE rc=', b'GATE_CLIPPY_FINDING ', b'GATE_CLIPPY_ERROR ', b'GATE_CLIPPY_COMPLETE ')):
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
    child_status = child.wait()
    status = child_status if child_status >= 0 else 128 - child_status
    text = b''.join(lines).decode('utf-8', errors='replace')
    footers = re.findall(r'^GATE_DONE rc=(-?[0-9]+)$', text, re.M)
    try:
        if footers != [str(status)]:
            raise ValueError('reviewed payload footer disagrees with its executor')
        if complete(text):
            for line in lines:
                if line.startswith((b'GATE_CLIPPY_FINDING ', b'GATE_CLIPPY_ERROR ', b'GATE_CLIPPY_COMPLETE ')):
                    sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
        elif re.search(r'^GATE_CLIPPY rc=', text, re.M):
            if 'GATE_CLIPPY_COMPLETE ' in text:
                raise ValueError('reviewed payload diagnostic receipt is damaged')
            output = payload_output(text)
            statuses = re.findall(r'^GATE_CLIPPY rc=(-?[0-9]+)$', text, re.M)
            if len(statuses) != 1:
                raise ValueError('reviewed payload has no unique Clippy status')
            expected = io.StringIO()
            with contextlib.redirect_stdout(expected):
                emit((output / '.gate-clippy.out').read_text(), int(statuses[0]))
            # Replace a legacy presentation from the actual completed Cargo
            # output. Do not turn its missing coded errors into INFRA, or
            # trust a truncated legacy warning list as a passing PR.
            sys.stdout.write(expected.getvalue())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        # Preserve the process result and quarantined protocol bytes before
        # replacing an unqualified outcome. Private diagnostics retire with
        # the job, so a generic mismatch would destroy the causal evidence.
        print('\nGATE_PAYLOAD_OBSERVED ' + json.dumps({
            'child_status': child_status, 'exit_status': status, 'footers': footers,
            'bytes': sum(map(len, lines)), 'sha256': sha256(b''.join(lines)).hexdigest(),
        }, sort_keys=True), flush=True)
        for line in lines:
            if line.startswith((b'GATE_DONE rc=', b'GATE_CLIPPY_FINDING ', b'GATE_CLIPPY_ERROR ', b'GATE_CLIPPY_COMPLETE ')):
                sys.stdout.buffer.write(b'GATE_PAYLOAD_RETAINED ' + line)
                if not line.endswith(b'\n'):
                    sys.stdout.buffer.write(b'\n')
        sys.stdout.buffer.flush()
        print(f'error: GATE_RUNNER_INFRA Clippy stream attestation step failed: {error}; '
              'recovery command: ' + shlex.join([sys.executable, str(Path(__file__)), 'run', *command]))
        status = 224
    print(f'GATE_DONE rc={status}', flush=True)
    return status


def findings(text):
    warnings = Counter()
    for count, finding in re.findall(r'^GATE_CLIPPY_FINDING ([0-9]+) (.+)$', text, re.M):
        path, separator, message = finding.partition(': ')
        warnings[path + separator + re.sub(r'[0-9]+', 'N', message)] += int(count)
    # Decode retained raw errors too: historical emitters dropped error[E...]
    # while retaining those exact bytes. A gate must not trust that omission.
    raw = '\n'.join(line.removeprefix('GATE_CLIPPY_RAW ') for line in text.splitlines()
                    if line.startswith('GATE_CLIPPY_RAW '))
    _, errors = diagnostics(raw)
    errors.update(re.findall(r'^GATE_CLIPPY_ERROR (.+)$', text, re.M))
    return warnings, errors


def compare(text, baseline):
    statuses = re.findall(r'^GATE_CLIPPY rc=(-?[0-9]+)$', text, re.M)
    old_statuses = re.findall(r'^GATE_CLIPPY rc=(-?[0-9]+)$', baseline, re.M)
    if not statuses and not old_statuses:
        print('clippy: no baseline')
        return 0
    if len(statuses) != 1 or int(statuses[0]) not in (0, 101):
        raise ValueError('Clippy executor completion is missing, duplicated or operationally failed')
    current, errors = findings(text)
    previous, old_errors = findings(baseline)
    if not complete(text) or not complete(baseline):
        raise ValueError('Clippy diagnostic stream is incomplete or unverified; regenerate exact-base Clippy evidence')
    if statuses[0] != '0' and (not errors or any(e.startswith('clippy exited without parsed diagnostics') for e in errors)):
        raise ValueError('Clippy failed without usable compiler diagnostics (rc=' + statuses[0] + ')')
    changed = False
    for finding, count in sorted(current.items()):
        if count > previous[finding]:
            print(f'clippy: +{count - previous[finding]} {finding}')
            changed = True
    for error in sorted(errors - old_errors):
        print('clippy-error: ' + error)
        changed = True
    # A failed current execution cannot inherit a successful main outcome,
    # even if the retained diagnostic happens to equal an old marker.
    if statuses[0] == '101' and old_statuses == ['0'] and not changed:
        print('clippy-error: Clippy compile failed (rc=101), qualified main completed rc=0')
        changed = True
    return int(changed)


def main():
    if len(sys.argv) > 2 and sys.argv[1] == 'run':
        return run_payload(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('emit', 'compare'))
    parser.add_argument('log', type=Path)
    parser.add_argument('baseline_or_status')
    args = parser.parse_args()
    try:
        text = args.log.read_text(encoding='utf-8', errors='replace')
        if args.operation == 'emit':
            emit(text, int(args.baseline_or_status))
            return 0
        baseline = Path(args.baseline_or_status).read_text(encoding='utf-8', errors='replace')
        return compare(text, baseline)
    except (OSError, ValueError) as error:
        print(f'Clippy {args.operation} step failed: {error}; recovery command: '
              f'python3 "{Path(__file__).as_posix()}" {args.operation} "{args.log.as_posix()}" "{args.baseline_or_status}"', file=sys.stderr)
        return 4


if __name__ == '__main__':
    raise SystemExit(main())
