#!/usr/bin/env python3
"""One short-diagnostic decoder and warning ratchet for every gate adapter."""
import argparse
from collections import Counter
from hashlib import sha256
import contextlib
import io
from pathlib import Path
import re
import sys
import subprocess

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


def run_payload(command):
    """Bridge reviewed older payloads without trusting their partial summaries.

    Read their retained Cargo diagnostics before the private job retires. This
    is the same decoder as both new producers, never a second lint policy.
    """
    try:
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as error:
        print(f'error: GATE_RUNNER_INFRA payload start step failed: {error}; recovery command: '
              f'python3 {Path(__file__)} run ' + ' '.join(command))
        print('GATE_DONE rc=224', flush=True)
        return 224
    lines = []
    for line in child.stdout:
        lines.append(line)
        if not line.startswith((b'GATE_DONE rc=', b'GATE_CLIPPY_FINDING ', b'GATE_CLIPPY_ERROR ', b'GATE_CLIPPY_COMPLETE ')):
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
    status = child.wait()
    status = status if status >= 0 else 128 - status
    text = b''.join(lines).decode('utf-8', errors='replace')
    try:
        if re.findall(r'^GATE_DONE rc=(-?[0-9]+)$', text, re.M) != [str(status)]:
            raise ValueError('reviewed payload footer disagrees with its executor')
        if complete(text):
            for line in lines:
                if line.startswith((b'GATE_CLIPPY_FINDING ', b'GATE_CLIPPY_ERROR ', b'GATE_CLIPPY_COMPLETE ')):
                    sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
        elif re.search(r'^GATE_CLIPPY rc=', text, re.M):
            if 'GATE_CLIPPY_COMPLETE ' in text:
                raise ValueError('reviewed payload diagnostic receipt is damaged')
            output = Path.cwd()
            if Path('scripts/gate-target-cache.py').is_file():
                output = Path(subprocess.check_output([sys.executable, 'scripts/gate-target-cache.py', 'output'], text=True).strip())
            statuses = re.findall(r'^GATE_CLIPPY rc=(-?[0-9]+)$', text, re.M)
            if len(statuses) != 1:
                raise ValueError('reviewed payload has no unique Clippy status')
            expected = io.StringIO()
            with contextlib.redirect_stdout(expected):
                emit((output / '.gate-clippy.out').read_text(), int(statuses[0]))
            emitted = expected.getvalue().splitlines()
            # Replace a legacy presentation from the actual completed Cargo
            # output. Do not turn its missing coded errors into INFRA, or
            # trust a truncated legacy warning list as a passing PR.
            sys.stdout.write(expected.getvalue())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'error: GATE_RUNNER_INFRA Clippy stream attestation step failed: {error}; '
              f'recovery command: python3 {Path(__file__)} run ' + ' '.join(command))
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
