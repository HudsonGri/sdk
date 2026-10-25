#!/usr/bin/env python3
"""One short-diagnostic decoder and warning ratchet for every gate adapter."""
import argparse
from collections import Counter
from pathlib import Path
import re
import sys

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
    for finding, count in sorted(warnings.items()):
        print(f'GATE_CLIPPY_FINDING {count} {finding}')
    for error in sorted(errors):
        print('GATE_CLIPPY_ERROR ' + error)
    if status != 0 and not errors:
        # Keep an unexplained failed executor visible even when Cargo only
        # prints its compilation summary. A matching sentinel is never PASS.
        print(f'GATE_CLIPPY_ERROR clippy exited without parsed diagnostics (rc={status})')
        raise ValueError(f'Clippy failed without usable compiler diagnostics (rc={status})')


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
    if statuses[0] != '0' and (not errors or any(e.startswith('clippy exited without parsed diagnostics') for e in errors)):
        raise ValueError('Clippy failed without usable compiler diagnostics (rc=' + statuses[0] + ')')
    if not old_statuses:
        print('clippy: no warning baseline')
        previous = current.copy()
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
