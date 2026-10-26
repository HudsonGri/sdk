#!/usr/bin/env python3
"""Recognize an unfinished libtest case at a confirmed idle-watchdog cutoff.

A slow warning alone proves nothing. A transport loss, admission ceiling or
spot stop remains INFRA. Terminal compute receipts retain the watchdog cause;
this reader supplies a separate source verdict, never a fabricated completion.
"""
import argparse
from pathlib import Path
import re
import shlex
import sys

RUNNING = re.compile(r'^\s+Running (.+) \((.+)\)$')
SLOW = re.compile(r'^test (.+) has been running for over [0-9]+ seconds$')
FINISHED = re.compile(r'^test (.+) \.\.\. (?:ok|FAILED|ignored)(?:\b|$)')
WATCHDOG = re.compile(r'^\[buildbox\] hang reaper: .*presumed hung .*killed \(rc 124\), admission slot freed\.')
BINARY_CUTOFF = re.compile(r'^\[buildbox\] test binary (\S+) still running after [0-9]+s: killed \(rc (124|137)\)\.')
RERUN = re.compile(r'^error: test failed, to rerun pass `([^`]+)`$')
ABNORMAL = re.compile(r"^\s+process didn't exit successfully: `([^`]+)` \(exit status: (124|137)\)$")


def observations(text):
    active = None
    runs = []
    watchdog = False
    for line in text.splitlines():
        running = RUNNING.match(line)
        if running:
            active = {'label': running.group(1), 'executable': running.group(2),
                      'pending': {}, 'cutoff': False, 'confirmed': False, 'rerun': []}
            runs.append(active)
        slow = SLOW.match(line)
        if slow and active:
            # Reinserting preserves the order of the latest observed starts.
            active['pending'].pop(slow.group(1), None)
            active['pending'][slow.group(1)] = None
        finished = FINISHED.match(line)
        if finished and active:
            active['pending'].pop(finished.group(1), None)
        cutoff = BINARY_CUTOFF.match(line)
        if cutoff:
            if not active or Path(active['executable']).name != cutoff.group(1):
                raise ValueError('binary cutoff has no matching active Cargo executable')
            active['cutoff'] = int(cutoff.group(2))
        rerun = RERUN.match(line)
        if rerun and active:
            active['rerun'] = shlex.split(rerun.group(1))
        abnormal = ABNORMAL.match(line)
        if abnormal and active and active['cutoff']:
            # The wrapper's basename diagnostic is insufficient on its own.
            # Cargo must confirm the same status and exact executable path.
            active['confirmed'] = (active['executable'] in shlex.split(abnormal.group(1))
                                   and int(abnormal.group(2)) == active['cutoff'])
        if WATCHDOG.match(line):
            watchdog = True
    return active, runs, watchdog


def unfinished(text, executor_rc):
    active, _, watchdog = observations(text)
    # The wrapper's stderr and the child's stdout can be forwarded in either
    # order. A completion arriving after the watchdog still retires the case.
    if watchdog and executor_rc == 124 and active and active['pending']:
        names = list(reversed(active['pending']))
        return f'hung test {names[0]} in {active["label"]}; unfinished cases: {", ".join(names)}; confirmed idle-watchdog cutoff rc=124'
    return None


def binary_cutoffs(text):
    _, runs, _ = observations(text)
    reasons = []
    for run in runs:
        if not run['cutoff']:
            continue
        if not run['confirmed']:
            raise ValueError('binary cutoff missing matching Cargo abnormal-exit evidence: ' + run['executable'])
        pending = list(reversed(run['pending']))
        culprit = pending[0] if pending else 'unidentified case (binary did not complete)'
        command = ['pwsh', '-NoProfile', '-File', 'Q:/lanes/bs.ps1', 'cargo', 'test',
                   '--locked', '--all-features', *run['rerun']]
        if pending:
            command += [pending[0], '--', '--exact', '--nocapture']
        reasons.append(f'incomplete test execution: {culprit} in {run["label"]} executable={run["executable"]}; '
                       f'confirmed per-binary cutoff rc={run["cutoff"]}; recovery command: {shlex.join(command)}')
    return '; '.join(reasons) or None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', type=Path)
    parser.add_argument('--executor-rc', type=int, required=True)
    parser.add_argument('--binary-cutoffs', action='store_true')
    args = parser.parse_args()
    try:
        text = args.log.read_text(encoding='utf-8', errors='replace')
        evidence = binary_cutoffs(text) if args.binary_cutoffs else unfinished(text, args.executor_rc)
    except (OSError, ValueError) as error:
        recovery = ['python', 'Q:/lanes/gate-hang.py', str(args.log), '--executor-rc', str(args.executor_rc)]
        if args.binary_cutoffs:
            recovery.append('--binary-cutoffs')
        print(f'GATE evidence read/identity step failed: {error}; recovery command: {shlex.join(recovery)}', file=sys.stderr)
        return 6
    if not evidence:
        return 1
    print(evidence)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
