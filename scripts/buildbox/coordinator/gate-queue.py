#!/usr/bin/env python3
"""The coordinator's sole queue writer: ordered sets, atomic files, one lock.

SQLite supplies the crash-released process lock on both Windows and POSIX.
Queue files remain inspectable text; no caller does read/modify/write itself.
Transfers publish the destination before removing the source. Claims stay in
the active queue until the drain acknowledges them, including partial clears,
failed output delivery and coordinator death. Admission removes duplicates.
"""
import argparse
import importlib.util
import json
from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import subprocess
import tempfile

QUEUES = {'pending', 'held-pending', 'baseline-pending', 'active'}


def members(path):
    try:
        text = path.read_text(encoding='utf-8')
    except FileNotFoundError:
        return []
    values = text.split()
    if any(not p.isascii() or not p.isdecimal() or int(p) <= 0 for p in values):
        raise ValueError(f'invalid PR queue: {path}')
    return sorted(set(map(int, values)))


def publish(path, prs):
    fd, name = tempfile.mkstemp(prefix='.queue-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
            stream.writelines(f'{p}\n' for p in sorted(set(prs)))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextmanager
def locked(state, name=".queue-lock.sqlite3", timeout=5):
    state.mkdir(parents=True, exist_ok=True)
    # Native database locking avoids PID files, stale-lock recovery and a
    # second per-OS implementation. No command is retried on failure.
    with sqlite3.connect(state/name, timeout=timeout) as lock:
        lock.execute('BEGIN IMMEDIATE')
        yield lock


def queue_path(state, name):
    if name not in QUEUES:
        raise ValueError(f'unknown queue: {name}')
    return state/(name+'.txt')


def add(state, name, prs):
    path = queue_path(state, name)
    if any(not isinstance(p, int) or p <= 0 for p in prs):
        raise ValueError('PR numbers must be positive integers')
    with locked(state):
        publish(path, [*members(path), *prs])


def take(state, names):
    """Called under the queue lock; persist custody before clearing sources."""
    paths = [queue_path(state, name) for name in names]
    claim = queue_path(state, 'active')
    prs = sorted({p for path in [claim, *paths] for p in members(path)})
    publish(claim, prs)
    for path in paths:
        if path != claim:
            publish(path, [])
    return prs


def acknowledge(state, prs):
    if any(not isinstance(p, int) or p <= 0 for p in prs):
        raise ValueError('PR numbers must be positive integers')
    claim = queue_path(state, 'active')
    publish(claim, set(members(claim)) - set(prs))


def withdraw(state, prs):
    """Release frozen requests only at the same idle boundary as dispatch."""
    if not prs or any(not isinstance(p,int) or p<=0 for p in prs):
        raise ValueError('withdraw requires positive PR numbers')
    state.mkdir(parents=True,exist_ok=True)
    lease=state/'.lock'
    try:lease.mkdir()
    except FileExistsError:
        import shlex
        command=shlex.join([sys.executable,str(Path(__file__).resolve()),str(state),'withdraw','all',*map(str,prs)])
        raise ValueError('kind=QUEUE_BUSY step=withdraw-frozen-request: active gate retains custody; '
                         'recovery command (after its terminal idle boundary): '+command)
    try:
        with locked(state):
            before={name:members(queue_path(state,name)) for name in sorted(QUEUES)}
            after={name:sorted(set(values)-set(prs)) for name,values in before.items()}
            for name,values in after.items():publish(queue_path(state,name),values)
            return {'operation':'withdraw','prs':sorted(set(prs)),'before':before,'after':after}
    finally:lease.rmdir()


def baseline_state(current):
    current.execute('CREATE TABLE IF NOT EXISTS baseline_ready (generation INTEGER NOT NULL, identity TEXT NOT NULL)')
    current.execute("INSERT INTO baseline_ready SELECT 0, '' WHERE NOT EXISTS (SELECT 1 FROM baseline_ready)")
    return current.execute('SELECT generation, identity FROM baseline_ready').fetchone()


def baseline_generation(state):
    with locked(state) as current:
        return baseline_state(current)[0]


def park_baseline(state, observed, prs):
    if observed < 0 or any(not isinstance(p, int) or p <= 0 for p in prs):
        raise ValueError('baseline parking requires a nonnegative generation and positive PR numbers')
    with locked(state) as current:
        generation, _ = baseline_state(current)
        # Qualification may have finished after the selector looked, but
        # before this parking transaction. That changed input belongs in the
        # next dispatch, not in a queue waiting for another main run.
        name = 'pending' if generation != observed else 'baseline-pending'
        path = queue_path(state, name)
        publish(path, [*members(path), *prs])
        return name


def publish_baseline(state, identity):
    """Publish changed evidence and report durable, still-unconsumed work.

    Pending admission and active claims are the dispatch outbox. They survive
    coordinator death before launch, dispatcher death before claiming, and
    death after claiming but before acknowledgement. Unchanged evidence never
    transfers parked scopes; it only wakes custody already in that outbox.
    """
    if not identity:
        raise ValueError('baseline publication requires an evidence identity')
    with locked(state) as current:
        generation, previous = baseline_state(current)
        pending = queue_path(state, 'pending')
        active = queue_path(state, 'active')
        if previous == identity:
            return bool(members(pending) or members(active))
        waiting = queue_path(state, 'baseline-pending')
        # Persist destination custody before clearing the parked queue. Both
        # orders of completion versus parking share this same transaction.
        publish(pending, [*members(pending), *members(waiting)])
        publish(waiting, [])
        current.execute('UPDATE baseline_ready SET generation=?, identity=?', (generation+1, identity))
        return bool(members(pending) or members(active))


def dispatch(state, script):
    lease = locked(state, '.drain-lock.sqlite3', timeout=0)
    owned = False
    try:
        # Admission and final lease release use the same queue transaction.
        # A late producer either wakes this owner or acquires the released lease.
        with locked(state) as current:
            current.execute('CREATE TABLE IF NOT EXISTS dispatch_wake (requested INTEGER NOT NULL)')
            current.execute('INSERT INTO dispatch_wake SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM dispatch_wake)')
            try:
                lease.__enter__()
            except sqlite3.OperationalError as error:
                if getattr(error, 'sqlite_errorcode', None) != sqlite3.SQLITE_BUSY:
                    raise
                current.execute('UPDATE dispatch_wake SET requested=1')
                print('GATE_DRAIN_BUSY wake-up retained for owner', file=sys.stderr)
                return 0
            owned = True
            current.execute('UPDATE dispatch_wake SET requested=0')
        shell = shutil.which('bash')
        if shell is None:
            raise FileNotFoundError('Bash is not available on PATH')
        while True:
            result = subprocess.call([shell, script, '--owned-by-dispatch'])
            with locked(state) as current:
                again = current.execute('SELECT requested FROM dispatch_wake').fetchone()[0]
                if again:
                    current.execute('UPDATE dispatch_wake SET requested=0')
                else:
                    # No producer can enter between this decision and release.
                    lease.__exit__(None, None, None)
                    owned = False
            if not again:
                return result
    finally:
        if owned:
            lease.__exit__(None, None, None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('state', type=Path)
    parser.add_argument('operation', choices=['add', 'take', 'move', 'has', 'ack', 'dispatch', 'withdraw',
                                            'baseline-version', 'baseline-park', 'baseline-ready'])
    parser.add_argument('queue')
    parser.add_argument('values', nargs='*')
    args = parser.parse_args()
    try:
        if args.operation=='withdraw':
            if args.queue!='all':raise ValueError('withdraw requires all queues')
            print(json.dumps(withdraw(args.state,list(map(int,args.values)))))
            return 0
        if args.operation == 'baseline-ready':
            spec = importlib.util.spec_from_file_location('queue_baseline', Path(__file__).resolve().with_name('gate-baseline.py'))
            baseline = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(baseline)
            evidence = baseline.publication_identity(args.queue, args.values)
            print(int(publish_baseline(args.state, evidence)))
            return 0
        if args.operation in ('baseline-version', 'baseline-park'):
            if args.queue != 'baseline-pending':
                raise ValueError('baseline operations require baseline-pending')
            if args.operation == 'baseline-version':
                if args.values:
                    raise ValueError('baseline-version accepts no values')
                print(baseline_generation(args.state))
            else:
                if len(args.values) < 2:
                    raise ValueError('baseline-park requires the observed generation and PR numbers')
                destination = park_baseline(args.state, int(args.values[0]), list(map(int, args.values[1:])))
                print(f'GATE_BASELINE_HANDOFF destination={destination}', file=sys.stderr)
            return 0
        if args.operation == 'dispatch':
            if args.values:
                raise ValueError('dispatch accepts one drain script')
            return dispatch(args.state, args.queue)
        if args.operation == 'add':
            add(args.state, args.queue, list(map(int, args.values)))
            return 0
        source = queue_path(args.state, args.queue)
        with locked(args.state):
            if args.operation == 'ack':
                if args.queue != 'active':
                    raise ValueError('only active claims can be acknowledged')
                acknowledge(args.state, list(map(int, args.values)))
                return 0
            if args.operation == 'has':
                if args.values:
                    raise ValueError('has accepts no additional arguments')
                return 0 if members(source) else 1
            if args.operation == 'move':
                if len(args.values) != 1:
                    raise ValueError('move requires one destination queue')
                dest = queue_path(args.state, args.values[0])
                if dest == source:
                    raise ValueError('move requires different queues')
                publish(dest, [*members(dest), *members(source)])
                publish(source, [])
            else:
                prs = take(args.state, [args.queue, *args.values])
                sys.stdout.writelines(f'{p}\n' for p in prs)
                sys.stdout.flush()
        return 0
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f'GATE preparation-INFRA: queue {args.operation} step failed: {error}; '
              f'recovery command: python {Path(__file__).as_posix()} {args.state.as_posix()} has pending', file=sys.stderr)
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
