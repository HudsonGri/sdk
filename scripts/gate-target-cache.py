#!/usr/bin/env python3
"""Seed completed feature-matrix artifacts; execute every requested check.

The installed store owns request identity, immutable publication and leases.
This adapter groups all matrix workers into one completed snapshot, rather
than publishing only the first Cargo invocation in each worker's target.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0, '/home/var/bin')
import bb_cargo_cache as cache


def private_inputs(env, targets):
    """An environment flag alone cannot establish private mount custody."""
    try:
        meta = json.loads(Path(env['BUILD_SLOT_RECEIPT_META']).read_text())
        if env.get('CARGO_INCREMENTAL') != '0' or not cache.store.private_target(meta):
            return None
        if any(not Path(p).resolve().is_relative_to(Path('/work/target')) for p in targets):
            return None
        return meta
    except (OSError, KeyError, TypeError, ValueError):
        return None


def diagnostic(row):
    print('GATE_FEATURE_SNAPSHOT ' + json.dumps(row, sort_keys=True), file=sys.stderr, flush=True)


def execute(command, env):
    rc = subprocess.run(command, env=env).returncode
    return rc if rc >= 0 else 128 - rc


def executable_runner(source, env=None):
    """Never chmod an uploaded source: even an unchanged mode changes ctime."""
    env = os.environ if env is None else env
    source = Path(source).resolve(strict=True)
    if os.access(source, os.X_OK):
        return source
    temporary = Path(env.get('TMPDIR', '/tmp')).resolve()
    meta = private_inputs(env, ['/work/target'])
    if meta is not None and temporary.is_relative_to(Path(env['BB_JOB_HOME']).parent):
        destination = temporary / 'gate-test-runner.sh'
    else:
        destination = Path(tempfile.mkdtemp(prefix='gate-runner-', dir=temporary)) / 'runner.sh'
    fd, staged = tempfile.mkstemp(prefix='.gate-runner-', dir=destination.parent)
    try:
        with os.fdopen(fd, 'wb') as out:
            out.write(source.read_bytes()); out.flush(); os.fchmod(out.fileno(), 0o700)
            os.fsync(out.fileno())
        os.replace(staged, destination)
    finally:
        Path(staged).unlink(missing_ok=True)
    return destination


def matrix(target, command, env=None):
    env = dict(os.environ if env is None else env)
    target = Path(target)
    meta = private_inputs(env, [target])
    if meta is None or cache.store.disabled(env):
        diagnostic({'status': 'bypass', 'reason': 'not-proven-private-or-cache-disabled'})
        return execute(command, env)
    # Every worker target is a child of this matrix root. Seed and publish hold
    # its exclusive hierarchical lease, but release it before any Cargo child
    # acquires a descendant lease. Sibling test/clippy phases remain independent.
    env['CARGO_TARGET_DIR'] = str(target / 'worker-0')
    request_env = {**env, 'CARGO_TARGET_DIR': str(target)}
    request = {**meta, 'argv': ['gate-feature-matrix-v1', *command]}
    job = Path(env['BB_JOB_HOME']).parent
    identity = {}
    key = cache.store.request_key(request, request_env, job, components=identity)
    if not key:
        diagnostic({'status': 'bypass', 'reason': 'no-qualified-request-key'})
        return execute(command, env)
    target.mkdir(parents=True, exist_ok=True)
    session = cache.store.Session(cache.store.ROOT, key, target)
    eligible = False
    try:
        with cache.local_target_leases(job, target):
            occupied = bool(cache.store.children(target))
            if occupied or not cache.store.source_unchanged(request, env):
                session.receipt['status'] = 'private-target' if occupied else 'changed-input'
            else:
                try:
                    session.open()
                    eligible = True
                except Exception as error:
                    session.close()
                    # Only the fresh, job-owned target was eligible for a clone.
                    # Discard a partial clone before executing the real checks.
                    for item in cache.store.children(target):
                        if item.is_dir() and not item.is_symlink():
                            cache.store.remove_tree(item)
                        else:
                            item.unlink()
                    session.receipt.update(status='fresh-fallback', error=type(error).__name__)
        diagnostic({**session.receipt, 'request_identity': identity})
        rc = execute(command, env)
        if eligible and rc == 0:
            with cache.local_target_leases(job, target):
                if cache.store.source_unchanged(request, env):
                    try:
                        session.publish()
                    except Exception as error:
                        session.receipt['publish_error'] = type(error).__name__
                else:
                    session.receipt['publish'] = 'source-mutated'
        session.receipt['matrix_rc'] = rc
        diagnostic(session.receipt)
        # Keep the complete store receipt beside the job log; accounting uses
        # the same record transport as individual Cargo compilation requests.
        try:
            cache.record(env['BUILD_SLOT_RECEIPT_META'], {**session.receipt, 'phase': 'feature-matrix'})
        except OSError as error:
            print('error: GATE_RUNNER_INFRA could not retain feature snapshot receipt: ' + str(error), file=sys.stderr)
            return 224
        return rc
    finally:
        session.close()


def main(argv):
    if argv[:1] == ['runner'] and len(argv) == 2:
        print(executable_runner(argv[1]))
        return 0
    if argv[:1] == ['private'] and len(argv) > 1:
        return 0 if private_inputs(os.environ, argv[1:]) is not None else 1
    if len(argv) >= 4 and argv[0] == 'matrix' and argv[2] == '--':
        return matrix(argv[1], argv[3:])
    print('usage: gate-target-cache.py runner SOURCE | private TARGET... | matrix TARGET -- COMMAND...', file=sys.stderr)
    return 2


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
