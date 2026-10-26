"""Reject changed Rust source without an exact, receipt-backed proof entry.

This is source drift enforcement, not effective-export enumeration or complete
mode qualification. The canonical rustdoc/TS/schema joins remain separate.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import sys

sys.dont_write_bytecode = True
from proof_inventory_scan import scan_file

MANIFEST = 'docs/proofs/entrypoint-inventory.json'
HST_POLICY_SOURCE = 'rust/crates/stream/src/conformance/wave1/coverage.rs'
HST_POLICY_INVARIANT = 'hst_reporting.availability_refusal'
SCOPES = ('rust/crates/', 'rust/fuzz/', 'compatibility/', 'plugin/', 'examples/sdk-services/')
RECOVERY = 'python scripts/check-proof-coverage.py --help'


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(root, *args):
    result = subprocess.run(['git', '-C', str(root), *args], capture_output=True)
    require(result.returncode == 0, 'git ' + ' '.join(args) + ': ' + result.stderr.decode('utf-8', errors='replace').strip())
    return result.stdout


def candidates(root, name):
    result = scan_file(name, root)
    rows = [('file_review', 1, '')]
    for row in result['entries']:
        rows.append((row['kind'], row.get('line', int(row['location'].rsplit(':', 1)[1])), row.get('name', '')))
    for row in result['unsafe']:
        if not row['test_or_example']:
            rows.append(('unsafe', row['line'], row.get('function') or ''))
    # All wildcard arms are candidates. Function-name heuristics cannot exempt
    # a mapper, and the file digest also fences constructs missed by discovery.
    for row in result['signals']:
        if row['kind'] == 'catch_all_arm':
            rows.append(('catch_all_arm', row['line'], row.get('function') or ''))
    return sorted(rows)


def validate_proofs(root, registry):
    spec = importlib.util.spec_from_file_location('wave1_receipt_policy', Path(__file__).with_name('check-wave1-inventory.py'))
    policy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(policy)
    policy.ROOT = root
    result = {}
    for name, proof in registry.items():
        require(proof['adapter'] == 'hst_reporting_v1', 'unsupported proof adapter: ' + proof['adapter'])
        policy.check_hst_reporting(proof['receipt'])
        receipt_path = root / proof['receipt']['repo_receipt']
        # Dependency input identity is not proof of dependency behavior. This
        # adapter admits only the reporting module exercised by its oracle.
        compiled = json.loads((receipt_path.parent / 'source.json').read_bytes())
        result[name] = dict(source=compiled, invariants={HST_POLICY_INVARIANT})
    return result


def check_review(root, inventory, changed):
    require(inventory['schema_version'] == 1, 'unsupported entrypoint inventory schema')
    reviews = inventory['reviewed_files']
    missing = [name for name in changed if name not in reviews]
    require(not missing, 'missing proof entries for changed Rust files: ' + ', '.join(missing))
    # Do not load an unrelated receipt to admit an empty changed-file selection.
    used = {entry['proof'] for name in changed for entry in reviews[name]['entries']}
    require(used <= inventory['proofs'].keys(), 'entry refers to a missing proof receipt')
    proofs = validate_proofs(root, {name: inventory['proofs'][name] for name in used})
    checked = 0
    for name in changed:
        review = reviews[name]
        require(review['sha256'] == digest(root / name), 'changed source invalidates reviewed file digest: ' + name)
        rows = review['entries']
        require(Counter((row['kind'], row['line'], row['name']) for row in rows) == Counter(candidates(root, name)), 'public/unsafe/wildcard proof entry multiset differs: ' + name)
        for row in rows:
            require(row['invariants'] and all(isinstance(value, str) and value.strip() for value in row['invariants']), 'entry lacks invariant: ' + name)
            require(set(row['invariants']) <= proofs[row['proof']]['invariants'], 'receipt does not prove the registered invariant: ' + name)
            require(proofs[row['proof']]['source'].get(name) == review['sha256'], 'receipt did not exercise the reviewed source: ' + name)
            if row['kind'] == 'unsafe':
                require(isinstance(row.get('safety_argument'), str) and row['safety_argument'].strip(), 'unsafe entry lacks safety argument: ' + name)
            checked += 1
    return dict(status='proof_entry_drift_pass', changed_rust_files=len(changed), entries=checked,
                scope='exact source and receipt registration only; not complete exports, invariants or cross-mode qualification')


def check(root, base):
    global RECOVERY
    root = root.resolve()
    base = git(root, 'rev-parse', '--verify', '--end-of-options', base + '^{commit}').decode().strip()
    RECOVERY = shlex.join(['python', str(Path(__file__).resolve()), '--base', base, '--root', str(root)])
    require(not git(root, 'status', '--porcelain', '--untracked-files=no').strip(), 'tracked worktree differs from HEAD; commit the reviewed artifact before checking')
    manifest = root / MANIFEST
    require(manifest.read_bytes() == git(root, 'show', 'HEAD:' + MANIFEST), 'entrypoint inventory must be committed at HEAD')
    inventory = json.loads(manifest.read_bytes())
    paths = git(root, 'diff', '--name-only', '-z', '--diff-filter=ACMR', base, 'HEAD', '--').decode('utf-8').split('\0')
    changed = sorted(name for name in paths if name.endswith('.rs') and name.startswith(SCOPES))
    # A rename appears as the destination and must be freshly bound. A deletion
    # needs no stale source proof; deletion-only consumers are not grandfathered.
    for name in changed:
        require((root / name).is_file() and not (root / name).is_symlink(), 'Rust source must be a regular file: ' + name)
        require((root / name).read_bytes() == git(root, 'show', 'HEAD:' + name), 'Rust source differs from HEAD: ' + name)
    return check_review(root, inventory, changed)


class Arguments(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


if __name__ == '__main__':
    step = 'read_arguments'
    try:
        parser = Arguments(description=__doc__)
        parser.add_argument('--base', required=True, help='actual reviewed PR base commit; never an inferred fallback')
        parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
        args = parser.parse_args()
        step = 'check_changed_proof_entries'
        print(json.dumps(check(args.root, args.base)))
    except (OSError, ValueError, KeyError, TypeError, ImportError, subprocess.SubprocessError) as error:
        print(f'VERIFY_COVERAGE_FAILURE step={step} error={error}; recovery: {RECOVERY}; add the exact source/invariant/receipt entries or restore the unreviewed edit', file=sys.stderr)
        sys.exit(1)
