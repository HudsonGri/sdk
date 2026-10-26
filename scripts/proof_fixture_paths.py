"""Locate immutable proof captures after their physical fixture relocation.

Captured receipts retain the logical paths recorded by their real producers.
Only that exact historical prefix maps to the canonical test-input directory;
there is no fallback to a second evidence directory.
"""
from pathlib import Path

CAPTURE_PREFIX = 'docs/proofs/evidence/'
FIXTURE_PREFIX = 'scripts/tests/fixtures/proof-policy/'


def repository_relative(relative):
    return FIXTURE_PREFIX + relative[len(CAPTURE_PREFIX):] if relative.startswith(CAPTURE_PREFIX) else relative


def repository_path(root, relative):
    root = Path(root).resolve()
    path = (root / repository_relative(relative)).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f'path escapes repository: {relative}')
    return path
