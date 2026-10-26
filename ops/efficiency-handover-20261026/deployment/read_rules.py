"""Codex: read lane rules first, emitting full text unless a known digest matches.

Pass --known-sha256 only when these exact rules are already in the active
conversation context. Omit it after context loss. Every byte change emits the
complete current instructions; no persisted acknowledgement or source edit.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

RULES = Path('C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/'
             'c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/lane-rules.md')


def main():
    # Rule updates may contain Unicode under a Windows CP1252 parent.
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--known-sha256')
    args = parser.parse_args()
    body = RULES.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    unchanged = args.known_sha256 == digest
    print(json.dumps({'author': 'Codex', 'rules_sha256': digest,
                      'bytes_read': len(body), 'matches_active_context': unchanged}))
    if not unchanged:
        # Windows stdout supplies CRLF; avoid adding a second CR to raw CRLF.
        print(body.decode('utf-8').replace('\r\n', '\n'))


if __name__ == '__main__':
    main()
