"""Fixture-directory adapter over the sole native process-tree host.

The native host owns spawn, EOF cancellation, descendants and exit barriers on
all operating systems. This adapter only retains the Linux FUSE mount lease
until that shared owner has physically drained its services.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--mount-root")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not Path(args.host).is_absolute() or not args.command or args.command[0] != "--":
        parser.error("an absolute native host and -- COMMAND are required")
    command = args.command[1:]
    if not command:
        parser.error("missing command")
    lease = None
    if args.mount_root:
        if sys.platform != "linux":
            parser.error("mount leases require Linux")
        from owned_mounts import MountLease
        lease = MountLease(args.mount_root)
    # Each command owns an exclusive directory. Retain it on failed fencing;
    # deleting an unresolved custody record would discard the recovery owner.
    root = Path(tempfile.mkdtemp(prefix="acyclic-process-"))
    try:
        result = subprocess.run([args.host, "--run", str(root), *command])
        subprocess.run([args.host, "--fence", str(root / "trees")], check=True,
                       stdin=subprocess.DEVNULL)
        if lease is not None:
            lease.retire()
        import shutil
        shutil.rmtree(root)
        return result.returncode if result.returncode >= 0 else 128 - result.returncode
    finally:
        if lease is not None:
            lease.close()


if __name__ == "__main__":
    sys.exit(main())
