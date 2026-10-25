"""Shared admission policy for direct and generated bb2 commands."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import resource
import ctypes

# The single host cache policy. All launchers and compiler clients import it;
# a client environment cannot reconfigure an already-running server.
SCCACHE_CACHE_SIZE = '50G'
SCCACHE_CACHE_BYTES = 53687091200
SCCACHE_DIRECTORY = '/mnt/bb/sccache'
CACHE_ROOT = Path('/run/buildbox-cache')
CACHE_ENDPOINT = CACHE_ROOT / 'server' / 'sccache.sock'
CACHE_RECOVERY = ['sudo', '-n', '/usr/bin/python3', '-B', '/home/var/bin/bb-cache.py', 'recover']


def process_identity(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        if fields[0] == 'Z': return None
        return Path('/proc/sys/kernel/random/boot_id').read_text().strip()+':'+fields[19]
    except (OSError, IndexError): return None


class CachePolicyError(RuntimeError):
    def __init__(self, actual='unknown', endpoint=None, detail=''):
        super().__init__('kind=CACHE_POLICY step=sccache-server-policy '
                         f'expected_bytes={SCCACHE_CACHE_BYTES} actual={shlex.quote(str(actual))} '
                         f'endpoint={shlex.quote(str(endpoint or CACHE_ENDPOINT))} owner=gate-speed '
                         f'recovery={shlex.quote(shlex.join(CACHE_RECOVERY))} detail={shlex.quote(detail)}')


def cache_environment(env, endpoint=CACHE_ENDPOINT):
    expected = {'SCCACHE_CACHE_SIZE': SCCACHE_CACHE_SIZE, 'SCCACHE_DIR': SCCACHE_DIRECTORY,
                'SCCACHE_SERVER_UDS': str(endpoint), 'SCCACHE_IDLE_TIMEOUT': '0'}
    for key, value in expected.items():
        if key in env and env[key] != value:
            raise CachePolicyError(env[key], env.get('SCCACHE_SERVER_UDS'), 'caller disagreement: '+key)
    if 'SCCACHE_SERVER_PORT' in env:
        raise CachePolicyError(env['SCCACHE_SERVER_PORT'], detail='TCP endpoint override')
    return {**env, **expected}


def cache_authority():
    import importlib.util
    spec = importlib.util.spec_from_file_location('bb_cache_authority', Path(__file__).with_name('bb-cache.py'))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


def disk_free():
    readings = []
    for path in ['/', '/mnt/bb']:
        space = os.statvfs(path)
        readings.append({'path': path, 'free_percent': 100*space.f_bavail/max(1,space.f_blocks),
                         'free_bytes': space.f_bavail*space.f_frsize})
    return readings


def disk_low(readings):
    return any(row['free_percent'] < 10 for row in readings)


def disk_pressure(readings):
    """Bounded existing accounting, never a recursive scan on admission."""
    if not any(row['free_percent'] < 15 for row in readings): return None
    import re
    source = Path('/mnt/bb/log/free-space-quota.last')
    report = {'disk_free': readings, 'accounting_source': str(source), 'largest_reported': []}
    try:
        with source.open('rb') as stream:
            stream.seek(max(0, source.stat().st_size - 65536))
            text = stream.read(65536).decode('utf-8', errors='replace')
        report['accounting_mtime_ns'] = source.stat().st_mtime_ns
        records = {}
        for path, size in re.findall(r'(/mnt/bb/targets/\S+) (\d+) (?:->|GB)', text):
            records[path] = int(size)
        report['largest_reported'] = sorted(
            [{'path': path, 'reported_gb': size} for path, size in records.items()],
            key=lambda row: row['reported_gb'], reverse=True)[:10]
        report['note'] = 'retained quota observations, not current physical usage'
    except OSError as error: report['accounting_unavailable'] = str(error)
    return 'BB_DISK_PRESSURE '+json.dumps(report)+'\n'

LIMIT_ENV = 'BB_CALLER_RLIMITS_JSON'
RLIM_MAX = (1 << (8 * ctypes.sizeof(ctypes.c_ulong))) - 1
# Preserve every resource exposed by this kernel/Python, including the five
# environment-parity limits. Collapse aliases such as OFILE/NOFILE by number.
LIMIT_NAMES = {number: name for name, number in sorted(vars(resource).items(), reverse=True)
               if name.startswith('RLIMIT_') and name != 'RLIMIT_OFILE'}


def capture_limits():
    return {name: list(resource.getrlimit(number)) for number, name in LIMIT_NAMES.items()}


def parse_limits(raw):
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or value.keys() != capture_limits().keys(): raise ValueError('resource set')
        for pair in value.values():
            if not isinstance(pair, list) or len(pair) != 2: raise ValueError('pair')
            if any(type(n) is not int or n > RLIM_MAX or (n < 0 and n != resource.RLIM_INFINITY) for n in pair): raise ValueError('value')
            soft, hard = pair
            if hard != resource.RLIM_INFINITY and (soft == resource.RLIM_INFINITY or soft > hard): raise ValueError('order')
        return value
    except (ValueError, TypeError) as error:
        raise RuntimeError('RLIMIT_INVALID admission-INFRA: '+str(error)) from error


def restore_limits(raw):
    requested = parse_limits(raw)
    try:
        for number, name in LIMIT_NAMES.items():
            resource.setrlimit(number, requested[name])
        if capture_limits() != requested: raise ValueError('resource limits changed')
    except (OSError, ValueError, OverflowError) as error:
        raise RuntimeError('RLIMIT_RESTORE admission-INFRA: '+str(error)) from error
    return requested

QUERIES = frozenset({'metadata', 'tree', 'locate-project', 'pkgid', 'verify-project', 'read-manifest'})


def cargo_query(argv):
    # Recognize Cargo's own subcommands only. Unknown options, wrappers and
    # aliases cannot turn a compile into an unaccounted query.
    if not argv or Path(argv[0]).name != 'cargo': return False
    rest = argv[1:]
    if rest and rest[0].startswith('+'): rest = rest[1:]
    while rest and rest[0] in ('--locked', '--offline', '--frozen', '--verbose', '-v', '--quiet', '-q'):
        rest = rest[1:]
    return bool(rest and rest[0] in QUERIES)


def canonical_argv(argv, env):
    # Generated wrappers are dispatched with their already-armed metadata.
    # No shell parsing or tenant-specific bypass is maintained here.
    if env.get('BB_ADMIT_WRAPPED_COMMAND') == '1':
        meta = env.get('BUILD_SLOT_RECEIPT_META')
        if meta:
            data = json.loads(Path(meta).read_text())
            command = data.get('argv')
            if isinstance(command, list) and all(isinstance(x, str) for x in command): return command
    return argv


def resolve_wrapper(env, compiler=False, cwd=None):
    cwd = Path(cwd or os.getcwd()).resolve()
    search = os.pathsep.join(str((cwd/entry).resolve()) if not os.path.isabs(entry) else entry
                             for entry in env.get('PATH', os.defpath).split(os.pathsep))
    def executable_path(command):
        candidate = str(cwd/command) if not os.path.isabs(command) and os.path.dirname(command) else command
        return shutil.which(candidate, path=search)
    wrapper = env.get('RUSTC_WRAPPER', '/home/var/bin/sccache-rustc' if compiler else '')
    if not wrapper or env.get('BB_CACHE_DISABLED') == '1': return None
    executable = executable_path(wrapper)
    if not executable: raise RuntimeError('RUSTC_WRAPPER_MISSING admission-INFRA: ' + wrapper)
    executable = str(Path(executable).resolve(strict=True))
    data = Path(executable).read_bytes()
    if data.startswith(b'#!'):
        try: fields = shlex.split(data.splitlines()[0][2:].decode())
        except ValueError as error:
            raise RuntimeError('RUSTC_WRAPPER_SHEBANG_UNSUPPORTED admission-INFRA') from error
        interpreter = fields[0] if fields else ''
        if not os.path.isabs(interpreter) or not os.access(interpreter, os.X_OK):
            raise RuntimeError('RUSTC_WRAPPER_INTERPRETER_MISSING admission-INFRA: ' + interpreter)
        if Path(interpreter).name == 'env':
            # Support only the portable one-program form. Refuse flags, env
            # assignments and -S split strings before any build executes.
            if len(fields) != 2 or fields[1].startswith('-') or '=' in fields[1]:
                raise RuntimeError('RUSTC_WRAPPER_SHEBANG_UNSUPPORTED admission-INFRA')
            selected = executable_path(fields[1])
            if not selected: raise RuntimeError('RUSTC_WRAPPER_INTERPRETER_MISSING admission-INFRA: ' + fields[1])
    env['RUSTC_WRAPPER'] = executable
    return {'path': executable, 'sha256': hashlib.sha256(data).hexdigest()}


if __name__ == '__main__':
    import sys
    try:
        if sys.argv[1:] == ['capture-limits']:
            print(json.dumps(capture_limits(), separators=(',', ':')))
        elif sys.argv[1:3] == ['exec-user', 'var'] and sys.argv[3:4] == ['--'] and len(sys.argv) > 4:
            import pwd
            # Do this after systemd/sudo and before setuid. runuser's PAM
            # session can reset the limits again, so do not cross it afterward.
            restore_limits(os.environ[LIMIT_ENV])
            user = pwd.getpwnam('var')
            os.setgroups(os.getgrouplist(user.pw_name, user.pw_gid))
            os.setgid(user.pw_gid); os.setuid(user.pw_uid)
            # Match runuser's identity environment after replacing its PAM
            # transition; do not leave sudo's root USER/LOGNAME in the job.
            os.environ.update(USER=user.pw_name, LOGNAME=user.pw_name,
                              HOME=user.pw_dir, SHELL=user.pw_shell)
            os.execvpe(sys.argv[4], sys.argv[4:], os.environ)
        else: raise RuntimeError('invalid admission policy command')
    except (OSError, RuntimeError, KeyError) as error:
        print('[buildbox] '+str(error), file=sys.stderr)
        raise SystemExit(224)
