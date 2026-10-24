import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const coordinator = dirname(fileURLToPath(import.meta.url));
const scripts = resolve(coordinator, '../..');
const checker = readFileSync(join(coordinator, 'gate-checks.sh'), 'utf8');
const pr = readFileSync(join(scripts, 'pr-gate-checks.sh'), 'utf8');
function fixture(run) {
  const cwd = mkdtempSync(join(tmpdir(), 'gate-public-windows-'));
  const put = (name, text) => {
    const path = join(cwd, name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, text);
    chmodSync(path, 0o700);
  };
  try {
    put('scripts/buildbox/coordinator/gate-checks.sh', checker);
    put('scripts/pr-gate-checks.sh', pr);
    put('scripts/gate-test-runner.sh', readFileSync(join(scripts, 'gate-test-runner.sh')));
    put('scripts/main-gate-checks.sh', `#!/bin/bash
echo main >> phases
echo "GATE_WINDOWS_FEATURES rc=$FEATURE_RC"
if [ "$COMPLETION" != missing ]; then echo "GATE_DONE rc=$MAIN_RC"; fi
if [ "$COMPLETION" = duplicate ]; then echo "GATE_DONE rc=$MAIN_RC"; fi
exit "$MAIN_RC"
`);
    put('scripts/gate-windows-features.sh', '#!/bin/bash\necho features >> phases\necho "GATE_WINDOWS_FEATURES rc=$FEATURE_RC"\nexit "$FEATURE_RC"\n');
    put('bin/cargo', `#!/bin/bash
if [ "$1" = clippy ] && [[ " $* " == *" --target "* ]]; then
 python3 -c 'import json,os,sys; print(json.dumps({"argv":sys.argv[1:],"flags":os.getenv("RUSTFLAGS"),"encoded":os.getenv("CARGO_ENCODED_RUSTFLAGS"),"target":os.getenv("CARGO_TARGET_DIR")}))' "$@" >> strict.jsonl
 echo 'error: retained strict runtime diagnostic' >&2
 exit "$STRICT_RC"
fi
case "$1" in
 metadata) echo '{"workspace_members":["native"],"packages":[{"id":"native","name":"acyclic-native-runtime","targets":[]}]}' ;;
 clean|test|clippy) exit 0 ;;
 *) exit 9 ;;
esac
`);
    put('.gate-scope-mode', 'affected\n');
    put('.gate-tests-packages', 'acyclic-native-runtime\n');
    put('.gate-clippy-packages', 'acyclic-native-runtime\n');
    const env = { ...process.env, PATH: join(cwd, 'bin') + ':' + process.env.PATH,
      TMPDIR: cwd, CARGO_TARGET_DIR: join(cwd, 'target'),
      GATE_WINDOWS_RUNTIME_TARGET_DIR: '', GATE_WINDOWS_RUNTIME_OUTPUT_DIR: join(cwd, 'output'),
      RUSTFLAGS: '--cap-lints warn', CARGO_ENCODED_RUSTFLAGS: '--cap-lints\x1fwarn',
      STRICT_RC: '0', MAIN_RC: '0', FEATURE_RC: '0', COMPLETION: 'normal' };
    const invoke = (adapter, extra = {}) => {
      rmSync(join(cwd, 'strict.jsonl'), { force: true });
      rmSync(join(cwd, 'phases'), { force: true });
      rmSync(join(cwd, 'output/.gate-windows-runtime.out'), { force: true });
      rmSync(join(cwd, '.gate-windows-runtime.out'), { force: true });
      const args = adapter === 'direct' ? ['scripts/buildbox/coordinator/gate-checks.sh', '--windows-runtime-only'] :
        [adapter === 'pr' ? 'scripts/pr-gate-checks.sh' : 'scripts/buildbox/coordinator/gate-checks.sh'];
      const result = spawnSync('bash', args, { cwd, encoding: 'utf8', env: { ...env, ...extra } });
      const calls = readFile(join(cwd, 'strict.jsonl')).trim().split('\n').filter(Boolean).map(JSON.parse);
      return { ...result, calls, phases: readFile(join(cwd, 'phases')).trim().split('\n').filter(Boolean) };
    };
    run({ cwd, put, invoke });
  } finally { rmSync(cwd, { recursive: true, force: true }); }
}
function readFile(path) {
  try { return readFileSync(path, 'utf8'); } catch (error) { if (error.code === 'ENOENT') return ''; throw error; }
}
function strict(result, status) {
  assert.equal(result.status, status, result.stdout + result.stderr);
  assert.equal(result.calls.length, 1, 'exactly one canonical strict invocation');
  assert.deepEqual(result.calls[0].argv, ['clippy', '--locked', '--target', 'x86_64-pc-windows-gnu',
    '-p', 'acyclic-native-runtime', '--all-targets', '--all-features', '--message-format=short']);
  assert.equal(result.calls[0].flags, '');
  assert.equal(result.calls[0].encoded, '');
  assert.match(result.calls[0].target, /\/target\/.*gate-windows-runtime$/);
  assert.match(result.stdout, /error: retained strict runtime diagnostic/);
  assert.match(result.stdout, new RegExp('GATE_WINDOWS_RUNTIME_CLIPPY rc=' + status));
}

test('direct, PR and main adapters execute the same strict policy and preserve failure', () => fixture(({ cwd, invoke }) => {
  for (const adapter of ['direct', 'pr', 'main']) for (const rc of [0, 101]) {
    const result = invoke(adapter, { STRICT_RC: String(rc) });
    strict(result, rc);
    if (adapter !== 'direct') {
      assert.deepEqual(result.stdout.match(/^GATE_DONE rc=\d+$/gm), ['GATE_DONE rc=' + rc]);
      assert.deepEqual(result.phases, [adapter === 'pr' ? 'features' : 'main']);
    }
    assert.match(readFile(join(cwd, adapter === 'pr' ? '.gate-windows-runtime.out' : 'output/.gate-windows-runtime.out')), /retained strict runtime diagnostic/);
  }
}));

test('one policy mutation is rejected through every host adapter', () => fixture(({ put, invoke }) => {
  for (const [from, to] of [
    ['x86_64-pc-windows-gnu', 'x86_64-pc-windows-msvc'],
    ['-p acyclic-native-runtime', '-p wrong-package'],
    ['--all-targets --all-features', '--lib --features test-support'],
    ['RUSTFLAGS= CARGO_ENCODED_RUSTFLAGS=', ''],
    ['rc=${status[0]}', 'rc=0'],
  ]) {
    assert.ok(checker.includes(from));
    put('scripts/buildbox/coordinator/gate-checks.sh', checker.replaceAll(from, to));
    for (const adapter of ['direct', 'pr', 'main']) {
      const result = invoke(adapter, { STRICT_RC: '101' });
      assert.equal(result.calls.length, 1, 'negative control must reach the shared policy');
      assert.throws(() => strict(result, 101), assert.AssertionError);
    }
  }
}));

test('missing shared policy refuses PR dispatch instead of falling back to a second implementation', () => fixture(({ cwd, invoke }) => {
  rmSync(join(cwd, 'scripts/buildbox/coordinator/gate-checks.sh'));
  const result = invoke('pr');
  assert.notEqual(result.status, 0);
  assert.equal(result.calls.length, 0);
  assert.doesNotMatch(result.stdout, /^GATE_DONE rc=0$/m);
}));

test('main requires its reviewed runner, exact completion and its owned feature result without retry', () => fixture(({ cwd, put, invoke }) => {
  for (const [overrides, expected] of [
    [{ MAIN_RC: '101' }, 101], [{ FEATURE_RC: '101' }, 2],
    [{ COMPLETION: 'missing' }, 2], [{ COMPLETION: 'duplicate' }, 2],
  ]) {
    const result = invoke('main', overrides);
    assert.equal(result.status, expected, result.stdout + result.stderr);
    assert.deepEqual(result.phases, ['main']);
    assert.deepEqual(result.stdout.match(/^GATE_DONE rc=\d+$/gm), ['GATE_DONE rc=' + expected]);
  }
  put('scripts/main-gate-checks.sh', '#!/bin/bash\necho "GATE_WINDOWS_FEATURES rc=0"\necho "GATE_DONE rc=0"\nexit 101\n');
  const mismatch = invoke('main');
  assert.equal(mismatch.status, 2);
  assert.match(mismatch.stdout, /completion does not match/);
  rmSync(join(cwd, 'scripts/main-gate-checks.sh'));
  const missing = invoke('main');
  assert.equal(missing.status, 224);
  assert.equal(missing.calls.length, 0);
  assert.match(missing.stdout, /preparation-INFRA: missing reviewed/);
}));

test('retention failures remain nonzero rather than passing clippy through a broken pipe', () => fixture(({ put, invoke }) => {
  put('bin/tee', '#!/bin/bash\ncat\nexit 23\n');
  const result = invoke('direct');
  assert.equal(result.status, 2);
  assert.match(result.stdout, /could not retain Windows runtime clippy output \(tee rc=23\)/);
  // The predecessor shape guessed success from Cargo alone.
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('if [ "${status[1]}" != 0 ]; then', 'if false; then'));
  assert.equal(invoke('direct').status, 0);
}));

test('completion and missing-runner negative controls expose fabricated main success', () => fixture(({ cwd, put, invoke }) => {
  const guard = 'if [ "${#completed[@]}" != 1 ] || [ "${completed[0]:-}" != "GATE_DONE rc=$main_rc" ]; then';
  assert.ok(checker.includes(guard));
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace(guard, 'if false; then'));
  assert.equal(invoke('main', { COMPLETION: 'missing' }).status, 0);
  assert.equal(invoke('main', { COMPLETION: 'duplicate' }).status, 0);
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('exit 224', 'exit 0'));
  rmSync(join(cwd, 'scripts/main-gate-checks.sh'));
  assert.equal(invoke('main').status, 0);
}));
