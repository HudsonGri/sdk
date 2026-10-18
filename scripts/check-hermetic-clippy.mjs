import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifest = 'rust/crates/harness/tests/ui/native-environment/Cargo.toml';
for (const negative of [false, true]) {
  const start = performance.now();
  const result = spawnSync('cargo', ['clippy', '--locked', '--manifest-path', manifest,
    '--lib', '--message-format=json', ...(negative ? ['--features', 'negative'] : []),
    '--', '-D', 'clippy::disallowed_methods'], {cwd: root, encoding: 'utf8', maxBuffer: 1 << 23});
  if (result.error) throw result.error;
  const diagnostics = result.stdout.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
    .filter(record => record.reason === 'compiler-message').map(record => record.message);
  const errors = diagnostics.filter(message => message.level === 'error');
  if (!negative) assert.equal(result.status, 0, result.stderr + errors.map(error => error.rendered).join(''));
  else {
    assert.notEqual(result.status, 0, 'negative control unexpectedly compiled');
    assert.equal(errors.length, 8, result.stderr + errors.map(error => error.rendered).join(''));
    assert.ok(errors.every(error => error.code?.code === 'clippy::disallowed_methods'), 'controls failed for an unrelated compiler error');
    assert.equal(new Set(errors.flatMap(error => error.spans.filter(span => span.is_primary).map(span => span.line_start))).size, 8);
  }
  console.log(`${negative ? 'negative (8 compiler-resolved rejections)' : 'positive (real seam)'}: ${(performance.now() - start).toFixed(0)} ms`);
}
