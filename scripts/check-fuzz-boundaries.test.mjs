import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import test from 'node:test';
import { violations } from './check-fuzz-boundaries.mjs';

const owner = resolve('fixture/harness-fuzz');
const target = resolve(owner, 'fuzz_targets/provider_scenario.rs');
test('rejects foreign production modules and test fixtures in every fuzz entry point', () => {
  for (const source of [
    '#[path = "../../harness/src/local/owner.rs"] mod owner;',
    '#[path = "../../harness/tests/e2e/provider_case.rs"] mod provider_case;',
    'include!("../../harness/src/sync/arc.rs");',
    'include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../src/codec.rs"));',
  ]) {
    for (const filename of [target, resolve(owner, 'benches/page.rs'), resolve(owner, 'support/media.rs')]) {
      assert.equal(violations(source, filename, owner).length, 1);
    }
  }
});
test('harness-fuzz rejects local path attributes as well', () => {
  assert.equal(violations('#[path = "input.rs"] mod input;', target, owner).length, 1);
});
test('public APIs and ordinary local modules pass', () => {
  assert.deepEqual(violations('use acyclic_harness::test_support; mod input;', target, owner), []);
});
test('other fuzz packages may split local modules but cannot import foreign implementations', () => {
  const packageRoot = resolve('fixture/filesystem/fuzz');
  const filename = resolve(packageRoot, 'fuzz_targets/reply.rs');
  assert.deepEqual(violations('#[path = "reply/linux.rs"] mod linux;', filename, packageRoot), []);
  assert.equal(violations('#[path = "../../src/reply.rs"] mod reply;', filename, packageRoot).length, 1);
});
