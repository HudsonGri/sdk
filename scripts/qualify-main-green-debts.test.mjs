import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { cargoArguments, verify } from './qualify-main-green-debts.mjs';

const entry = { package: 'acyclic-harness', target: 'lib', regression: 'module::regression', controls: ['module::negative'] };
const success = 'test module::regression ... ok\ntest module::negative - should panic ... ok\n\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.12s\n';

test('the checked-in plan names each debt once with its class and structural fix', async () => {
  const entries = JSON.parse(await readFile(new URL('./main-green-debts.json', import.meta.url), 'utf8'));
  assert.ok(entries.length > 0);
  assert.equal(new Set(entries.map(item => item.regression)).size, entries.length);
  for (const item of entries) {
    assert.ok(item.class.length > 0);
    assert.match(item.fixed_by, /^[0-9a-f]{9}(,[0-9a-f]{9})*$/);
    cargoArguments(item);
  }
});

test('regression and negative control are exact named tests in one package', () => {
  assert.deepEqual(cargoArguments(entry), ['test', '--locked', '-p', 'acyclic-harness', '--all-features', '--lib', '--', 'module::regression', 'module::negative', '--exact', '--test-threads=4', '--color=never']);
  assert.equal(verify(entry, 0, success).seconds, 0.12);
  assert.equal(verify(entry, 0, success.replaceAll('\n', '\r\n')).tests.length, 2);
});

test('integrated negative controls execute their property once', () => {
  const integrated = { ...entry, controls: [entry.regression] };
  const output = 'test module::regression ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.01s\n';
  assert.deepEqual(verify(integrated, 0, output).tests, [entry.regression]);
});

test('zero-test success is the negative control for exit-code-only qualification', () => {
  const output = 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 43 filtered out; finished in 0.00s\n';
  const exitCodeOnly = status => status === 0;
  assert.equal(exitCodeOnly(0), true);
  assert.throws(() => verify(entry, 0, output), /expected every regression/);
});

test('missing, substituted, duplicate and ignored controls cannot qualify', () => {
  for (const output of [success.replace('module::negative', 'module::unrelated'),
    success.replace('module::negative', 'module::regression'),
    success.replace('module::negative - should panic ... ok', 'module::negative ... ignored'),
    success.replace('0 ignored', '1 ignored'), success.replace('2 passed', '1 passed')]) {
    assert.throws(() => verify(entry, 0, output));
  }
});

test('process failures and incomplete or over-budget summaries cannot qualify', () => {
  for (const status of [1, 101, null]) assert.throws(() => verify(entry, status, success));
  for (const output of [success.replace('0.12s', '10.00s'), success.replace('0 failed', '1 failed'),
    success.replace('0 measured', '1 measured'), success.replace('test result:', 'incomplete result:'), success + success]) {
    assert.throws(() => verify(entry, 0, output));
  }
});

test('a debt requires a regression, a negative control and literal test names', () => {
  for (const invalid of [{ ...entry, controls: [] }, { ...entry, regression: '' },
    { ...entry, controls: ['--skip'] }, { ...entry, package: '--workspace' }, { ...entry, target: '--all-targets' },
    { ...entry, target: undefined }, { ...entry, controls: 'module::negative' }]) {
    assert.throws(() => cargoArguments(invalid));
  }
});
