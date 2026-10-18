import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {models, rules, violations} from './check-loom-models.mjs';

test('repository model registration and source policies are clean', () => {
  const files = execFileSync('git', ['ls-files', '-z', '--', '*.rs'], {encoding: 'utf8'}).split('\0').filter(Boolean);
  const sources = new Map(files.map(file => [file, readFileSync(file, 'utf8').replaceAll('\r\n', '\n')]));
  const {locks: _compilerOwned, ...sourceRules} = rules;
  assert.deepEqual(violations(sources, models, sourceRules), []);
  assert.ok(Object.keys(models).length > 0);
  for (const model of Object.values(models)) assert.notEqual(model.grade, 'substitute');
});
