import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {models, rules, violations} from './check-loom-models.mjs';

const path = 'rust/crates/example/src/model.rs';
// A healthy real model: it drives a production component and asserts that
// the crate shim is the loom lock, which is not a model-owned lock.
const healthy = [
  'const _: fn(crate::sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;',
  'fn check() { loom::model(|| production::Component::new().race()); }',
].join('\n');
const bounded = 'let mut builder = loom::model::Builder::new();\nbuilder.preemption_bound = Some(2);\n';

// Each mutant must be reported by exactly its named rule: with every rule the
// gate reports it, and with only that rule deleted the same fixture is clean.
const mutants = [
  ['unregistered', {[path]: healthy}, {}],
  ['grade', {[path]: healthy}, {[path]: {grade: 'substitute'}}],
  ['scope', {[path]: healthy}, {[path]: {grade: 'abstract', scope: ' '}}],
  // The deleted local admission model: provider head state behind a loom lock.
  ['locks', {[path]: 'use loom::{sync::Mutex as DataMutex, thread};\n' + healthy}, {[path]: {grade: 'real'}}],
  // The deleted refresh model: a shim lock owning a hand-advanced generation.
  ['locks', {[path]: healthy + '\nlet index = Arc::new(Mutex::new(Generation { position: 1 }));'}, {[path]: {grade: 'real'}}],
  ['locks', {[path]: healthy}, {[path]: {grade: 'real', constructs: 'production lock'}}],
  ['bound', {[path]: bounded + healthy}, {[path]: {grade: 'real'}}],
  ['bound', {[path]: healthy}, {[path]: {grade: 'real', bound: 'two preemptions'}}],
  ['exhaustive', {[path]: '// Exhaustive schedules of the race.\n' + bounded + healthy}, {[path]: {grade: 'real', bound: 'two preemptions'}}],
  ['missing', {}, {[path]: {grade: 'real'}}],
];

test('every repository Loom model is classified and clean', () => {
  const files = execFileSync('git', ['ls-files', '-z', '--', '*.rs'], {encoding: 'utf8'}).split('\0').filter(Boolean);
  const sources = new Map(files.map(file => [file, readFileSync(file, 'utf8').replaceAll('\r\n', '\n')]));
  assert.deepEqual(violations(sources), []);
  assert.ok(Object.keys(models).length > 0);
  for (const model of Object.values(models)) assert.notEqual(model.grade, 'substitute');
});

test('the healthy fixture passes every rule', () => {
  assert.deepEqual(violations(new Map([[path, healthy]]), {[path]: {grade: 'real'}}), []);
  assert.deepEqual(violations(new Map([[path, bounded + healthy]]), {[path]: {grade: 'real', bound: 'two preemptions'}}), []);
});

for (const [index, [rule, sources, registry]] of mutants.entries()) {
  test(`mutant ${index} is reported by rule ${rule} alone`, () => {
    const files = new Map(Object.entries(sources));
    const found = violations(files, registry);
    assert.ok(found.length > 0, `${rule} mutant passed the gate`);
    assert.deepEqual(found, violations(files, registry, {[rule]: rules[rule]}));
    const {[rule]: _deleted, ...others} = rules;
    assert.deepEqual(violations(files, registry, others), [], `${rule} mutant failed another rule`);
  });
}

test('every rule has a mutant', () => {
  assert.deepEqual(new Set(mutants.map(([rule]) => rule)), new Set(Object.keys(rules)));
});
