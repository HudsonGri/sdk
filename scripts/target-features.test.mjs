import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {check, evaluate, parseCfg, strip, useTree} from './target-features.mjs';

// A two-crate workspace laid out on disk with the metadata Cargo would report.
function workspace(t, {targetSource, required, extraFiles = {}, devFeatures = [], kind = 'test', dependencies = [], features = {}}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'target-features-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const files = {
    'core/src/lib.rs': [
      '//! Fixture library.',
      'pub mod client;',
      '#[cfg(feature = "grpc")]',
      'pub mod remote;',
      '#[cfg(feature = "test-support")]',
      'pub mod test_support { pub fn helper() {} }',
    ].join('\n'),
    'core/src/client/mod.rs': [
      'mod swarm;',
      '#[cfg(any(all(feature = "grpc", not(target_arch = "wasm32")), all(feature = "wasm", target_arch = "wasm32")))]',
      'pub use swarm::SwarmClient;',
      'pub use swarm::{SwarmLimits as Limits, SwarmUpdate};',
      'pub struct Plain;',
    ].join('\n'),
    'core/src/client/swarm.rs': 'pub struct SwarmClient; pub struct SwarmLimits; pub struct SwarmUpdate;',
    'core/src/remote.rs': 'pub fn connect() {}',
    'core/tests/target.rs': targetSource,
    ...extraFiles,
  };
  for (const [name, text] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(root, name)), {recursive: true});
    fs.writeFileSync(path.join(root, name), text);
  }
  const core = {
    name: 'core',
    id: 'core 0.1.0',
    features: {default: [], grpc: [], wasm: [], 'test-support': [], ...features},
    dependencies: [
      ...(devFeatures.length ? [{name: 'core', rename: null, kind: 'dev', optional: false, uses_default_features: true, features: devFeatures}] : []),
      ...dependencies.map(dep => ({rename: null, kind: null, optional: false, uses_default_features: true, features: [], target: null, ...dep})),
    ],
    targets: [
      {name: 'core', kind: ['lib'], src_path: path.join(root, 'core/src/lib.rs')},
      {name: 'target', kind: [kind], src_path: path.join(root, 'core/tests/target.rs'), ...(required ? {'required-features': required} : {})},
    ],
  };
  return {packages: [core], workspace_members: [core.id]};
}

test('an undeclared feature-gated re-export is reported with its gating features', t => {
  const findings = check(workspace(t, {targetSource: 'use core::client::{Plain, SwarmClient};\nfn main() {}'}));
  assert.deepEqual(findings.map(f => [f.target, f.segment, f.features]), [['target', 'core::client::SwarmClient', ['grpc', 'wasm']]]);
});

test('declaring the feature in required-features passes', t => {
  assert.deepEqual(check(workspace(t, {targetSource: 'use core::client::SwarmClient;', required: ['grpc']})), []);
});

test('a gated module reached through an inline path or a nested mod file is reported', t => {
  const inline = check(workspace(t, {targetSource: 'fn main() { core::remote::connect(); }'}));
  assert.deepEqual(inline.map(f => f.segment), ['core::remote']);
  const nested = check(workspace(t, {
    targetSource: '#[path = "../shared/helper.rs"]\nmod helper;',
    extraFiles: {'core/shared/helper.rs': 'use core::{client::Limits, remote::connect};'},
  }));
  assert.deepEqual(nested.map(f => f.segment), ['core::remote']);
});

test('the target\'s own cfg regions and crate-level cfg exclude references', t => {
  assert.deepEqual(check(workspace(t, {targetSource: '#[cfg(feature = "grpc")]\nmod remote_tests { use core::remote::connect; }\n#[cfg(feature = "grpc")]\nfn f() { core::client::SwarmClient; }'})), []);
  assert.deepEqual(check(workspace(t, {targetSource: '#![cfg(feature = "grpc")]\nuse core::client::SwarmClient;'})), []);
  // Unknown predicates stay quiet rather than guessing.
  assert.deepEqual(check(workspace(t, {targetSource: '#[cfg(loom)]\nuse core::remote::connect;'})), []);
});

test('dev-dependency features on the package itself are part of the closure', t => {
  const target = 'use core::test_support::helper;';
  assert.equal(check(workspace(t, {targetSource: target})).length, 1);
  assert.deepEqual(check(workspace(t, {targetSource: target, devFeatures: ['test-support']})), []);
});

test('ungated items, aliases and literals never produce findings', t => {
  const source = 'use core::client::{Limits, SwarmUpdate, Plain};\nconst S: &str = "core::remote::connect";\n// core::remote::connect\nfn main() {}';
  assert.deepEqual(check(workspace(t, {targetSource: source})), []);
});

test('cfg evaluation is three-valued for a native Linux host', () => {
  const on = new Set(['grpc']);
  assert.equal(evaluate(parseCfg('all(feature = "grpc", not(target_arch = "wasm32"))'), on), true);
  assert.equal(evaluate(parseCfg('any(feature = "wasm", windows)'), on), false);
  assert.equal(evaluate(parseCfg('all(feature = "grpc", loom)'), on), undefined);
  assert.equal(evaluate(parseCfg('any(feature = "grpc", loom)'), on), true);
});

test('stripping preserves offsets and use trees expand nested groups', () => {
  const source = 'let a = "x::y"; // c\nlet b = r#"q"#; let c = \'"\'; let d: &\'static str;';
  assert.equal(strip(source).length, source.length);
  assert.doesNotMatch(strip(source), /x::y|q|"\s*'|\/\/ c/);
  assert.deepEqual(useTree('a::{b::{C, D as E}, self as f, g::*}').map(e => [e.path.join('::'), e.alias, Boolean(e.glob)]),
    [['a::b::C', 'C', false], ['a::b::D', 'E', false], ['a', 'f', false], ['a::g', undefined, true]]);
});

test('an optional dependency outside the closure is reported with the features that enable it', t => {
  const options = {
    targetSource: 'fn main() { let _ = tempfile::tempdir(); }',
    kind: 'bench',
    features: {'local-runner': ['dep:tempfile']},
    dependencies: [{name: 'tempfile', optional: true}],
  };
  assert.deepEqual(check(workspace(t, options)).map(f => [f.kind, f.segment, f.features]), [['bench', 'tempfile', ['local-runner']]]);
  assert.deepEqual(check(workspace(t, {...options, required: ['local-runner']})), []);
  // A non-optional dev-dependency of the same crate makes it available to tests and benches.
  assert.deepEqual(check(workspace(t, {...options, dependencies: [...options.dependencies, {name: 'tempfile', kind: 'dev'}]})), []);
  // Dependencies scoped to another platform never count as missing on the host.
  assert.deepEqual(check(workspace(t, {...options, dependencies: [{name: 'tempfile', optional: true, target: 'cfg(target_arch = "wasm32")'}]})), []);
});

test('bin targets do not see dev-dependency features', t => {
  const target = 'use core::test_support::helper;\nfn main() {}';
  assert.deepEqual(check(workspace(t, {targetSource: target, devFeatures: ['test-support']})), []);
  assert.deepEqual(check(workspace(t, {targetSource: target, devFeatures: ['test-support'], kind: 'bin'})).map(f => f.segment), ['core::test_support']);
});
