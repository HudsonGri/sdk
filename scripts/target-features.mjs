// Every test, example, bench and bin target must declare the crate features
// its sources need. Cargo builds a target whenever its `required-features` are
// enabled, so an undeclared dependency on a feature-gated item compiles only
// under whichever feature set happened to be selected, and breaks the rest.
//
// This check needs no compilation. For each target it resolves the feature
// closure Cargo would build it with (the package default, its
// `required-features`, dev-dependency features for non-bin targets and their
// workspace implications), walks the target's module closure while honouring its own
// `cfg` regions, and resolves every path into a workspace library crate
// against that crate's `cfg`-gated public declarations. A reference that is
// configured out under the closure, or any use of an optional dependency the
// closure leaves disabled, is an undeclared feature dependency.
// Evaluation assumes a native x86_64 Linux host (`test`, `windows` and
// `wasm32` are false). Unknown predicates and unresolvable paths (globs,
// macros, other crates) never produce a finding.
import fs from 'node:fs';
import path from 'node:path';

const KINDS = new Set(['test', 'example', 'bench', 'bin']);

// Comments and literal contents become spaces, preserving every offset.
export function strip(source) {
  const out = source.split('');
  const blank = (from, to) => {
    for (let k = from; k < to; k++) if (out[k] !== '\n') out[k] = ' ';
  };
  let i = 0;
  while (i < source.length) {
    const c = source[i];
    const next = source[i + 1];
    const rawString = (c === 'r' || c === 'b') && !/\w/.test(source[i - 1] ?? '') ? /^b?r(#*)"/.exec(source.slice(i, i + 260)) : null;
    if (c === '/' && next === '/') {
      const end = source.indexOf('\n', i);
      const stop = end < 0 ? source.length : end;
      blank(i, stop);
      i = stop;
    } else if (c === '/' && next === '*') {
      let depth = 1;
      let k = i + 2;
      while (k < source.length && depth) {
        if (source[k] === '/' && source[k + 1] === '*') { depth++; k += 2; }
        else if (source[k] === '*' && source[k + 1] === '/') { depth--; k += 2; }
        else k++;
      }
      blank(i, k);
      i = k;
    } else if (rawString) {
      const close = '"' + rawString[1];
      const start = i + rawString[0].length;
      const end = source.indexOf(close, start);
      const stop = end < 0 ? source.length : end;
      blank(start, stop);
      i = stop + close.length;
    } else if (c === '"') {
      let k = i + 1;
      while (k < source.length && source[k] !== '"') k += source[k] === '\\' ? 2 : 1;
      blank(i + 1, k);
      i = k + 1;
    } else if (c === '\'') {
      // A character literal, or a lifetime or label when no closing quote follows.
      const literal = /^'(?:\\(?:u\{[0-9a-fA-F]{1,6}\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'/u.exec(source.slice(i, i + 12));
      if (literal) {
        blank(i + 1, i + literal[0].length - 1);
        i += literal[0].length;
      } else i++;
    } else i++;
  }
  return out.join('');
}

const CLOSE = {'(': ')', '[': ']', '{': '}'};

// Index just past the bracket that closes the one at `start`.
function matching(text, start) {
  const stack = [];
  for (let k = start; k < text.length; k++) {
    const c = text[k];
    if (CLOSE[c]) stack.push(CLOSE[c]);
    else if (c === ')' || c === ']' || c === '}') {
      if (stack.pop() !== c) return text.length;
      if (!stack.length) return k + 1;
    }
  }
  return text.length;
}

// --- cfg predicates, three-valued: true, false or undefined (unknown) ---

export function parseCfg(text) {
  let i = 0;
  const space = () => { while (/\s/.test(text[i] ?? '')) i++; };
  const fail = () => { throw new Error(`unparsable cfg: ${text}`); };
  const expr = () => {
    space();
    const name = /^[A-Za-z_]\w*/.exec(text.slice(i))?.[0] ?? fail();
    i += name.length;
    space();
    if (text[i] === '(') {
      i++;
      const args = [];
      for (space(); text[i] !== ')'; space()) {
        if (i >= text.length) fail();
        args.push(expr());
        space();
        if (text[i] === ',') i++;
      }
      i++;
      return {op: name, args};
    }
    if (text[i] === '=') {
      i++;
      space();
      const value = /^"([^"]*)"/.exec(text.slice(i)) ?? fail();
      i += value[0].length;
      return {key: name, value: value[1]};
    }
    return {flag: name};
  };
  const result = expr();
  space();
  if (i !== text.length) fail();
  return result;
}

const HOST_FLAGS = {unix: true, windows: false, test: false, debug_assertions: true, doc: false, doctest: false, miri: false};
const HOST_KEYS = {
  target_arch: 'x86_64',
  target_os: 'linux',
  target_family: 'unix',
  target_env: 'gnu',
  target_pointer_width: '64',
  target_endian: 'little',
  panic: 'unwind',
};

export function evaluate(node, features) {
  if (node.op === 'all' || node.op === 'any') {
    const values = node.args.map(arg => evaluate(arg, features));
    const decisive = node.op === 'any';
    if (values.includes(decisive)) return decisive;
    return values.includes(undefined) ? undefined : !decisive;
  }
  if (node.op === 'not') {
    const value = evaluate(node.args[0], features);
    return value === undefined ? undefined : !value;
  }
  if (node.op) return undefined;
  if (node.key === 'feature') return features.has(node.value);
  if (node.key) return node.key in HOST_KEYS ? HOST_KEYS[node.key] === node.value : undefined;
  return HOST_FLAGS[node.flag];
}

function featureNames(node, out) {
  if (node.key === 'feature') out.add(node.value);
  for (const arg of node.args ?? []) featureNames(arg, out);
  return out;
}

// --- items ---

const MODIFIERS = String.raw`(?:(?:default|const|async|unsafe|extern\s*(?:"\s*")?)\s+)*`;
const VIS = String.raw`(?:pub(?:\s*\([^)]*\))?\s+)?`;
const BLOCK_HEAD = new RegExp(String.raw`^\s*(?:${VIS}${MODIFIERS}(?:fn|struct|enum|trait|union|mod|impl)\b|macro_rules\s*!|(?:if|match|for|while|loop|unsafe|async)\b|$)|!\s*$`);

// Splits stripped `text` within [from, to) into items and statements with
// their outer attributes. A braced item ends at its closing brace; any other
// construct runs to its `;` or `,`.
export function items(text, raw, from = 0, to = text.length) {
  const result = [];
  let attributes = [];
  let i = from;
  while (i < to) {
    while (i < to && /\s/.test(text[i])) i++;
    if (i >= to) break;
    if (text.startsWith('#![', i) || text.startsWith('#[', i)) {
      const inner = text[i + 1] === '!';
      const open = i + (inner ? 2 : 1);
      const end = matching(text, open);
      const parsed = attribute(text, raw, open + 1, end - 1);
      if (inner) result.push({inner: parsed});
      else attributes.push(parsed);
      i = end;
      continue;
    }
    if (')]}'.includes(text[i])) { i++; continue; }
    const start = i;
    let k = i;
    let body;
    while (k < to) {
      const c = text[k];
      if (c === ';' || c === ',') { k++; break; }
      if (c === '(' || c === '[') { k = matching(text, k); continue; }
      if (c === ')' || c === ']' || c === '}') break;
      if (c === '{') {
        const end = matching(text, k);
        const head = text.slice(start, k);
        body = [k + 1, end - 1];
        k = end;
        const after = /^\s*[;,]/.exec(text.slice(k, k + 64));
        if (after) { k += after[0].length; break; }
        if (BLOCK_HEAD.test(head)) break;
        continue;
      }
      k++;
    }
    result.push({attributes, start, end: k, head: text.slice(start, body ? body[0] - 1 : k), body});
    attributes = [];
    i = Math.max(k, start + 1);
  }
  return result;
}

function attribute(text, raw, from, to) {
  const body = text.slice(from, to).trim();
  // Structure comes from the stripped text; literal values from the source.
  const literal = raw.slice(from, to).replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, ' ').trim();
  const cfg = /^cfg\s*\(/.test(body) ? /^cfg\s*\(([\s\S]*)\)$/.exec(literal) : null;
  return {
    cfg: cfg ? parseCfg(cfg[1].trim()) : undefined,
    path: /^path\s*=\s*"/.test(body) ? /"([^"]*)"/.exec(raw.slice(from, to))[1] : undefined,
  };
}

function enabled(attributes, features) {
  let value = true;
  for (const {cfg} of attributes) {
    if (!cfg) continue;
    const next = evaluate(cfg, features);
    if (next === false) return false;
    if (next === undefined) value = undefined;
  }
  return value;
}

// --- use trees ---

export function useTree(text) {
  let i = 0;
  const space = () => { while (/\s/.test(text[i] ?? '')) i++; };
  const tree = prefix => {
    space();
    if (text[i] === '{') {
      i++;
      const out = [];
      while (i < text.length) {
        space();
        if (text[i] === '}') { i++; break; }
        out.push(...tree(prefix));
        space();
        if (text[i] === ',') i++;
      }
      return out;
    }
    if (text[i] === '*') { i++; return [{path: prefix, glob: true}]; }
    const segment = /^(?:::\s*)?((?:r#)?[A-Za-z_]\w*)/.exec(text.slice(i));
    if (!segment) { i = text.length; return []; }
    i += segment[0].length;
    const name = segment[1].replace(/^r#/, '');
    space();
    if (text.startsWith('::', i)) {
      i += 2;
      return tree([...prefix, name]);
    }
    const rename = /^as\s+((?:r#)?\w+)/.exec(text.slice(i));
    if (rename) i += rename[0].length;
    const alias = rename ? rename[1].replace(/^r#/, '') : name;
    if (name === 'self') return [{path: prefix, alias: alias === 'self' ? prefix.at(-1) : alias}];
    return [{path: [...prefix, name], alias}];
  };
  return tree([]);
}

// Where `mod name;` in `file` (whose children live in `childDir`) loads from,
// and where that module's own children live.
function moduleFile(file, childDir, name, explicit, inline) {
  if (explicit) {
    const found = path.join(inline ? childDir : path.dirname(file), explicit);
    return [found, path.dirname(found)];
  }
  const flat = path.join(childDir, `${name}.rs`);
  if (fs.existsSync(flat)) return [flat, path.join(childDir, name)];
  const nested = path.join(childDir, name, 'mod.rs');
  return fs.existsSync(nested) ? [nested, path.dirname(nested)] : [undefined];
}

function source(file) {
  const raw = fs.readFileSync(file, 'utf8');
  return {raw, text: strip(raw)};
}

// --- library module index ---

const PUBLIC = new RegExp(String.raw`^\s*pub\s+${MODIFIERS}(fn|struct|enum|trait|type|const|static|union|mod)\s+(?:r#)?(\w+)`);

class Library {
  constructor(crate, root, features) {
    this.crate = crate;
    this.root = root;
    this.features = features;
    this.modules = new Map();
  }

  rootModule() {
    return this.module(this.root, path.dirname(this.root));
  }

  module(file, childDir, inline) {
    const key = inline ? `${file}#${inline.from}` : file;
    if (this.modules.has(key)) return this.modules.get(key);
    const module = {names: new Map()};
    this.modules.set(key, module);
    if (!inline && !fs.existsSync(file)) return module;
    const {text, raw} = inline ?? source(file);
    const add = (name, entry, attributes, on) => {
      if (!module.names.has(name)) module.names.set(name, []);
      module.names.get(name).push({...entry, enabled: on, cfg: attributes.filter(a => a.cfg).map(a => a.cfg)});
    };
    for (const item of items(text, raw, inline?.from, inline?.to)) {
      if (item.inner) continue;
      const on = enabled(item.attributes, this.features);
      const named = PUBLIC.exec(item.head);
      if (named?.[1] === 'mod') {
        const name = named[2];
        const explicit = item.attributes.find(a => a.path)?.path;
        add(name, {mod: () => {
          if (item.body) return this.module(file, path.join(childDir, name), {text, raw, from: item.body[0], to: item.body[1]});
          const [found, dir] = moduleFile(file, childDir, name, explicit, inline);
          return found ? this.module(found, dir) : undefined;
        }}, item.attributes, on);
      } else if (named) add(named[2], {}, item.attributes, on);
      else {
        const use = /^\s*pub\s+use\s+([\s\S]*?)\s*;?\s*$/.exec(text.slice(item.start, item.end));
        for (const entry of use ? useTree(use[1]) : []) {
          if (!entry.glob) add(entry.alias, {use: entry.path, from: module}, item.attributes, on);
        }
      }
    }
    return module;
  }

  // The first configured-out segment of `segments`, or undefined.
  resolve(segments) {
    let module = this.rootModule();
    for (let n = 0; n < segments.length; n++) {
      const entries = module.names.get(segments[n]);
      if (!entries?.length) return undefined;
      const live = entries.filter(entry => entry.enabled !== false);
      if (!live.length) {
        const features = new Set();
        for (const entry of entries) for (const cfg of entry.cfg) featureNames(cfg, features);
        return {segment: [this.crate, ...segments.slice(0, n + 1)].join('::'), features: [...features].sort()};
      }
      if (n === segments.length - 1) return undefined;
      module = live.map(entry => this.descend(entry, 0)).find(Boolean);
      if (!module) return undefined;
    }
    return undefined;
  }

  descend(entry, depth) {
    if (entry.mod) return entry.mod();
    if (!entry.use || depth > 8) return undefined;
    let [first, ...rest] = entry.use;
    let module = entry.from;
    if (first === 'crate') module = this.rootModule();
    else if (first === 'super') return undefined;
    else if (first !== 'self') rest = entry.use;
    for (const segment of rest) {
      const live = (module.names.get(segment) ?? []).filter(e => e.enabled !== false);
      module = live.map(e => this.descend(e, depth + 1)).find(Boolean);
      if (!module) return undefined;
    }
    return module;
  }
}

// --- feature resolution over `cargo metadata` ---

// The features Cargo enables in each workspace package when building targets
// of `root` with `features`, including dev-dependencies of `root` only.
export function resolveFeatures(metadata, root, features, dev = true) {
  const byName = new Map(metadata.packages.map(p => [p.name, p]));
  const on = new Map();
  const active = new Map();
  const depName = dep => dep.rename ?? dep.name;
  const edge = dep => {
    if (!onHost(dep)) return;
    const target = byName.get(dep.name);
    if (!target) return;
    activate(target);
    for (const feature of dep.features) enable(target, feature);
    if (dep.uses_default_features && target.features.default) enable(target, 'default');
  };
  const activate = (pkg, dev = false) => {
    if (active.has(pkg.name)) return;
    active.set(pkg.name, new Set());
    on.set(pkg.name, new Set());
    for (const dep of pkg.dependencies) {
      if (!dep.optional && (dep.kind === null || (dev && dep.kind === 'dev'))) edge(dep);
    }
  };
  const optional = (pkg, name) => {
    if (active.get(pkg.name).has(name)) return;
    active.get(pkg.name).add(name);
    for (const dep of pkg.dependencies) if (dep.optional && depName(dep) === name && dep.kind !== 'build') edge(dep);
  };
  const live = (pkg, name) => active.get(pkg.name).has(name) || pkg.dependencies.some(d => depName(d) === name && !d.optional && d.kind !== 'build');
  const enable = (pkg, feature) => {
    activate(pkg);
    if (on.get(pkg.name).has(feature)) return;
    on.get(pkg.name).add(feature);
    for (const entry of pkg.features[feature] ?? []) {
      const dep = /^dep:(.+)$/.exec(entry);
      const sub = /^([^/?]+)(\??)\/(.+)$/.exec(entry);
      if (dep) optional(pkg, dep[1]);
      else if (sub) {
        if (sub[2] && !live(pkg, sub[1])) continue;
        if (!sub[2]) optional(pkg, sub[1]);
        for (const d of pkg.dependencies) {
          if (depName(d) === sub[1] && d.kind !== 'build' && byName.has(d.name)) enable(byName.get(d.name), sub[3]);
        }
      } else if (pkg.features[entry]) enable(pkg, entry);
      else optional(pkg, entry);
    }
  };
  activate(root, dev);
  for (const feature of features) enable(root, feature);
  return {on, active};
}

// Whether a dependency's `target` table applies to the native Linux host.
function onHost(dep) {
  if (!dep.target) return true;
  const cfg = /^cfg\((.*)\)$/s.exec(dep.target);
  return cfg ? evaluate(parseCfg(cfg[1]), new Set()) !== false : dep.target === 'x86_64-unknown-linux-gnu';
}

// An optional dependency the closure leaves disabled: any use of it fails.
class Inactive {
  constructor(crate, features) {
    this.crate = crate;
    this.features = features;
  }

  resolve() {
    return {segment: this.crate, features: this.features};
  }
}

function inactiveDependencies(pkg, active, dev) {
  const applies = dep => dep.kind !== 'build' && (dev || dep.kind !== 'dev') && onHost(dep);
  const name = dep => dep.rename ?? dep.name;
  const out = new Map();
  for (const dep of pkg.dependencies) {
    if (!dep.optional || !applies(dep) || active.has(name(dep))) continue;
    if (pkg.dependencies.some(other => name(other) === name(dep) && !other.optional && applies(other))) continue;
    const enabling = Object.entries(pkg.features)
      .filter(([, entries]) => entries.some(e => e === name(dep) || e === `dep:${name(dep)}` || e.startsWith(`${name(dep)}/`)))
      .map(([feature]) => feature);
    const crate = name(dep).replaceAll('-', '_');
    out.set(crate, new Inactive(crate, enabling.sort()));
  }
  return out;
}

// --- targets ---

function references(text, crates, visit) {
  for (const match of text.matchAll(/\buse\s+((?:::)?\s*\w[\s\S]*?);/g)) {
    for (const entry of useTree(match[1])) {
      const [first, ...rest] = entry.path;
      if (crates.has(first) && rest.length) visit(crates.get(first), rest);
    }
  }
  for (const [crate, library] of crates) {
    const inline = new RegExp(String.raw`(?<![\w:])${crate}((?:\s*::\s*(?:r#)?[A-Za-z_]\w*)+)`, 'g');
    for (const match of text.matchAll(inline)) {
      visit(library, match[1].split('::').map(s => s.trim().replace(/^r#/, '')).filter(Boolean));
    }
  }
}

function scanTarget(file, childDir, features, crates, visit, seen) {
  if (seen.has(file) || !fs.existsSync(file)) return;
  seen.add(file);
  const {text, raw} = source(file);
  const walk = (from, to, dir, inline) => {
    for (const item of items(text, raw, from, to)) {
      if (item.inner) {
        if (item.inner.cfg && evaluate(item.inner.cfg, features) !== true) return;
        continue;
      }
      if (enabled(item.attributes, features) !== true) continue;
      const mod = /^\s*(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+(?:r#)?(\w+)\s*;?\s*$/.exec(text.slice(item.start, item.body ? item.body[0] - 1 : item.end));
      if (mod && !item.body) {
        const [found, next] = moduleFile(file, dir, mod[1], item.attributes.find(a => a.path)?.path, inline);
        if (found) scanTarget(found, next, features, crates, visit, seen);
        continue;
      }
      if (!item.body || /^\s*(?:pub(?:\s*\([^)]*\))?\s+)?use\b/.test(item.head)) {
        references(text.slice(item.start, item.end), crates, visit);
        continue;
      }
      references(text.slice(item.start, item.body[0]), crates, visit);
      walk(item.body[0], item.body[1], mod ? path.join(dir, mod[1]) : dir, inline || Boolean(mod));
      references(text.slice(item.body[1], item.end), crates, visit);
    }
  };
  walk(0, text.length, childDir, false);
}

export function check(metadata) {
  const members = new Set(metadata.workspace_members);
  const libraryTarget = pkg => pkg.targets.find(t => t.kind.some(k => k === 'lib' || k === 'rlib' || k === 'proc-macro'));
  const findings = [];
  for (const pkg of metadata.packages) {
    if (!members.has(pkg.id)) continue;
    for (const target of pkg.targets) {
      if (!target.kind.some(k => KINDS.has(k))) continue;
      const features = [...(target['required-features'] ?? []), ...(pkg.features.default ? ['default'] : [])];
      const dev = !target.kind.includes('bin');
      const {on: sets, active} = resolveFeatures(metadata, pkg, features, dev);
      const crates = inactiveDependencies(pkg, active.get(pkg.name), dev);
      for (const other of metadata.packages) {
        const lib = libraryTarget(other);
        const deps = pkg.dependencies.filter(dep => dep.name === other.name && dep.kind !== 'build' && (dev || dep.kind !== 'dev'));
        if (!lib || !sets.has(other.name) || (other !== pkg && !deps.length)) continue;
        const library = new Library(lib.name.replaceAll('-', '_'), lib.src_path, sets.get(other.name));
        crates.set(library.crate, library);
        for (const dep of deps) if (dep.rename) crates.set(dep.rename.replaceAll('-', '_'), library);
      }
      const reported = new Set();
      const visit = (library, segments) => {
        const finding = library.resolve(segments);
        if (!finding || reported.has(finding.segment)) return;
        reported.add(finding.segment);
        findings.push({package: pkg.name, kind: target.kind[0], target: target.name, ...finding});
      };
      scanTarget(target.src_path, path.dirname(target.src_path), sets.get(pkg.name), crates, visit, new Set());
    }
  }
  return findings;
}

export function format(finding) {
  const features = finding.features.map(f => JSON.stringify(f)).join(', ') || 'a non-feature cfg';
  return `${finding.package} ${finding.kind} ${JSON.stringify(finding.target)} uses ${finding.segment}, configured out by its feature closure (gated on ${features}); declare the needed features in required-features`;
}
