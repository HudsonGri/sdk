import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {posix} from 'node:path';
import {expandFuzzIncludes, fuzzOracleSymbols, fuzzOracleViolations, rustTokens} from './fuzz-oracle-policy.mjs';

const [ref, ...extra] = process.argv.slice(2);
if (extra.length) throw new Error('usage: check-fuzz-oracles.mjs [git-ref]');
const git = (...args) => execFileSync('git', args, {encoding: 'utf8', maxBuffer: 32 * 1024 * 1024});
const commit = ref && git('rev-parse', '--verify', '--end-of-options', `${ref}^{commit}`).trim();
const tracked = new Set((commit ? git('ls-tree', '-r', '-z', '--name-only', commit) : git('ls-files', '-z')).split('\0').filter(Boolean));
const paths = [...tracked].filter(path => /(?:^|\/)fuzz_targets\/.*\.rs$/.test(path));
const readSources = names => {
  if (!commit) return names.map(path => readFileSync(path, 'utf8'));
  const output = execFileSync('git', ['cat-file', '--batch'], {
    input: names.map(path => `${commit}:${path}\n`).join(''), maxBuffer: 128 * 1024 * 1024,
  });
  let offset = 0;
  return names.map(() => {
    const end = output.indexOf(10, offset), header = output.subarray(offset, end).toString();
    const match = /^[0-9a-f]+ blob (\d+)$/.exec(header);
    if (end < 0 || !match) throw new Error('could not read exact Git source blob: ' + header);
    const start = end + 1, size = Number(match[1]);
    offset = start + size + 1;
    return output.subarray(start, start + size).toString('utf8');
  });
};
const rawSources = readSources(paths);
const raw = new Map(paths.map((path, index) => [path, rawSources[index]]));
const load = path => {
  if (!tracked.has(path)) throw new Error('include is outside the tracked source tree: ' + path);
  if (!raw.has(path)) raw.set(path, readSources([path])[0]);
  return raw.get(path);
};
const resolveInclude = (file, reference, manifest) => {
  let directory = posix.dirname(file);
  if (manifest) {
    while (!tracked.has(posix.join(directory, 'Cargo.toml'))) {
      const parent = posix.dirname(directory);
      if (parent === directory) throw new Error('cannot resolve Cargo manifest for ' + file);
      directory = parent;
    }
    if (!reference.startsWith('/')) throw new Error('non-relative manifest include');
    reference = reference.slice(1);
  } else if (posix.isAbsolute(reference) || reference.includes('\\')) throw new Error('non-relative Rust include');
  const path = posix.normalize(posix.join(directory, reference));
  if (path.startsWith('../') || !path.endsWith('.rs')) throw new Error('invalid Rust include: ' + path);
  return path;
};
const sources = paths.map(path => expandFuzzIncludes(path, load(path), resolveInclude, load));
const imports = new Set();
for (const source of sources) {
  const tokens = rustTokens(source);
  for (let i = 0; i < tokens.length; i++) if (tokens[i].text === 'use') {
    while (++i < tokens.length && tokens[i].text !== ';') {
      if (/^[a-z_][a-z_0-9]*$/.test(tokens[i].text)) imports.add(tokens[i].text);
    }
  }
  for (let i = 0; i < tokens.length; i++) if (/^[a-z_][a-z_0-9]*$/.test(tokens[i].text)
    && (tokens[i + 1]?.text === '(' || tokens[i + 1]?.text === '::' && tokens[i + 2]?.text === '<')) imports.add(tokens[i].text);
}
const names = [...imports].join('|');
let declarations;
try {
  declarations = git('grep', '--null', '--name-only', '-E', `fn[[:space:]]+(r#)?(${names})|as[[:space:]]+(${names})`,
    ...(commit ? [commit] : []), '--', 'rust/crates/*/src/*.rs', 'compatibility/*.rs', 'plugin/src/*.rs');
} catch (error) {
  if (error.status !== 1) throw error;
  declarations = '';
}
const symbolPaths = declarations.split('\0').filter(Boolean).map(path => commit ? path.slice(commit.length + 1) : path);
const inputFunctions = fuzzOracleSymbols([...sources, ...readSources(symbolPaths)]);
const errors = [];
for (let index = 0; index < paths.length; index++) {
  const path = paths[index];
  try {
    errors.push(...fuzzOracleViolations(path, sources[index], inputFunctions));
  } catch (error) {
    errors.push(`${path}: fuzz round-trip oracle analysis failed closed: ${error.message}`);
  }
}
if (!paths.length) errors.push('no Rust fuzz targets found');
if (errors.length) { console.error(errors.join('\n')); process.exitCode = 1; }
else console.log(`Fuzz round-trip oracle lint passed (${paths.length} Rust sources${commit ? ` at ${commit}` : ''})`);
