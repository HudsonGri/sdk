import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';

export function decodeTypedVerdicts(response, paths) {
  if (!Array.isArray(response) || response.length !== paths.length) throw new Error('incomplete typed compiler response');
  const expected = new Set(paths);
  const found = new Map();
  for (const row of response) {
    if (!Array.isArray(row) || row.length !== 2
      || !expected.has(row[0]) || found.has(row[0])
      || typeof row[1] !== 'string' || !/^(?:[01]|!indeterminate: [^\r\n\0]+)$/.test(row[1])) {
      throw new Error('invalid or duplicate typed compiler response row');
    }
    found.set(row[0], row[1]);
  }
  if (found.size !== expected.size) throw new Error('missing typed compiler response');
  return found;
}

// A source fragment has no crate bindings. Repository files must instead be
// judged by the compiler in their Cargo target, including macro expansion,
// lexical bindings, method resolution and included module bodies.
export function typedVerdicts(sources, paths) {
  if (!paths.length) return new Map();
  const reject = reason => new Map(paths.map(path => [path, `!indeterminate: ${reason}`]));
  const unchanged = () => [...sources].every(([path, source]) =>
    readFileSync(new URL(`../${path}`, import.meta.url), 'utf8').replaceAll('\r\n', '\n') === source);
  try {
    if (!unchanged()) return reject('source map differs from the compilation inputs');
    const response = JSON.parse(execFileSync('python3', [fileURLToPath(new URL('./loom-type-contexts.py', import.meta.url))], {
      input: JSON.stringify(paths), encoding: 'utf8', maxBuffer: 1 << 26,
    }));
    const verdicts = decodeTypedVerdicts(response, paths);
    if (!unchanged()) return reject('compilation inputs changed during analysis');
    return verdicts;
  } catch (error) {
    return reject(`typed model compilation failed: ${String(error.message).replaceAll(/[\r\n\0]/g, ' ')}`);
  }
}
