import { readFileSync, readdirSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export function violations(source, filename, packageRoot) {
  const problems = [];
  const harnessFuzz = packageRoot.split(sep).at(-1) === 'harness-fuzz';
  for (const match of source.matchAll(/#\[\s*path\s*=\s*"([^"]+)"\s*\]/g)) {
    const target = resolve(dirname(filename), match[1]);
    const within = relative(packageRoot, target);
    if ((harnessFuzz && match[1]) || within === '..' || within.startsWith(`..${sep}`) || isAbsolute(within)) {
      const line = source.slice(0, match.index).split('\n').length;
      problems.push(`${filename}:${line}: import ${match[1]} through the owning crate's API`);
    }
  }
  for (const match of source.matchAll(/\binclude!\s*\(/g)) {
    const line = source.slice(0, match.index).split('\n').length;
    problems.push(`${filename}:${line}: compile shared code through the owning crate instead of include!`);
  }
  return problems;
}

export function check(root) {
  const problems = [];
  function visit(directory, fuzzPackage) {
    const entries = readdirSync(directory, { withFileTypes: true });
    const manifest = entries.find(entry => entry.name === 'Cargo.toml');
    if (manifest) {
      fuzzPackage = /cargo-fuzz\s*=\s*true/.test(readFileSync(join(directory, manifest.name), 'utf8'))
        ? directory : undefined;
    }
    for (const entry of entries) {
      const filename = join(directory, entry.name);
      if (entry.isDirectory() && !['target', 'node_modules', '.git'].includes(entry.name)) visit(filename, fuzzPackage);
      else if (entry.isFile() && entry.name.endsWith('.rs') && fuzzPackage) {
        problems.push(...violations(readFileSync(filename, 'utf8'), filename, fuzzPackage));
      }
    }
  }
  visit(resolve(root), undefined);
  return problems;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const problems = check(resolve(dirname(fileURLToPath(import.meta.url)), '..'));
  if (problems.length) {
    process.stderr.write(`${problems.join('\n')}\n`);
    process.exitCode = 1;
  } else process.stdout.write('All Rust fuzz packages use their owning crates for external code.\n');
}
