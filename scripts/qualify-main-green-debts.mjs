#!/usr/bin/env node
// A successful Cargo exit with no matching tests cannot discharge a main debt.
import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

export function cargoArguments(entry) {
  if (!/^acyclic-[a-z0-9-]+$/.test(entry.package)) throw new Error('invalid package');
  if (typeof entry.target !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]*$/.test(entry.target)) throw new Error('invalid test target');
  if (!entry.regression || !Array.isArray(entry.controls) || !entry.controls.length) throw new Error('regression and negative control required');
  const names = [...new Set([entry.regression, ...entry.controls])];
  if (names.some(name => typeof name !== 'string' || !/^[a-zA-Z0-9_:]+$/.test(name))) throw new Error('invalid exact test name');
  const target = entry.target === 'lib' ? ['--lib'] : ['--test', entry.target];
  return ['test', '--locked', '-p', entry.package, '--all-features', ...target,
    '--', ...names, '--exact', '--test-threads=4', '--color=never'];
}

export function verify(entry, status, output) {
  cargoArguments(entry);
  if (status !== 0) throw new Error(`Cargo failed: ${status}`);
  const expected = [...new Set([entry.regression, ...entry.controls])];
  const actual = [...output.matchAll(/^test (\S+)(?: - should panic)? \.\.\. ok\r?$/gm)].map(match => match[1]);
  if (actual.length !== expected.length || new Set(actual).size !== actual.length
    || expected.some(name => !actual.includes(name))) {
    throw new Error(`expected every regression and negative control: ${JSON.stringify({ expected, actual })}`);
  }
  const summaries = [...output.matchAll(/^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; \d+ filtered out; finished in ([\d.]+)s\r?$/gm)];
  if (summaries.length !== 1) throw new Error('exactly one libtest summary required');
  const [, passed, failed, ignored, measured, seconds] = summaries[0];
  if (Number(passed) !== expected.length || [failed, ignored, measured].some(value => Number(value) !== 0)) {
    throw new Error('missing, ignored, failed or measured tests cannot discharge a debt');
  }
  if (!Number.isFinite(Number(seconds)) || Number(seconds) >= 10) throw new Error('suite must finish in under 10 seconds');
  return { regression: entry.regression, tests: actual, seconds: Number(seconds) };
}

async function run(entry) {
  const args = cargoArguments(entry);
  console.log(`MAIN_GREEN_COMMAND cargo ${args.join(' ')}`);
  const child = spawn('cargo', args, { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, RUST_TEST_THREADS: '4' } });
  let output = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => { output += chunk; process.stdout.write(chunk); });
  child.stderr.pipe(process.stderr);
  const status = await new Promise((accept, reject) => {
    child.on('error', reject);
    child.on('close', accept);
  });
  const result = verify(entry, status, output);
  console.log(`MAIN_GREEN_QUALIFIED ${JSON.stringify(result)}`);
  return result;
}

export async function main(selected = process.argv.slice(2)) {
  process.chdir(fileURLToPath(new URL('../', import.meta.url)));
  const entries = JSON.parse(await readFile(new URL('./main-green-debts.json', import.meta.url), 'utf8'));
  const names = new Set(entries.map(entry => entry.regression));
  if (names.size !== entries.length) throw new Error('duplicate debt');
  for (const name of selected) if (!names.has(name)) throw new Error(`unknown debt: ${name}`);
  // Validate the complete plan before starting any build.
  for (const entry of entries) cargoArguments(entry);
  const results = [];
  const failures = [];
  for (const entry of entries) {
    if (!selected.length || selected.includes(entry.regression)) {
      try {
        results.push(await run(entry));
      } catch (error) {
        const failure = { regression: entry.regression, error: String(error) };
        failures.push(failure);
        console.error(`MAIN_GREEN_FAILED ${JSON.stringify(failure)}`);
      }
    }
  }
  console.log(`MAIN_GREEN_DONE ${JSON.stringify({ qualified: results.length, total: entries.length, results, failures })}`);
  if (failures.length) throw new Error(`${failures.length} debt qualifications failed`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}
