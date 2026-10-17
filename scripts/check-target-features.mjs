import {execFileSync} from 'node:child_process';
import {check, format} from './target-features.mjs';

const metadata = JSON.parse(execFileSync('cargo', ['metadata', '--locked', '--offline', '--no-deps', '--format-version', '1'], {encoding: 'utf8', maxBuffer: 1 << 28}));
const findings = check(metadata);
if (findings.length) {
  console.error(findings.map(format).join('\n'));
  process.exitCode = 1;
} else {
  const targets = metadata.packages.flatMap(p => p.targets).filter(t => t.kind.some(k => ['test', 'example', 'bench', 'bin'].includes(k))).length;
  console.log(`Target feature declarations passed for ${targets} test, example, bench and bin targets`);
}
