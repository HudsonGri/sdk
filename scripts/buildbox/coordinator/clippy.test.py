#!/usr/bin/env python3
"""Captured Cargo short errors exercise emitters, public verdict and coordinator."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / 'fixtures'
RAW = (FIXTURES / 'authority104-clippy-errors.txt').read_text()
BASE = (FIXTURES / 'authority104-main-clippy-status.txt').read_text()



class ClippyDiagnostics(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / 'cargo.log'; self.raw.write_text(RAW)
        self.base = self.root / 'main.log'; self.base.write_text(BASE)
        self.log = self.root / 'pr.log'
        for name in ['gate-checks.sh', 'gate-checks-results.sh', 'gate-clippy.py', 'failed-tests.sh']:
            shutil.copyfile(HERE / name, self.root / name)

    def verdict(self):
        return subprocess.run(['bash', str(self.root / 'gate-checks-results.sh'), str(self.log), str(self.base)],
                              capture_output=True, text=True)

    def test_captured_short_errors_fail_even_through_old_emission(self):
        emitted = subprocess.run(['python3', str(HERE / 'gate-clippy.py'), 'emit', str(self.raw), '101'],
                                 capture_output=True, text=True, check=True)
        self.assertEqual(emitted.stdout.count('GATE_CLIPPY_ERROR '), 2)
        self.assertEqual(emitted.stdout.count('error[E0277]'), 2)
        raw = ''.join('GATE_CLIPPY_RAW ' + line + '\n' for line in RAW.splitlines())
        # The live legacy emitters retained raw diagnostics but lost parsed
        # error[E...] markers. Both this case and the new emitter must fail.
        for parsed in ['', emitted.stdout]:
            self.log.write_text('GATE_CLIPPY rc=101\n' + raw + parsed)
            result = self.verdict()
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertEqual(result.stdout.count('clippy-error: '), 2)
            self.assertIn('error[E0277]', result.stdout)
        shutil.copyfile(FIXTURES / 'authority104-old-results.sh.txt', self.root / 'old.sh')
        self.log.write_text('GATE_CLIPPY rc=101\n' + raw)
        old = subprocess.run(['bash', str(self.root / 'old.sh'), str(self.log), str(self.base)], capture_output=True, text=True)
        self.assertEqual(old.returncode, 0, 'actual old public verdict falsely accepts these captured compiler errors')

    def test_missing_diagnostics_and_operational_status_are_typed_infra(self):
        for status in ['101', '127', '-9']:
            self.log.write_text('GATE_CLIPPY rc=' + status + '\n')
            result = self.verdict()
            self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
            self.assertIn('recovery command:', result.stdout)

    def test_both_actual_emitters_decode_captured_short_errors(self):
        (self.root / '.gate-clippy.out').write_text(RAW)
        (self.root / '.gate-clippy.rc').write_text('101\n')
        hook = 'python3 scripts/buildbox/coordinator/gate-clippy.py emit '
        for name, end in [('main-gate-checks.sh', 'wait "$features"'),
                          ('pr-gate-checks.sh', 'if [ -n "$features" ]; then')]:
            source = (HERE.parents[1] / name).read_text()
            start = source.index('echo "GATE_CLIPPY rc=')
            emission = source[start:source.index(end, start)]
            for negative in [False, True]:
                code = emission.replace(hook, ': # ' + hook) if negative else emission
                output = subprocess.run(['bash', '-c', code], cwd=HERE.parents[2], capture_output=True, text=True,
                                        env={**os.environ, 'gate_output': str(self.root)})
                self.assertEqual(output.returncode, 0, output.stdout + output.stderr)
                markers = [line for line in output.stdout.splitlines() if line.startswith('GATE_CLIPPY_ERROR ')]
                self.assertEqual(len(markers), 0 if negative else 2, name)
                self.log.write_text(output.stdout)
                self.assertEqual(self.verdict().returncode, 1, 'raw-output defense remains active even without emitter markers')

    def test_warning_ratchet_and_known_source_errors_remain_comparable(self):
        # The warning bytes come from the same real Cargo capture.
        warning = next(line for line in (FIXTURES / 'main-32b799-phases.txt').read_text().splitlines()
                       if line.startswith('GATE_CLIPPY_FINDING '))
        self.base.write_text(BASE + warning + '\n')
        self.log.write_text(BASE + warning + '\n')
        self.assertEqual(self.verdict().returncode, 0)
        self.base.write_text(BASE)
        self.assertEqual(self.verdict().returncode, 1)
        raw = ''.join('GATE_CLIPPY_RAW ' + line + '\n' for line in RAW.splitlines())
        self.base.write_text('GATE_CLIPPY rc=101\n' + raw)
        self.log.write_text(self.base.read_text())
        self.assertEqual(self.verdict().returncode, 0)

    def test_coordinator_preserves_source_and_infra_verdicts(self):
        source = (HERE / 'pr-gate.sh').read_text()
        chunk = source[source.index('pr_fails=$('):].replace('Q:/lanes/failed-tests.sh', str(self.root / 'failed-tests.sh')).replace(
            'Q:/lanes/flakes.txt', str(self.root / 'flakes')).replace('Q:/lanes/gate-checks-verdict.sh', str(self.root / 'gate-checks-results.sh'))
        (self.root / 'flakes').write_text(''); (self.root / '.codex-fleet').mkdir()
        raw = ''.join('GATE_CLIPPY_RAW ' + line + '\n' for line in RAW.splitlines())
        for status, diagnostics, expected in [('101', raw, 1), ('101', '', 4), ('0', '', 0)]:
            self.log.write_text('GATE_DONE rc=0\nGATE_CLIPPY rc=' + status + '\n' + diagnostics)
            header = f'rc=0;n=1;base=main;log={self.log};base_log={self.base};test_base_log="";clippy_base={self.base};features_base={self.base}\n'
            run = subprocess.run(['bash', '-c', header + chunk], capture_output=True, text=True, env={**os.environ, 'HOME': str(self.root)})
            self.assertEqual(run.returncode, expected, run.stdout + run.stderr)
        old = chunk.replace('*) v="INFRA #$n: clippy/feature interpretation', '*) v="FAIL #$n: clippy/feature interpretation')
        self.log.write_text('GATE_DONE rc=0\nGATE_CLIPPY rc=101\n')
        negative = subprocess.run(['bash', '-c', header + old], capture_output=True, text=True, env={**os.environ, 'HOME': str(self.root)})
        self.assertEqual(negative.returncode, 1, 'former nonzero-as-source classification hides typed INFRA')


if __name__ == '__main__':
    unittest.main(verbosity=2)
