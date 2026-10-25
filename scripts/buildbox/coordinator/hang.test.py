import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('gate_hang', HERE/'gate-hang.py')
hang = importlib.util.module_from_spec(spec); spec.loader.exec_module(hang)
START = '     Running tests/identity.rs (/mnt/bb/targets/debug/deps/identity-abcd)\n'
SLOW = 'test identity_processes has been running for over 60 seconds\n'
KILLED = '[buildbox] hang reaper: the job used 394 ms of CPU in the last 10 min (minimum 5 s): presumed hung (deadlocked test or stuck wait); killed (rc 124), admission slot freed.\n'
CAPTURE = (HERE/'fixtures/cargo-binary-cutoff.txt').read_text()
CASE = 'local::tests::checkout_reopen::echo_reuses_the_lazy_checkout_without_reading_untouched_files'


class HangTests(unittest.TestCase):
    def test_real_cargo_cutoff_survives_later_successful_targets(self):
        reason = hang.binary_cutoffs(CAPTURE)
        self.assertIn(CASE, reason)
        self.assertIn('executable=/work/target/.gate-pr-tests/debug/deps/acyclic_harness-a76c91bcbdd4f049', reason)
        self.assertIn('cargo test --locked --all-features -p acyclic-harness --lib '+CASE, reason)
        self.assertNotIn('hung test', reason, 'a ceiling is not an idle-deadlock proof')
        for name in ['partition_7', 'partition_8', 'partition_9']:
            self.assertNotIn(name, reason, 'completed cases are retired')
        # The old idle-only reader misses this actual Cargo101 failure.
        self.assertIsNone(hang.unfinished(CAPTURE, 101))
        self.assertIsNone(hang.binary_cutoffs('\n'.join(line for line in CAPTURE.splitlines()
                                                    if not line.startswith('[buildbox] test binary '))))

    def test_cutoff_identity_and_late_completions_fail_closed(self):
        executable = '/work/target/.gate-pr-tests/debug/deps/acyclic_harness-a76c91bcbdd4f049'
        # Same basename from a different directory is not this Cargo artifact.
        with self.assertRaisesRegex(ValueError, 'missing matching Cargo'):
            hang.binary_cutoffs(CAPTURE.replace(' '+executable+'`', ' /other/'+Path(executable).name+'`'))
        with self.assertRaisesRegex(ValueError, 'no matching active Cargo'):
            hang.binary_cutoffs(CAPTURE.replace('test binary acyclic_harness-', 'test binary unrelated-'))
        with self.assertRaisesRegex(ValueError, 'missing matching Cargo'):
            hang.binary_cutoffs('\n'.join(line for line in CAPTURE.splitlines() if 'process didn\'t exit' not in line))
        boundary = '     Running tests/client_mailbox_model.rs'
        late = CAPTURE.replace(boundary, f'test {CASE} ... ok\n'+boundary)
        reason = hang.binary_cutoffs(late)
        self.assertNotIn(CASE, reason)
        self.assertIn('binary did not complete', reason)

    def test_incomplete_main_binary_never_qualifies_matching_footer(self):
        source = (HERE/'gate-test-baseline.py').read_text()
        namespace = {'__file__': str(HERE/'gate-test-baseline.py'), '__name__': 'baseline_control'}
        exec(compile(source, namespace['__file__'], 'exec'), namespace)
        actual = CAPTURE+'GATE_TEST_BASELINE_COMPLETE rc=101\nGATE_TEST_BASELINE_EXECUTOR rc=101\n'
        self.assertFalse(namespace['baseline_complete'](actual))
        # Remove precisely the new shared-reader refusal: the former reader
        # accepts this killed target because another target has a real summary.
        old = source.replace('if evidence.binary_cutoffs(output):', 'if False:')
        exec(compile(old, 'old-baseline-cutoff-control', 'exec'), namespace)
        self.assertTrue(namespace['baseline_complete'](actual))

    def test_actual_completed_payload_dispatch_cannot_ratchet_away_a_cutoff(self):
        source = (HERE/'pr-gate.sh').read_text()
        block = source[source.index('# Cargo can finish with101'):source.index('# Keep the prepared tree')]
        self.assertLess(source.index('focused main baseline unavailable'), source.index('# Cargo can finish with101'))
        block = block.replace('python Q:/lanes/gate-hang.py', f'"{sys.executable}" "{HERE / "gate-hang.py"}"')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root/'.codex-fleet').mkdir()
            log = root/'gate.log'; log.write_text(CAPTURE)
            for negative in [False, True]:
                actual = ':' if negative else block
                run = subprocess.run(['bash', '-c', f'rc=101;n=1;log="{log}"\n{actual}\nexit 0\n'],
                                     env={**os.environ, 'HOME': tmp}, capture_output=True, text=True)
                self.assertEqual(run.returncode, 0 if negative else 1, run.stdout+run.stderr)
                if not negative:
                    self.assertIn('FAIL #1: incomplete test execution: '+CASE, run.stdout)
                    self.assertIn('recovery command:', run.stdout)
            log.write_text(CAPTURE.replace('test binary acyclic_harness-', 'test binary unrelated-'))
            run = subprocess.run(['bash', '-c', f'rc=101;n=1;log="{log}"\n{block}\nexit 0\n'],
                                 env={**os.environ, 'HOME': tmp}, capture_output=True, text=True)
            self.assertEqual(run.returncode, 4, run.stdout+run.stderr)
            self.assertIn('INFRA #1: binary-cutoff evidence step failed', run.stdout)
            self.assertIn('recovery command:', run.stdout+run.stderr)

    def test_idle_watchdog_names_the_last_unfinished_test(self):
        self.assertIn('hung test identity_processes in tests/identity.rs', hang.unfinished(START+SLOW+KILLED, 124))
        self.assertIsNone(hang.unfinished(START+SLOW, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+KILLED, 224))
        self.assertIsNone(hang.unfinished(START+'test done ... ok\n'+KILLED, 124))

    def test_finished_or_previous_binary_warnings_do_not_accuse_a_test(self):
        self.assertIsNone(hang.unfinished(START+SLOW+'test identity_processes ... ok\n'+KILLED, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+'     Running tests/other.rs (/tmp/other)\n'+KILLED, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+'test identity_processes ... FAILED\n'+KILLED, 124))
        text = START+SLOW+'test second has been running for over 60 seconds\n'+KILLED
        self.assertIn('hung test second', hang.unfinished(text, 124))
        self.assertIn('second, identity_processes', hang.unfinished(text, 124))

    def test_late_completions_after_watchdog_are_read_before_any_verdict(self):
        source = (HERE/'gate-hang.py').read_text()
        old = {'__name__':'early_stop_control'}
        exec(compile(source.replace('watchdog = True', 'watchdog = True\n            break'), 'early-stop-control', 'exec'), old)
        for status in ['ok', 'FAILED', 'ignored']:
            with self.subTest(status=status):
                text = START+SLOW+KILLED+f'test identity_processes ... {status}\n'
                self.assertIsNone(hang.unfinished(text, 124))
                self.assertIn('hung test identity_processes', old['unfinished'](text, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+KILLED+'     Running tests/next.rs (/tmp/next)\n', 124))

    def test_actual_coordinator_separates_source_hang_from_transport_loss(self):
        source = (HERE/'pr-gate.sh').read_text()
        block = source[source.index('# An ended client stream'):source.index('# A successful payload')]
        block = block.replace('python Q:/lanes/gate-hang.py', f'"{sys.executable}" "{HERE / "gate-hang.py"}"')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root/'.codex-fleet').mkdir()
            for text, rc, negative, expected in [
                (START+SLOW+KILLED, 124, False, 1),
                (START+SLOW, 124, False, 4),
                (START+SLOW+KILLED, 224, False, 4),
                (START+SLOW+'test identity_processes ... ok\n'+KILLED, 124, False, 4),
                (START+SLOW+KILLED+'test identity_processes ... ok\n', 124, False, 4),
                (START+SLOW+KILLED, 124, True, 4),
            ]:
                log = root/'gate.log'; log.write_text(text)
                actual = block
                if negative:
                    actual = actual.replace('if [ "$rc" = 124 ]; then', 'if false; then')
                run = subprocess.run(['bash', '-c', f'rc={rc};n=1;log="{log}"\n{actual}\nexit 0\n'],
                                     env={**os.environ, 'HOME':tmp}, capture_output=True, text=True)
                self.assertEqual(run.returncode, expected, run.stdout+run.stderr)
                self.assertIn('hung test identity_processes' if expected == 1 else 'INFRA', run.stdout)
                self.assertNotIn('GATE_DONE', log.read_text())


if __name__ == '__main__':
    unittest.main(verbosity=2)
