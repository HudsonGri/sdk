"""Real Git repositories and captured Cargo/main output qualify the base."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
FIXTURES = HERE/'fixtures'
spec = importlib.util.spec_from_file_location('baseline', HERE/'gate-baseline.py')
baseline = importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)
PHASES = (FIXTURES/'main-32b799-phases.txt').read_text()
RED_FEATURES = (FIXTURES/'main-da7eee-red-features.txt').read_text()
CAPTURED_HEAD = '32b79969bc504e875bb6e9f65611bf0d02ea45e9'


class ExactBaselineTests(unittest.TestCase):
    def test_completed_red_matrix_counts_sets_not_failed_commands(self):
        self.assertTrue(baseline.check_complete(RED_FEATURES, 'features'))
        # This is actual main109/110: one all-targets command failed and is
        # printed both immediately and in the final summary.
        lines = RED_FEATURES.splitlines(keepends=True)
        failed = [line for line in lines if line.startswith('GATE_FEATURES FAILED cargo ')]
        completed = [line for line in lines if line.startswith('GATE_FEATURE_CHECK feature check: ')]
        self.assertEqual(len(failed), 2)
        self.assertEqual(len(completed), 110)
        self.assertTrue(baseline.check_complete(RED_FEATURES+''.join(failed), 'features'),
                        'repeated command summaries do not change set completion')
        missing = RED_FEATURES.replace(completed[-1], '')
        self.assertFalse(baseline.check_complete(missing, 'features'))
        self.assertFalse(baseline.check_complete(missing+completed[0], 'features'),
                         'duplicate completed sets cannot replace an uncompleted set')
        # The actual former command-count comparison rejects this complete run.
        source = (HERE/'gate-baseline.py').read_text()
        old = source.replace('and len(completed) == len(set(completed)) == total',
            "and len(completed) == total and len(re.findall(r'^GATE_FEATURES FAILED cargo ', native, re.M)) == total-passed")
        self.assertNotEqual(old, source)
        namespace = {'__name__': 'old_command_count_control'}
        exec(compile(old, 'old-command-count-control', 'exec'), namespace)
        self.assertFalse(namespace['check_complete'](RED_FEATURES, 'features'))

    def test_exact_check_selection_never_uses_a_newer_dated_ancestor(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); repo = root/'repo'; repo.mkdir()
            env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
            def git(*args):
                return subprocess.check_output(['git', '-C', str(repo), *args], env=env, stderr=subprocess.DEVNULL).decode().strip()
            git('init', '-q')
            def commit():
                git('-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                    'commit', '-q', '--allow-empty', '-m', 'captured-output identity fixture')
                return git('rev-parse', 'HEAD')
            old = commit(); new = commit()
            logs = root/'logs'; (logs/'wide').mkdir(parents=True)
            ancestor = logs/(old+'.log'); exact = logs/'wide'/(new+'.log')
            ancestor.write_text(PHASES.replace(CAPTURED_HEAD, old))
            exact.write_text(PHASES.replace(CAPTURED_HEAD, new))
            os.utime(ancestor, (exact.stat().st_mtime+10,)*2)
            alerts = f'MAIN_WIDE RED {old}: captured\nMAIN_WIDE RED {new}: captured\n'
            alert_file = root/'alerts'; alert_file.write_text(alerts)
            for phase in ['clippy', 'features']:
                self.assertEqual(baseline.select(repo,new,logs,alerts,phase,True),exact)
                self.assertEqual(baseline.select(repo,new,logs,alerts,phase,False),ancestor,
                                 'former ancestor selection compares the wrong source')
                # Execute the actual coordinator call too. Removing --exact
                # reproduces selection of the newer-dated ancestor.
                statement = next(line for line in (HERE/'pr-gate.sh').read_text().splitlines()
                                 if line.startswith(phase+'_base=$(python '))
                statement = statement.replace('python Q:/lanes/gate-baseline.py',
                    f'"{sys.executable}" "{HERE / "gate-baseline.py"}" --directory "{logs}" --alerts "{alert_file}"')
                for negative in [False, True]:
                    script = f'W="{repo}";base={new};log="{root / "job.log"}"\n'
                    script += statement.replace(' --exact', '') if negative else statement
                    script += '\nprintf "%s" "$'+phase+'_base"\n'
                    run = subprocess.run(['bash','-c',script],capture_output=True,text=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertEqual(run.stdout,str(ancestor if negative else exact))
                original = exact.read_text()
                for broken in [original.replace('GATE_EXECUTOR rc=1', 'GATE_EXECUTOR rc=-9'),
                               original.replace('GATE_EXECUTOR rc=1', ''),
                               original.replace('"head": "'+new+'"', '"head": "'+old+'"')]:
                    exact.write_text(broken)
                    with self.assertRaisesRegex(RuntimeError, 'recovery command:'):
                        baseline.select(repo,new,logs,alerts,phase,True)
                exact.write_text(original)

    def test_native_completion_cannot_be_borrowed_from_windows(self):
        self.assertTrue(baseline.check_complete(PHASES, 'features'))
        lines = PHASES.splitlines(keepends=True)
        wall = next(line for line in lines if line.startswith('GATE_FEATURES_WALL '))
        # The captured native rc marker is joined to an unfinished lint line.
        # Its own complete wall receipt is independent of the Windows phase.
        self.assertFalse(baseline.check_complete(PHASES.replace(wall, wall.replace('rc=0', 'rc=101')), 'features'))
        self.assertFalse(baseline.check_complete(PHASES.replace(wall, ''), 'features'))
        self.assertFalse(baseline.check_complete(PHASES.replace('GATE_EXECUTOR rc=1', ''), 'features'))
        self.assertFalse(baseline.check_complete(PHASES.replace(wall, 'GATE_FEATURES rc=101\n'+wall), 'features'))

    def test_focused_current_results_replace_obsolete_failure_names(self):
        source = (HERE/'pr-gate.sh').read_text()
        block = source[source.index('pr_fails=$('):]
        pr_text = (FIXTURES/'baseline-delta-pr.txt').read_text()
        main_text = (FIXTURES/'baseline-delta-main.txt').read_text()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root/'.codex-fleet').mkdir(); (root/'flakes').write_text('')
            (root/'checks.sh').write_text('exit 0\n')
            (root/'failed-tests.sh').write_text((HERE/'failed-tests.sh').read_text())
            block = block.replace('Q:/lanes/failed-tests.sh', str(root/'failed-tests.sh')).replace(
                'Q:/lanes/flakes.txt', str(root/'flakes')).replace('Q:/lanes/gate-checks-verdict.sh', str(root/'checks.sh'))
            pr = root/'pr.log'; pr.write_text(pr_text)
            stale = root/'old.log'; stale.write_text(pr_text)
            focused = root/'current.log'; focused.write_text(main_text)
            for negative, focus, expected in [(False,str(focused),1),(True,str(focused),0),(False,'',5)]:
                actual = block.replace('effective_test_base=$test_base_log', 'effective_test_base=$base_log') if negative else block
                script = f'rc=101;n=1;base={CAPTURED_HEAD};W=fixture;log={pr};base_log={stale};test_base_log="{focus}";clippy_base={focused};features_base={focused}\n'+actual
                result = subprocess.run(['bash','-c',script],env={**os.environ,'HOME':str(root)},capture_output=True,text=True)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                if expected == 1:
                    self.assertIn('new failures vs exact main (32b79969bc)', result.stdout)
                    self.assertIn('core::element_sizes::conversation_message', result.stdout)
                if expected == 5:
                    self.assertIn('exact-base test comparison step unavailable', result.stdout)
                    self.assertIn('recovery command:', result.stdout)

    def test_both_phase_emitters_separate_an_unterminated_diagnostic(self):
        lint = next(line for line in PHASES.splitlines() if line.startswith('GATE_CLIPPY_FINDING ')).split('GATE_FEATURES rc=')[0]
        for name in ['main-gate-checks.sh','pr-gate-checks.sh']:
            source = (HERE.parent.parent/name).read_text()
            statement = next(line.strip() for line in source.splitlines() if "printf '\\nGATE_FEATURES rc=" in line)
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); (root/'.gate-features.rc').write_text('0\n')
                for negative in [False,True]:
                    emit = 'echo "GATE_FEATURES rc=0"' if negative else statement
                    run = subprocess.run(['bash','-c','printf "%s" "$CAPTURED_LINT"; '+emit],
                                         env={**os.environ,'CAPTURED_LINT':lint,'gate_output':str(root)},capture_output=True,text=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertEqual('GATE_FEATURES rc=0' in run.stdout.splitlines(),not negative)


if __name__ == '__main__':
    unittest.main(verbosity=2)
