"""Captured RED main evidence and the real queue close both wake-up orders."""
import importlib.util
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


queue = load('queue_handoff', HERE/'gate-queue.py')
drain = load('drain_handoff', HERE/'drain.test.py')
CAPTURED = HERE/'fixtures/main-da7eee-red-features.txt'


class BaselineHandoffTests(unittest.TestCase):
    def test_publication_and_parking_orders_preserve_members_without_unchanged_retries(self):
        for before in [False, True]:
            with self.subTest(publication_before_parking=before), tempfile.TemporaryDirectory() as tmp:
                state = Path(tmp)
                observed = queue.baseline_generation(state)
                queue.add(state, 'pending', [9])
                if before:
                    self.assertTrue(queue.publish_baseline(state, 'captured-evidence'))
                destination = queue.park_baseline(state, observed, [1, 2])
                self.assertEqual(destination, 'pending' if before else 'baseline-pending')
                if not before:
                    self.assertTrue(queue.publish_baseline(state, 'captured-evidence'))
                self.assertEqual(queue.members(state/'pending.txt'), [1, 2, 9])
                self.assertEqual(queue.members(state/'baseline-pending.txt'), [])
                self.assertEqual(queue.baseline_generation(state), observed+1)
                self.assertEqual(queue.park_baseline(state, observed+1, [3]), 'baseline-pending')
                self.assertTrue(queue.publish_baseline(state, 'captured-evidence'),
                                'unchanged evidence still wakes unconsumed pending custody')
                with queue.locked(state):
                    claimed = queue.take(state, ['pending'])
                self.assertTrue(queue.publish_baseline(state, 'captured-evidence'),
                                'a dispatcher killed after claiming still leaves durable custody')
                with queue.locked(state):
                    queue.acknowledge(state, claimed)
                self.assertFalse(queue.publish_baseline(state, 'captured-evidence'))
                self.assertEqual(queue.members(state/'baseline-pending.txt'), [3])

    def test_failed_transfer_preserves_custody_and_does_not_publish_generation(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            queue.park_baseline(state, 0, [1])
            original = queue.publish
            def refuse_clear(path, prs):
                if path.name == 'baseline-pending.txt' and not prs:
                    raise OSError('clear refused')
                original(path, prs)
            with patch.object(queue, 'publish', side_effect=refuse_clear):
                with self.assertRaises(OSError):
                    queue.publish_baseline(state, 'captured-evidence')
            self.assertEqual(queue.baseline_generation(state), 0)
            self.assertEqual(queue.members(state/'pending.txt'), [1])
            self.assertEqual(queue.members(state/'baseline-pending.txt'), [1])

    def test_actual_main_handoff_accepts_red_and_refuses_incomplete_or_unchanged_evidence(self):
        main = (HERE/'main-gate.sh').read_text()
        # One function is called both on restart and after GREEN/RED promotion.
        marker = 'notify_baseline(){'
        block = main[main.index(marker):main.index('\n}\n', main.index(marker))+3]
        self.assertEqual(main.count('\n  notify_baseline\n'), 1)
        self.assertEqual(main.count('\n    notify_baseline\n'), 1)
        self.assertLess(main.index('PR baseline retained:'), main.rindex('\n    notify_baseline\n'))
        for negative, killed in [(False, False), (True, False), (False, True), (True, True)]:
            with self.subTest(former_behavior=negative, killed_before_dispatch=killed), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); repo = root/'repo'; repo.mkdir()
                env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
                def git(*args):
                    return subprocess.check_output(['git', '-C', str(repo), *args], env=env,
                                                   stderr=subprocess.DEVNULL, text=True).strip()
                git('init', '-q')
                git('-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                    'commit', '-q', '--allow-empty', '-m', 'captured RED output identity fixture')
                head = git('rev-parse', 'HEAD')
                logs = root/'logs'; (logs/'wide').mkdir(parents=True)
                log = logs/'wide'/(head+'.log')
                captured = CAPTURED.read_text().replace('da7eee5758388031eb27ec958582cab95bb78651', head)
                log.write_text(captured)
                # Phase evidence is captured separately in production too:
                # the older RED matrix log predates diagnostic receipts.
                # Reuse the real complete Clippy capture, never attest the
                # older log's unverified presentation in the test fixture.
                (logs/'clippy').mkdir()
                clippy_log = logs/'clippy'/(head+'.log')
                clippy_log.write_text((HERE/'fixtures/main-32b799-complete-clippy.txt').read_text().replace(
                    '32b79969bc504e875bb6e9f65611bf0d02ea45e9', head))
                alerts = root/'alerts'; alerts.write_text(f'MAIN_WIDE RED {head}: captured\n')
                state = root/'state'; queue.park_baseline(state, 0, [1])
                selected_queue = HERE/'gate-queue.py'
                if negative and killed:
                    selected_queue = root/'old-queue.py'
                    source = (HERE/'gate-queue.py').read_text()
                    old = source.replace('if previous == identity:\n            return bool(members(pending) or members(active))',
                                         'if previous == identity:\n            return False')
                    self.assertNotEqual(old, source)
                    selected_queue.write_text(old)
                wake = root/'wake.sh'
                wake.write_text(f'echo WAKE >> "{root / "wakes"}"\n'
                    f'claimed=$({shlex.quote(sys.executable)} "{HERE / "gate-queue.py"}" "{state}" take pending) || exit $?\n'
                    f'{shlex.quote(sys.executable)} "{HERE / "gate-queue.py"}" "{state}" ack active $claimed\n')
                selected = block.replace('python Q:/lanes/gate-baseline.py',
                    f'{shlex.quote(sys.executable)} {shlex.quote(str(HERE / "gate-baseline.py"))} --directory "{logs}" --alerts "{alerts}"')
                selected = selected.replace('python Q:/lanes/gate-queue.py Q:/lanes/pr-gate',
                    f'{shlex.quote(sys.executable)} {shlex.quote(str(selected_queue))} "{state}"')
                selected = selected.replace('bash Q:/lanes/gate-drain.sh', f'bash "{wake}"').replace(
                    'Q:/lanes/pr-gate/baseline-resume.out', f'"{root / "resume.out"}"')
                boundary = 'elif [ "$dispatch_needed" = 1 ]; then'
                self.assertIn(boundary, selected)
                selected = selected.replace(boundary, boundary+'\n'
                    '    if [ "${KILL_BEFORE_DISPATCH:-}" = 1 ]; then echo BASELINE_COMMITTED; read -r release; fi')
                selected += '\nif [ "$rc" = 0 ]; then notify_baseline; fi\n' if negative and not killed else '\nnotify_baseline\n'
                script = f'W="{repo}"; tip={head}; log="{log}"; rc=1\n'+selected+'\nwait\n'
                def run():
                    result = subprocess.run(['bash', '-c', script], capture_output=True, text=True, env=env)
                    self.assertEqual(result.returncode, 0, result.stderr)
                if killed:
                    child = subprocess.Popen(['bash', '-c', script], stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                        env={**env, 'KILL_BEFORE_DISPATCH':'1'})
                    try:
                        self.assertEqual(child.stdout.readline().strip(), 'BASELINE_COMMITTED')
                        self.assertEqual(queue.baseline_generation(state), 1)
                        self.assertEqual(queue.members(state/'pending.txt'), [1])
                        self.assertFalse((root/'wakes').exists())
                    finally:
                        child.kill(); child.communicate()
                    self.assertEqual(child.returncode, -9)
                run()
                self.assertEqual(queue.baseline_generation(state), 0 if negative and not killed else 1)
                self.assertEqual(queue.members(state/'pending.txt'), [1] if negative and killed else [])
                if negative:
                    self.assertFalse((root/'wakes').exists(), 'former code loses the wake-up')
                    continue
                queue.park_baseline(state, 1, [2])
                run()
                self.assertEqual(queue.members(state/'baseline-pending.txt'), [2])
                self.assertEqual((root/'wakes').read_text().split(), ['WAKE'])
                log.write_text(captured.replace('GATE_EXECUTOR rc=1', ''))
                run()
                self.assertEqual(queue.baseline_generation(state), 1)
                self.assertEqual(queue.members(state/'baseline-pending.txt'), [2])

    def test_main_completion_before_parking_resumes_direct_and_bisect_paths(self):
        probe = drain.DispatchTests()
        for bisect, negative in [(False, False), (True, False), (False, True)]:
            with self.subTest(bisect=bisect, former_parking=negative):
                body = ('if [ "$*" = "1 2 3" ]; then exit 1; fi\n' if bisect else '')
                body += f'''if [ ! -f "$GATE_STATE/published" ]; then
touch "$GATE_STATE/published"
python "$GATE_LANES/gate-queue.py" "$GATE_STATE" baseline-ready da7eee {shlex.quote(str(CAPTURED))}
exit 5
fi
exit 0
'''
                values, output = probe.run_drain(body, pending='1\n2\n3\n' if bisect else '1\n',
                                                 negative_baseline=negative)
                expected = ['1 2 3', '1 2', '3', '1 2'] if bisect else (['1'] if negative else ['1', '1'])
                self.assertEqual(values['runs'].splitlines(), expected, output)
                self.assertEqual(values['baseline-pending.txt'].split(), ['1'] if negative else [])
                if not negative:
                    self.assertIn('1 2' if bisect else '1', values['merged'].splitlines())


if __name__ == '__main__':
    unittest.main(verbosity=2)
