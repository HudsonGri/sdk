"""Actual coordinator dispatch with recorded outputs and the real selector.

Git identities alone are mapped onto a disposable real Git repository. The
recording transport replays captured candidate phase output; it never builds.
"""
import importlib.util
from hashlib import sha256
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
FIXTURE = HERE/'fixtures/baseline-admission'
BASE = 'b7333953cbd5d3b31adc05cf2f0851f049f6c16a'
spec = importlib.util.spec_from_file_location('admission_queue', HERE/'gate-queue.py')
queue = importlib.util.module_from_spec(spec); spec.loader.exec_module(queue)


class BaselineAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        origin = json.loads((FIXTURE/'origin.json').read_text())
        for record in origin['files']:
            data = (FIXTURE/record['file']).read_bytes()
            if len(data) != record['bytes'] or sha256(data).hexdigest() != record['sha256']:
                raise AssertionError('captured baseline evidence changed: '+record['file'])
        if sha256((FIXTURE/'former-pr-gate.sh.txt').read_bytes()).hexdigest() != origin['former_sha256']:
            raise AssertionError('negative control must execute the pinned deployed coordinator')
        prior = json.loads((HERE/'fixtures/baseline-ready-before.origin.json').read_text())
        if sha256((HERE/'fixtures/baseline-ready-before.py.txt').read_bytes()).hexdigest() != prior['sha256']:
            raise AssertionError('publication negative must execute the pinned committed queue')

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root/'repo'; self.repo.mkdir()
        self.env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
        subprocess.run(['git','init','-q',str(self.repo)], env=self.env, check=True)
        subprocess.run(['git','-C',str(self.repo),'-c','commit.gpgsign=false',
                        '-c','user.name=Fixture','-c','user.email=fixture@example.invalid',
                        'commit','-q','--allow-empty','-m','captured phase identity'],
                       env=self.env, check=True)
        self.head = subprocess.check_output(['git','-C',str(self.repo),'rev-parse','HEAD'],
                                            env=self.env, text=True).strip()
        self.logs = self.root/'logs'; (self.logs/'wide').mkdir(parents=True)
        (self.logs/'clippy').mkdir()
        self.alerts = self.root/'alerts'
        self.alerts.write_text((FIXTURE/'main-alert.txt').read_text().replace(BASE,self.head))
        (self.root/'.codex-fleet').mkdir()
        self.lanes = self.root/'lanes'; self.lanes.mkdir()
        for name in ['gate-checks.sh','gate-test-runner.sh','gate-clippy.py']:
            original = HERE.parent.parent/name if name == 'gate-test-runner.sh' else HERE/name
            shutil.copyfile(original,self.lanes/name)
        for name in ['input_closure.py','admission_policy.py']:
            shutil.copyfile(HERE.parent/name,self.lanes/name)
        (self.repo/'scripts').mkdir()
        shutil.copyfile(HERE.parent.parent/'pr-gate-checks.sh',self.repo/'scripts/pr-gate-checks.sh')
        scope = json.loads((FIXTURE/'candidate-phases.txt').read_text().splitlines()[0].removeprefix('GATE_SCOPE '))
        (self.repo/'.gate-selection.json').write_text(json.dumps(scope))
        (self.repo/'.gate-scope-mode').write_text('affected\n')
        for name in ['tests','clippy']:
            (self.repo/f'.gate-{name}-packages').write_text('acyclic-fs\n')
        self.payload = self.root/'payload.txt'
        self.payload.write_text((FIXTURE/'candidate-phases.txt').read_text())
        self.bin = self.root/'bin'; self.bin.mkdir()
        transport = self.bin/'pwsh'
        transport.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$RECORD_TRANSPORT"\ncat "$RECORDED_PAYLOAD"\n')
        transport.chmod(0o755)
        self.state = self.root/'queue'

    def complete(self):
        (self.logs/(self.head+'.log')).write_text((FIXTURE/'main-phases.txt').read_text().replace(BASE,self.head))
        (self.logs/'clippy'/(self.head+'.log')).write_text((FIXTURE/'main-clippy.txt').read_text().replace(BASE,self.head))

    def publish(self, head=None, paths=None, script=None):
        return subprocess.run([sys.executable, '-B', str(script or HERE/'gate-queue.py'),
            str(self.state), 'baseline-ready', head or self.head,
            *map(str, paths if paths is not None else [self.logs/'clippy'/(self.head+'.log'),
                                                       self.logs/(self.head+'.log')])],
            capture_output=True, text=True)

    def test_publication_boundary_rejects_unqualified_captures_without_queue_changes(self):
        queue.park_baseline(self.state, 0, [1495])
        for mutation in ['missing', 'partial', 'wrong-head', 'missing-source-proof',
                         'damaged-clippy', 'infra', 'one-phase', 'short-head']:
            with self.subTest(mutation=mutation):
                self.complete()
                lint = self.logs/'clippy'/(self.head+'.log')
                native = self.logs/(self.head+'.log')
                paths = [lint, native]
                head = self.head
                if mutation == 'missing':
                    lint.unlink()
                elif mutation == 'partial':
                    native.write_text(native.read_text().split('GATE_DONE rc=')[0])
                elif mutation == 'wrong-head':
                    native.write_text(native.read_text().replace(self.head, '0'*40))
                elif mutation == 'missing-source-proof':
                    native.write_text(native.read_text().split('GATE_MAIN_VERIFIED ')[0])
                elif mutation == 'damaged-clippy':
                    lint.write_bytes(native.read_bytes())
                elif mutation == 'infra':
                    native.write_text(native.read_text()+'error: GATE_RUNNER_INFRA export failed\n')
                elif mutation == 'one-phase':
                    paths = [lint]
                else:
                    head = self.head[:7]
                result = self.publish(head, paths)
                self.assertEqual(result.returncode, 6, result.stdout+result.stderr)
                self.assertIn('preparation-INFRA', result.stderr)
                self.assertEqual(queue.baseline_generation(self.state), 0)
                self.assertEqual(queue.members(self.state/'pending.txt'), [])
                self.assertEqual(queue.members(self.state/'baseline-pending.txt'), [1495])

    def test_committed_publication_accepts_actual_interrupted_capture(self):
        self.complete()
        native = self.logs/(self.head+'.log')
        native.write_text(native.read_text().split('GATE_DONE rc=')[0])
        queue.park_baseline(self.state, 0, [1495])
        result = self.publish()
        self.assertEqual(result.returncode, 6, result.stderr)
        self.assertEqual(queue.baseline_generation(self.state), 0)
        prior = self.publish(script=HERE/'fixtures/baseline-ready-before.py.txt')
        self.assertEqual((prior.returncode, prior.stdout.strip()), (0, '1'), prior.stderr)
        self.assertEqual(queue.baseline_generation(self.state), 1)
        self.assertEqual(queue.members(self.state/'pending.txt'), [1495])

    def run_gate(self, former=False):
        source = (FIXTURE/'former-pr-gate.sh.txt' if former else HERE/'pr-gate.sh').read_text()
        # Run the actual production staging, transport, executor/phase checks
        # and baseline block in their actual order. Preparation is already
        # represented by the captured selection and real fixture repository.
        begin = 'cp "$W/.gate-selection.json" "$log.scope.json"'
        code = source[source.index(begin):source.index('test_base_log=""')]
        self.assertEqual(code.count('if ! base_log=$('),1)
        selector = (f'{shlex.quote(sys.executable)} {shlex.quote(str(HERE/"gate-baseline.py"))}'
                    f' --directory {shlex.quote(str(self.logs))} --alerts {shlex.quote(str(self.alerts))}')
        code = code.replace('python Q:/lanes/gate-baseline.py',selector)
        code = code.replace('python Q:/lanes/input_closure.py',shlex.quote(sys.executable)+' '+shlex.quote(str(self.lanes/'input_closure.py')))
        code = code.replace('Q:/lanes/',str(self.lanes)+'/')
        log = self.root/'candidate.log'
        log.write_text('')
        header = f'W={shlex.quote(str(self.repo))}; L={shlex.quote(str(self.root))}; base={self.head}; n=1495; prs=1495; log={shlex.quote(str(log))}; gate_started=$SECONDS; preparation_seconds=0; box_env=(); skips=""\n'
        result = subprocess.run(['bash','-c',header+code],cwd=self.repo,capture_output=True,text=True,
            env={**self.env,'HOME':str(self.root),'PATH':str(self.bin)+os.pathsep+self.env['PATH'],
                 'RECORD_TRANSPORT':str(self.root/'transport'),'RECORDED_PAYLOAD':str(self.payload)})
        count = len((self.root/'transport').read_text().splitlines()) if (self.root/'transport').exists() else 0
        return result,count

    def test_baseline_missing_never_dispatches(self):
        self.assertIn('no complete exact-base main tests baseline for '+BASE,
                      (FIXTURE/'tests-selection.txt').read_text())
        for former in [False,True]:
            result,count = self.run_gate(former)
            self.assertEqual(result.returncode,5,result.stdout+result.stderr)
            self.assertEqual(count,int(former),'captured old production dispatches before refusing the missing prerequisite')
            self.assertIn('no complete exact-base main tests baseline for '+self.head,
                          (self.root/'candidate.log.baseline-error').read_text())

    def test_completed_exact_base_dispatches_once(self):
        self.complete()
        result,count = self.run_gate()
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual(count,1)
        self.assertIn('GATE_CHECKS_BASELINE clippy=',(self.root/'candidate.log').read_text())
        self.assertIn('--env BUILD_SLOT_PR=1495',(self.root/'transport').read_text())

    def test_partial_or_wrong_base_never_dispatches(self):
        self.complete()
        exact = self.logs/(self.head+'.log')
        saved = exact.read_text()
        for mutation in ['missing-executor','wrong-source','damaged-clippy']:
            with self.subTest(mutation=mutation):
                self.complete()
                if mutation == 'missing-executor':
                    # A test-derived prefix models interruption. No completion
                    # markers are manufactured and the original stays intact.
                    exact.write_text(saved.split('GATE_DONE rc=')[0])
                    (self.logs/'clippy'/(self.head+'.log')).unlink()
                elif mutation == 'wrong-source':
                    exact.write_text(saved.replace('"head": "'+self.head+'"','"head": "'+('0'*40)+'"'))
                    (self.logs/'clippy'/(self.head+'.log')).unlink()
                else:
                    # The real GREEN main capture contains a truncated legacy
                    # Clippy stream; env/phase success cannot repair it.
                    (self.logs/'clippy'/(self.head+'.log')).unlink()
                result,count = self.run_gate()
                self.assertEqual(result.returncode,5,result.stdout+result.stderr)
                self.assertEqual(count,0)

    def test_unchanged_prerequisite_stays_parked(self):
        observed = queue.baseline_generation(self.state)
        result,count = self.run_gate()
        self.assertEqual((result.returncode,count),(5,0))
        self.assertEqual(queue.park_baseline(self.state,observed,[1495]),'baseline-pending')
        self.assertEqual(queue.members(self.state/'pending.txt'),[])
        self.assertEqual(queue.members(self.state/'baseline-pending.txt'),[1495])
        self.assertEqual(queue.baseline_generation(self.state),observed)

    def test_verified_publication_wakes_existing_request_once(self):
        queue.park_baseline(self.state,0,[1495])
        self.complete()
        # These are the real evidence identities consumed by the one queue
        # authority, after the actual selector admits the candidate.
        published = self.publish()
        self.assertEqual((published.returncode,published.stdout.strip()),(0,'1'),published.stderr)
        with queue.locked(self.state):
            claimed = queue.take(self.state,['pending'])
        self.assertEqual(claimed,[1495])
        result,count = self.run_gate()
        self.assertEqual((result.returncode,count),(0,1),result.stdout+result.stderr)
        with queue.locked(self.state):
            queue.acknowledge(self.state,claimed)
        published = self.publish()
        self.assertEqual((published.returncode,published.stdout.strip()),(0,'0'),published.stderr)
        self.assertEqual(queue.members(self.state/'baseline-pending.txt'),[])
        # A second parked scope cannot borrow a consumed wake-up just because
        # the coordinator polls the unchanged evidence again.
        queue.park_baseline(self.state,queue.baseline_generation(self.state),[1504])
        published = self.publish()
        self.assertEqual((published.returncode,published.stdout.strip()),(0,'0'),published.stderr)
        self.assertEqual(queue.members(self.state/'baseline-pending.txt'),[1504])


if __name__ == '__main__':
    unittest.main(verbosity=2)
