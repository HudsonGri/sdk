"""The Windows helper owns argv, never a transient worktree input pathname."""
import ast
import errno
import json
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


def load_helper():
    source = (Path(__file__).resolve().parents[1] / 'windows-test.sh').read_text()
    source = source.split("<<'WIN_TEST_PY'\n", 1)[1].rsplit('\nWIN_TEST_PY', 1)[0]
    module = ast.parse(source)
    assert isinstance(module.body[-1], ast.Try)
    # Load the real factories without launching Windows main on the build box.
    module.body.pop()
    namespace = {'__name__': 'windows_input_fixture'}
    exec(compile(module, 'windows-test.sh', 'exec'), namespace)
    return namespace


def consumer_command(command):
    # SDK preparation belongs to the public wrapper. These input-ownership
    # controls execute its exact consumer without preparing a compiler SDK.
    assert command[:5] == ['python3', 'scripts/with-xwin-cache.py', '--workers', '1', '--']
    return command[5:]


class ImmutableWindowsInputTests(unittest.TestCase):
    def test_production_fixture_is_staged_before_test_feature_graph_overwrites_output(self):
        helper = load_helper()
        producer = helper['REMOTE'].split('files={}\n', 1)[0]
        stager = Path(__file__).resolve().parents[1] / 'graphcoder-e2e' / 'artifacts.mjs'
        for selection, expects_fixture in [(['--test', 'cli'], True), (['--lib'], False)]:
            with self.subTest(selection=selection), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                script = root / 'scripts' / 'graphcoder-e2e' / 'artifacts.mjs'
                script.parent.mkdir(parents=True)
                shutil.copyfile(stager, script)
                output = root / 'cargo-output'
                output.mkdir()
                binary = output / 'host.exe'
                driver = output / 'cli.exe'
                metadata = {'packages': [{'name': 'host', 'id': 'host-id',
                            'targets': [{'name': 'host', 'kind': ['bin']}]}]}
                commands = []
                real_popen = subprocess.Popen

                def cargo(command, **kwargs):
                    if command[0] != 'cargo':
                        return real_popen(command, **kwargs)
                    commands.append(command)
                    test = command[2] == 'test'
                    binary.write_bytes(b'test feature graph' if test else b'production feature graph')
                    records = [dict(reason='compiler-artifact', package_id='host-id',
                                    target={'name': 'host', 'kind': ['bin']},
                                    profile={'test': False}, executable=str(binary))]
                    if test:
                        driver.write_bytes(b'test driver')
                        records.append(dict(reason='compiler-artifact', package_id='host-id',
                                            target={'name': 'cli', 'kind': ['test']},
                                            profile={'test': True}, executable=str(driver)))
                    return mock.Mock(stdout=io.StringIO(''.join(json.dumps(row)+'\n' for row in records)),
                                     wait=lambda: 0)

                previous = Path.cwd()
                try:
                    os.chdir(root)
                    with mock.patch.dict(os.environ, {'GATE_XWIN_CACHE_ROOT': directory,
                                          'XWIN_CACHE_DIR': directory, 'CARGO_TARGET_DIR': str(output)}), \
                         mock.patch.object(subprocess, 'check_output', return_value=json.dumps(metadata)), \
                         mock.patch.object(subprocess, 'Popen', side_effect=cargo):
                        scope = {'CONFIG': {'crate': 'host', 'stem': 'fixture', 'cargo': selection}}
                        exec(compile(producer, 'actual-windows-producer', 'exec'), scope)
                finally:
                    os.chdir(previous)
                rows = scope['rows']
                self.assertEqual([row['test'] for row in rows], [False, True] if expects_fixture else [True])
                self.assertEqual([command[2] for command in commands], ['build', 'test'] if expects_fixture else ['test'])
                if expects_fixture:
                    self.assertEqual(Path(rows[0]['remote']).read_bytes(), b'production feature graph')
                    # Former selection after cargo test takes the overwritten
                    # normal-bin path from the test dependency feature graph.
                    self.assertEqual(binary.read_bytes(), b'test feature graph')
                    self.assertNotEqual(Path(rows[0]['remote']).read_bytes(), binary.read_bytes())

    def test_fixture_binding_uses_admitted_identity_for_every_binary_target(self):
        helper = load_helper()['runner_module'](Path(__file__).resolve().parents[1] / 'windows-runner.py').__dict__
        with tempfile.TemporaryDirectory() as directory:
            records = Path(directory)
            binary = records / 'bin' / 'immutable-export.exe'
            binary.parent.mkdir()
            binary.write_bytes(b'admitted fixture')
            test = {'target': 'cli', 'test': True, 'name': 'driver.exe'}
            for target in ['acyclic', 'graphcoder', 'another-host']:
                row = {'target': target, 'test': False, 'name': binary.name}
                manifest = {'artifacts': [test, row]}
                self.assertEqual(helper['fixture_binary'](records, manifest), binary.resolve())
                if target != 'acyclic':
                    old = next((item for item in manifest['artifacts'] if not item['test'] and item['target'] == 'acyclic'), None)
                    self.assertIsNone(old, 'the former target-specific selector drops this admitted fixture')
            self.assertIsNone(helper['fixture_binary'](records, {'artifacts': [test]}))
            with self.assertRaisesRegex(RuntimeError, 'Ambiguous normal executable fixture'):
                helper['fixture_binary'](records, {'artifacts': [test, row, dict(row, target='second')]})
            binary.unlink()
            with self.assertRaisesRegex(RuntimeError, 'Missing admitted normal executable fixture'):
                helper['fixture_binary'](records, manifest)

    def test_payload_survives_producer_cleanup_and_owns_exact_config(self):
        helper = load_helper()
        helper['REMOTE'] = 'import json; print(json.dumps(CONFIG, ensure_ascii=False))'
        config = {'value': "quotes '\"; $() ` newline\n Unicode \u25cb", 'fixtures': ['a b']}
        expected = json.loads(json.dumps(config))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helper['INPUT_ROOT'] = str(root)
            command, program = helper['remote_invocation'](config)
            staged = subprocess.run(helper['stage_invocation'](command), input=program, capture_output=True)
            self.assertEqual(staged.returncode, 0, staged.stderr)
            old_input = root / 'transient-job.py'
            old_input.write_bytes(program)
            old_command = [sys.executable, str(old_input)]
            old_input.unlink()
            # Former pathname ownership loses the program at producer cleanup.
            broken = subprocess.run(old_command, cwd=root, capture_output=True)
            self.assertNotEqual(broken.returncode, 0)
            self.assertIn(b'No such file or directory', broken.stderr)
            config['value'] = 'changed after dispatch'
            helper['REMOTE'] = 'raise RuntimeError("changed after dispatch")'
            result = subprocess.run(consumer_command(command), cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), expected)
            self.assertEqual(list(root.iterdir()), [])



    def test_program_size_never_enters_executor_argv_and_forged_input_is_refused(self):
        helper = load_helper()
        helper['REMOTE'] = '#' * (1024 * 1024) + '\nprint(CONFIG["value"])'
        with tempfile.TemporaryDirectory() as directory:
            helper['INPUT_ROOT'] = directory
            command, program = helper['remote_invocation']({'value': 'owned'})
            # Restore the former full-program argument: Linux refuses its size.
            with self.assertRaises(OSError) as failure:
                subprocess.run([sys.executable, '-c', program.decode()], capture_output=True)
            self.assertEqual(failure.exception.errno, errno.E2BIG)
            self.assertLess(sum(map(len, command)), 1024)
            staged = subprocess.run(helper['stage_invocation'](command), input=program, capture_output=True)
            self.assertEqual(staged.returncode, 0, staged.stderr)
            result = subprocess.run(consumer_command(command), capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), 'owned')
            self.assertEqual(list(Path(directory).iterdir()), [])
            staged = subprocess.run(helper['stage_invocation'](command), input=program, capture_output=True)
            self.assertEqual(staged.returncode, 0, staged.stderr)
            owned = Path(command[-2])
            owned.chmod(0o644)
            owned.write_text('raise RuntimeError("forged")')
            refused = subprocess.run(consumer_command(command), capture_output=True, text=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn('Windows input digest mismatch', refused.stderr)

if __name__ == '__main__':
    unittest.main()
