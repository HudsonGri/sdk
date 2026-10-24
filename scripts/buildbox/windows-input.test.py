"""The Windows helper owns argv, never a transient worktree input pathname."""
import ast
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


def load_helper():
    source = (Path(__file__).parent / 'win-test.sh').read_text()
    source = source.split("<<'WIN_TEST_PY'\n", 1)[1].rsplit('\nWIN_TEST_PY', 1)[0]
    module = ast.parse(source)
    assert isinstance(module.body[-1], ast.Try)
    # Load the real factories without launching Windows main on the build box.
    module.body.pop()
    namespace = {'__name__': 'windows_input_fixture'}
    exec(compile(module, 'win-test.sh', 'exec'), namespace)
    return namespace


class ImmutableWindowsInputTests(unittest.TestCase):
    def test_payload_survives_producer_cleanup_and_owns_exact_config(self):
        helper = load_helper()
        helper['REMOTE'] = 'import json; print(json.dumps(CONFIG, ensure_ascii=False))'
        config = {'value': "quotes '\"; $() ` newline\n Unicode \u25cb", 'fixtures': ['a b']}
        expected = json.loads(json.dumps(config))
        command = helper['remote_invocation'](config)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_input = root / 'transient-job.py'
            old_input.write_text(command[-1])
            old_command = [sys.executable, str(old_input)]
            old_input.unlink()
            # Former pathname ownership loses the program at producer cleanup.
            broken = subprocess.run(old_command, cwd=root, capture_output=True)
            self.assertNotEqual(broken.returncode, 0)
            self.assertIn(b'No such file or directory', broken.stderr)
            config['value'] = 'changed after dispatch'
            helper['REMOTE'] = 'raise RuntimeError("changed after dispatch")'
            result = subprocess.run(command, cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), expected)
            self.assertEqual(list(root.iterdir()), [])


if __name__ == '__main__':
    unittest.main()
