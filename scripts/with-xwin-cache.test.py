import importlib.util
import inspect
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import fcntl

spec = importlib.util.spec_from_file_location("xwin_cache", Path(__file__).with_name("with-xwin-cache.py"))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)
run = subprocess.run


def prepared(command, **kwargs):
    if command[:3] == ["cargo", "xwin", "cache"]:
        return subprocess.CompletedProcess(command, 0)
    return run(command, **kwargs)


class SnapshotTests(unittest.TestCase):
    def seed(self, root):
        seed = root / "seed"
        for name in ("crt", "sdk"):
            (seed / "xwin" / name).mkdir(parents=True)
            (seed / "xwin" / name / "data").write_text(name)
        (seed / "xwin" / "DONE").write_text("prepared")
        return seed

    def test_workers_own_writes_deletes_and_release_the_seed_before_compilation(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(cache.subprocess, "run", prepared):
            root = Path(temporary)
            seed = self.seed(root)
            first, second = root / "first", root / "second"
            cache.snapshot(seed, first)
            cache.snapshot(seed, second)
            paths = [entry / "xwin" / "crt" / "data" for entry in (seed, first, second)]
            self.assertEqual(len({path.stat().st_ino for path in paths}), 3)
            paths[1].write_text("first owns this write")
            self.assertEqual(paths[0].read_text(), "crt")
            self.assertEqual(paths[2].read_text(), "crt")
            paths[1].unlink()
            self.assertTrue(paths[0].exists() and paths[2].exists())
            with (seed / ".win-test.lock").open("a") as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_escaping_sdk_alias_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(cache.subprocess, "run", prepared):
            root = Path(temporary)
            seed = self.seed(root)
            outside = root / "outside"
            outside.write_text("outside snapshot")
            (seed / "xwin" / "sdk" / "alias").symlink_to(outside)
            with self.assertRaisesRegex(RuntimeError, "SDK alias escapes"):
                cache.snapshot(seed, root / "worker")

    def test_seed_preparation_keeps_payload_stdout_machine_readable(self):
        source = inspect.getsource(cache.snapshot)
        former = source.replace("stdout=sys.stderr, ", "")
        self.assertNotEqual(source, former)
        namespace = dict(cache.__dict__)
        exec(compile(former, "<former-sdk-stdout-control>", "exec"), namespace)
        for factory, protected in [(cache.snapshot, True), (namespace["snapshot"], False)]:
            with self.subTest(protected=protected), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                seed = self.seed(root)
                with (root / "stdout").open("w+") as machine, (root / "stderr").open("w+") as diagnostics:
                    def noisy_preparation(command, **kwargs):
                        if command[:3] == ["cargo", "xwin", "cache"]:
                            kwargs.setdefault("stdout", machine)
                            return run([sys.executable, "-c", "print('SDK preparation status')"], **kwargs)
                        return run(command, **kwargs)

                    with patch.object(cache.subprocess, "run", noisy_preparation), patch.object(cache.sys, "stderr", diagnostics):
                        factory(seed, root / "worker")
                    run([sys.executable, "-c", "print('{\"reason\":\"compiler-artifact\"}')"], stdout=machine, check=True)
                    machine.seek(0)
                    output = machine.read()
                    diagnostics.seek(0)
                    status = diagnostics.read()
                if protected:
                    self.assertEqual(json.loads(output), {"reason": "compiler-artifact"})
                    self.assertIn("SDK preparation status", status)
                else:
                    with self.assertRaises(json.JSONDecodeError):
                        json.loads(output)
                    self.assertIn("SDK preparation status", output)


if __name__ == "__main__":
    unittest.main()
