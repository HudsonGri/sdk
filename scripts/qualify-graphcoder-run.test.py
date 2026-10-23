import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class QualificationPaths(unittest.TestCase):
    def test_cargo_default_and_explicit_target_directories(self):
        main = runpy.run_path(str(Path(__file__).with_name("qualify-graphcoder-run.py")))["main"]
        for configured in [None, "relative-target", str(Path.cwd() / "absolute-target")]:
            with self.subTest(target=configured), tempfile.TemporaryDirectory() as output:
                env = {"E2E_OUTPUT": output}
                if configured is not None:
                    env["CARGO_TARGET_DIR"] = configured
                with patch.dict(os.environ, env, clear=True), patch("subprocess.run") as run:
                    run.return_value = subprocess.CompletedProcess([], 1)
                    self.assertEqual(main(), 1)
                    target = Path(configured or "target") / "debug"
                    self.assertEqual(run.call_args.args[0], [
                        "node", "scripts/graphcoder-e2e/run.mjs",
                        str(target / "graphcoder"), str(target / "harness-mock-model"),
                    ])
                    self.assertEqual(json.loads((Path(output) / "wall.json").read_text())["exit"], 1)
                    if configured is None:
                        # The former mandatory lookup fails under the same environment.
                        with self.assertRaises(KeyError):
                            os.environ["CARGO_TARGET_DIR"]


if __name__ == "__main__":
    unittest.main()
