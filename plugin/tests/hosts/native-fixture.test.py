"""Capture actual child temp-directory selection; reject the previous env shape."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from native_fixture import fixture_environment


with tempfile.TemporaryDirectory(prefix="plugins-fixture-environment-") as temporary:
    root = Path(temporary)
    env = fixture_environment(root)
    command = [sys.executable, "-c", "import json,os,tempfile; print(json.dumps([tempfile.gettempdir(),os.environ.get('XDG_RUNTIME_DIR')]))"]

    def capture(environment):
        return json.loads(subprocess.check_output(command, env=environment, text=True))

    actual = capture(env)
    assert all(path and Path(path).is_relative_to(root) for path in actual), (
        "step=admit private fixture paths; recovery: python3 plugin/tests/hosts/native-fixture.test.py", actual)
    previous = {key: value for key, value in env.items() if key not in ("TMPDIR", "XDG_RUNTIME_DIR")}
    outside = capture(previous)
    assert not all(path and Path(path).is_relative_to(root) for path in outside), (
        "step=reject previous shared temporary paths; recovery: python3 plugin/tests/hosts/native-fixture.test.py", outside)
    print(json.dumps({"actual_child_paths": actual, "previous_child_paths": outside,
                      "previous_shape_rejected": True}))
