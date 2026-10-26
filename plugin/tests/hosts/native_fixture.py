"""One private, credential-free environment for scripted native host fixtures."""
import os
from pathlib import Path


def fixture_environment(root, variables=None):
    env = {"PATH": os.environ["PATH"]}
    for key in ("HOME", "USERPROFILE", "CODEX_HOME", "CLAUDE_CONFIG_DIR",
                "APPDATA", "LOCALAPPDATA", "XDG_STATE_HOME", "XDG_CONFIG_HOME",
                "XDG_DATA_HOME", "XDG_RUNTIME_DIR", "TMPDIR"):
        directory = Path(root) / key.lower()
        directory.mkdir(mode=0o700)
        env[key] = str(directory)
    if variables:
        env.update(variables)
    return env
