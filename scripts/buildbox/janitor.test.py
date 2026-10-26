import importlib.util
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

spec = importlib.util.spec_from_file_location('bb_janitor', Path(__file__).with_name('bb-janitor.py'))
janitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(janitor)


def write(path, size, age):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'x' * size)
    stamp = time.time() - age
    os.utime(path, (stamp, stamp))


class Janitor(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: janitor.shutil.rmtree(self.root, ignore_errors=True))
        patches = {'LOG': self.root / 'janitor.log', 'SCCACHE': self.root / 'sccache', 'JOBS': self.root / 'jobs'}
        for name, value in patches.items():
            patcher = mock.patch.object(janitor, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_sccache_evicts_least_recently_used_to_budget(self):
        for index, age in enumerate([500, 400, 300, 200, 100]):
            write(self.root / 'sccache' / str(index % 2) / f'entry{index}', 10, age)
        with mock.patch.object(janitor, 'SCCACHE_BUDGET', 25):
            janitor.evict_sccache()
        left = sorted(p.name for p in (self.root / 'sccache').rglob('entry*'))
        self.assertEqual(left, ['entry3', 'entry4'])  # The two most recent; 20 <= 25.

    def test_jobs_keep_live_and_recent_entries(self):
        hour = 3600
        stale, live, recent = (self.root / 'jobs' / name for name in ('job-stale', 'job-live', 'job-recent'))
        write(stale / 'out', 1, 7 * hour)
        write(live / 'sccache.sock', 1, 7 * hour)
        write(recent / 'out', 1, 60)
        for path, age in ((stale, 7 * hour), (live, 7 * hour), (recent, 60)):
            os.utime(path, (time.time() - age, time.time() - age))
        with mock.patch.object(janitor, 'live_references', return_value={str(live / 'sccache.sock')}):
            janitor.clean_jobs()
        self.assertEqual(sorted(p.name for p in (self.root / 'jobs').iterdir()), ['job-live', 'job-recent'])


if __name__ == '__main__':
    unittest.main()
