"""Codex: reused private artifacts remain exportable; source freshness stays strict."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import uuid

spec=importlib.util.spec_from_file_location('candidate_exports',Path(__file__).with_name('job_exports.cow-candidate.py'))
exports=importlib.util.module_from_spec(spec);spec.loader.exec_module(exports)


class TargetExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/mnt/bb/efficiency')
        self.base=Path(self.tmp.name);self.target=self.base/'target';self.target.mkdir()
        self.cwd=self.base/'src';self.cwd.mkdir()
        self.prefix=Path('/mnt/bb/jobs')/('efficiency-export-proof-'+uuid.uuid4().hex)
        Path(str(self.prefix)+'.t0').write_text('job start')
        self.spec={'prefix':str(self.prefix),'cwd':str(self.cwd),'build':False,'paths':[]}

    def tearDown(self):
        Path(str(self.prefix)+'.t0').unlink()
        shutil.rmtree(Path(str(self.prefix)+'.exports'),ignore_errors=True)
        self.tmp.cleanup()

    def run_export(self,paths,reused):
        exports.prepare({**self.spec,'paths':paths},str(self.target),cache_reused=reused)
        return json.loads(Path(str(self.prefix)+'.exports/manifest.json').read_text())

    def test_old_reflink_artifact_directory_and_old_shape_negative_control(self):
        (self.target/'debug').mkdir();binary=self.target/'debug/binary';binary.write_bytes(b'compiled')
        os.utime(binary,ns=(1,1))
        self.assertEqual([],self.run_export(['/work/target/debug'],False))
        result=self.run_export(['/work/target/debug'],True)
        self.assertEqual(1,result[0]['count']);self.assertTrue(result[0]['target'])

    def test_old_reflink_explicit_file_is_exported(self):
        binary=self.target/'binary';binary.write_bytes(b'compiled');os.utime(binary,ns=(1,1))
        self.assertEqual([],self.run_export(['/work/target/binary'],False))
        self.assertEqual(1,self.run_export(['/work/target/binary'],True)[0]['count'])

    def test_old_source_is_not_exported_due_to_target_reuse(self):
        source=self.cwd/'source';source.write_text('source');os.utime(source,ns=(1,1))
        self.assertEqual([],self.run_export(['source'],True))

    def test_target_reuse_does_not_allow_symlink_or_traversal(self):
        (self.target/'escape').symlink_to(self.cwd)
        (self.cwd/'source').write_text('outside the target')
        for path in ['/work/target/escape','/work/target/escape/source','/work/target/../source','/outside']:
            with self.assertRaises(ValueError):self.run_export([path],True)


if __name__=='__main__':unittest.main(verbosity=2)
