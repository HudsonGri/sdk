import fcntl
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('pruner',HERE/'prune-wt-sdk.py')
pruner=importlib.util.module_from_spec(spec);spec.loader.exec_module(pruner)


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(dir='/mnt/bb/tmp')
        self.d=Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_prep_files_share_age_count_and_source_lease_with_directories(self):
        root=self.d/'preparations';root.mkdir();locks=self.d/'locks';locks.mkdir()
        mirror=self.d/'mirror';now=100000
        for name in ['old.prep','leased.prep','fresh.prep','active.prep']:
            (root/name).write_text('preparation marker')
            os.utime(root/name,(now-50000,)*2)
        os.utime(root/'fresh.prep',(now,)*2)
        (root/'leased').mkdir();os.utime(root/'leased',(now-50000,)*2)
        outside=self.d/'outside';outside.write_text('must survive')
        (root/'link.prep').symlink_to(outside)
        with (locks/'leased.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            result=pruner.prune(root,mirror,locks,active={str(root/'active.prep')},now=now)
            self.assertEqual(result['removed'],['old'])
            self.assertEqual(set(result['held']),{'leased','active'})
            self.assertTrue((root/'leased').is_dir());self.assertTrue((root/'leased.prep').is_file())
            self.assertTrue((root/'fresh.prep').is_file());self.assertEqual(outside.read_text(),'must survive')
        result=pruner.prune(root,mirror,locks,active=set(),now=now,max_count=0)
        self.assertEqual(set(result['removed']),{'leased','fresh','active'})
        self.assertEqual([p.name for p in root.iterdir()],['link.prep'])
        # Actual deployed source, captured before this change, leaks old markers.
        (root/'old.prep').write_text('abandoned')
        old={'__name__':'captured_deployed_pruner'}
        exec(compile((HERE/'fixtures/prep-retirement/deployed-pruner.py.txt').read_text(),
                     'actual-deployed-pruner','exec'),old)
        self.assertEqual(old['prune'](root,mirror,locks,active=set(),now=now,max_count=0)['remaining'],0)
        self.assertTrue((root/'old.prep').exists())

    def test_failed_prep_unlink_reuses_tomb_and_preserves_peer_cleanup(self):
        root=self.d/'markers';root.mkdir();locks=self.d/'locks';mirror=self.d/'mirror'
        for name in ['a.prep','b.prep']:(root/name).write_text(name)
        remove=pruner.remove_tree
        def retire(path):
            if path.name=='.prune-a.prep':raise PermissionError('marker ownership control')
            remove(path)
        for _ in range(2):
            with patch.object(pruner,'remove_tree',side_effect=retire):
                result=pruner.prune(root,mirror,locks,max_count=0,active=set())
            self.assertEqual(result['errors'][0]['source'],'a')
            self.assertEqual([p.name for p in root.iterdir()],['.prune-a.prep'])
        self.assertEqual(pruner.prune(root,mirror,locks,max_count=0,active=set())['removed'],['a'])

    def test_failed_retirement_keeps_its_name_and_original_source_lease(self):
        root=self.d/'retire';root.mkdir();source=root/'old';source.mkdir()
        (source/'keep').write_text('partial removal evidence')
        locks=self.d/'locks';mirror=self.d/'mirror'
        with patch.object(pruner,'remove_tree',side_effect=PermissionError('ownership control')):
            for _ in range(2):
                result=pruner.prune(root,mirror,locks,max_count=0,active=set())
                self.assertEqual(result['errors'][0]['source'],'old')
                self.assertIn('ownership control',result['errors'][0]['error'])
                self.assertEqual([p.name for p in root.iterdir()],['.prune-old'])
        with (locks/'old.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            result=pruner.prune(root,mirror,locks,max_count=0,active=set())
            self.assertEqual(result['held'],['old']);self.assertTrue(result['over_cap'])
        result=pruner.prune(root,mirror,locks,max_count=0,active=set())
        self.assertEqual(result['removed'],['old']);self.assertEqual(list(root.iterdir()),[])
        # Former naming included the tomb itself and a new PID every time;
        # even the original source lease no longer protected that directory.
        original=root/'.prune-old-123';original.mkdir()
        old={'__name__':'old_pruner'}
        code=Path(pruner.__file__).read_text().replace("name=p.name.removeprefix('.prune-').removesuffix('.prep')","name=p.name.removesuffix('.prep')")
        code=code.replace("tomb=p if p.name.startswith('.prune-') else root/('.prune-'+p.name)","tomb=root/('.prune-'+p.name+'-'+str(os.getpid()))")
        exec(compile(code,'old-retirement','exec'),old)
        with (locks/'old.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            with patch.object(old['remove_tree'].__globals__['shutil'],'rmtree',side_effect=PermissionError('old control')):
                self.assertEqual(old['prune'](root,mirror,locks,max_count=0,active=set())['errors'][0]['source'],'.prune-old-123')
        self.assertTrue(next(root.iterdir()).name.startswith('.prune-.prune-old-123-'))

    def test_one_failed_retirement_does_not_prevent_healthy_peer_cleanup(self):
        root=self.d/'copies';root.mkdir()
        for name in ['a-bad','b-good']:(root/name).mkdir()
        remove=pruner.remove_tree
        def retire(path):
            if path.name=='.prune-a-bad':raise PermissionError('foreign owner')
            remove(path)
        with patch.object(pruner,'remove_tree',side_effect=retire):
            result=pruner.prune(root,self.d/'mirror',self.d/'locks',max_count=0,active=set())
        self.assertEqual(result['removed'],['b-good'])
        self.assertEqual([row['source'] for row in result['errors']],['a-bad'])
        self.assertEqual([p.name for p in root.iterdir()],['.prune-a-bad'])
        # The former global exception aborts before b-good is even visited.
        (root/'b-good').mkdir()
        code=Path(pruner.__file__).read_text().replace("errors.append({'source':name,'error':str(error),'errno':error.errno})",'raise')
        old={'__name__':'old_pruner'};exec(compile(code,'abort-all-peers-control','exec'),old)
        with patch.dict(old,remove_tree=retire):
            with self.assertRaisesRegex(PermissionError,'foreign owner'):
                old['prune'](root,self.d/'mirror',self.d/'locks',max_count=0,active=set())
        self.assertTrue((root/'b-good').is_dir())


if __name__=='__main__':unittest.main()
