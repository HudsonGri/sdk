"""Production retirement states, including interrupted and maximum-name copies."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SOURCE=Path(__file__).with_name('prune-wt-sdk.py')
def load(source=SOURCE):
    spec=importlib.util.spec_from_file_location('prune_retirement',source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.directory=Path(self.temp.name)
        self.root=self.directory/'sources';self.root.mkdir()
        self.module=load()
    def tearDown(self):self.temp.cleanup()
    def prune(self):
        return self.module.prune(self.root,self.directory/'mirror',self.directory/'locks',max_count=0,active=set())
    def test_maximum_source_name_has_bounded_retirement_identity(self):
        # Source copies also own the shared <source>.lock lease filename.
        name='s'*(os.pathconf(self.root,'PC_NAME_MAX')-len('.lock'))
        source=self.root/name;source.mkdir();(source/'content').write_text('owned')
        result=self.prune()
        self.assertEqual(result['removed'],[name]);self.assertEqual(list(self.root.iterdir()),[])
    def test_interrupted_retirement_never_reenters_source_or_lease_admission(self):
        source=self.root/'source';source.mkdir();(source/'content').write_text('owned')
        with patch.object(self.module,'remove_tree',side_effect=OSError('interrupted deletion')):
            with self.assertRaisesRegex(OSError,'interrupted deletion'):self.prune()
        retired=list(self.root.iterdir());self.assertEqual(len(retired),1)
        self.assertTrue(retired[0].name.startswith('.prune-'))
        with patch.object(self.module.tempfile,'mkdtemp',side_effect=AssertionError('retirement renamed again')):
            result=self.prune()
        self.assertEqual(result['remaining'],0);self.assertEqual(list(self.root.iterdir()),[])
        self.assertFalse(any(p.name.startswith('.prune-') for p in (self.directory/'locks').iterdir()))
    def test_legacy_retirement_at_name_limit_needs_no_longer_lease_name(self):
        name='.prune-'+'x'*(os.pathconf(self.root,'PC_NAME_MAX')-7)
        retired=self.root/name;retired.mkdir();(retired/'content').write_text('owned')
        self.assertEqual(self.prune()['remaining'],0)
        self.assertEqual(list(self.root.iterdir()),[])
    def test_live_retirement_and_symlink_target_are_preserved(self):
        retired=self.root/'.prune-live';retired.mkdir()
        outside=self.directory/'outside';outside.mkdir();(outside/'keep').write_text('keep')
        (self.root/'.prune-link').symlink_to(outside,target_is_directory=True)
        result=self.module.prune(self.root,self.directory/'mirror',self.directory/'locks',max_count=0,active={str(retired/'child')})
        self.assertEqual(result['held'],['.prune-live'])
        self.assertTrue(result['over_cap'])
        self.assertTrue(retired.exists());self.assertEqual((outside/'keep').read_text(),'keep')
    def test_former_source_derived_name_is_rejected_negative_control(self):
        candidate=self.directory/'former.py'
        source=SOURCE.read_text()
        anchor="tomb=Path(tempfile.mkdtemp(prefix='.prune-',dir=root))"
        self.assertEqual(source.count(anchor),1)
        candidate.write_text(source.replace(anchor,"tomb=root/('.prune-'+p.name+'-'+str(os.getpid()))"))
        self.module=load(candidate)
        with self.assertRaisesRegex(OSError,'File name too long'):
            self.test_maximum_source_name_has_bounded_retirement_identity()
    def test_reclassifying_tombs_is_rejected_negative_control(self):
        candidate=self.directory/'reclassify.py'
        source=SOURCE.read_text()
        anchor="if p.name.startswith('.prune-'):"
        self.assertEqual(source.count(anchor),1)
        candidate.write_text(source.replace(anchor,'if False:'))
        self.module=load(candidate)
        with self.assertRaisesRegex(AssertionError,'retirement renamed again'):
            self.test_interrupted_retirement_never_reenters_source_or_lease_admission()


if __name__=='__main__':unittest.main()
