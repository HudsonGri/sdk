"""Codex: prevent unsafe repetition inference from incomplete validation metadata."""
import copy,json,tempfile,unittest
from pathlib import Path
import lane_validation as m

class Audit(unittest.TestCase):
 def row(self,tree='head tree uploaded-a',**extra):
  r=dict(at='2026-10-25T12:00:00Z',repo='sdk',lane='test',tree=tree,cmd='python3 .bb-proof.py',wall=360,rc=1,log_id='test',exec_start='2026-10-25T11:54:00Z',schema_version=1);r.update(extra);return r
 def collect(self,rows):
  with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as folder:
   path=Path(folder);(path/'jobs.jsonl').write_text(''.join(json.dumps(r)+'\n'for r in rows))
   (path/'test.log').write_text('test something ... FAILED\n')
   return m.collect('2026-10-25T00:00:00Z','2026-10-26T00:00:00Z',str(path/'jobs.jsonl'),folder)
 def test_uploaded_inputs_and_admission_are_not_duplicate_builds(self):
  r=self.row();rows=self.collect([r,self.row(tree='head tree uploaded-b'),self.row(exec_start=None),self.row(repo='other',tkey='other/test')])['rows']
  self.assertEqual(len(rows),3);self.assertEqual(m.repeat_groups(rows),[])
 def test_control_repeat_is_candidate_never_reuse_permission(self):
  rows=self.collect([self.row(),self.row(),self.row()])['rows'];g=m.repeat_groups(rows)[0]
  self.assertEqual(g['runs'],3);self.assertAlmostEqual(g['hours_beyond_two'],.1);self.assertTrue(g['expected_control_candidate']);self.assertNotIn('reusable',g)
 def test_log_flags_do_not_become_command_scope(self):
  value=m.metadata(self.row(cmd='python3 opaque.py'),'cargo test --workspace --all-features\nAssertionError: {"command":"secret withheld"}')
  self.assertEqual(value['kind'],'opaque_or_focused');self.assertIn('workspace_log',value['tags']);self.assertNotIn('workspace_command',value['tags']);self.assertNotIn('secret',json.dumps(value))
 def test_replayed_proof_is_not_executed_again(self):
  rows=self.collect([self.row(),self.row(execution_kind='existing-verdict-replay')])['rows'];self.assertEqual(m.repeat_groups(rows),[])
 def test_exclusive_kind_hours_reconcile(self):
  v=self.collect([self.row(),self.row(cmd='cargo test -p acyclic-fs --all-features')]);s=m.summarize(v,'start','end')
  self.assertAlmostEqual(sum(r['hours']for r in s['exclusive_kinds'].values()),s['lane_validation']['hours']);self.assertEqual(s['overlapping_tags']['all_features_command']['jobs'],1)
 def test_distinct_crates_with_same_source_path_are_counted(self):
  v=m.metadata(self.row(),'Running unittests src/lib.rs (target/a-123)\nRunning unittests src/lib.rs (target/b-456)')
  self.assertEqual(v['observed_test_binaries'],2)

if __name__=='__main__':unittest.main()
