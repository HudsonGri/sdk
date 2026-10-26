"""Codex: counter replay, rate and progress attribution controls; no real jobs."""
import copy,json,tempfile,unittest
from pathlib import Path
import measure,token_audit

class Tokens(unittest.TestCase):
    def test_resume_counter_replay_and_unknown_identity(self):
        sid='01a158d8-0164-7930-8a64-46abdb35109f'
        header='OpenAI Codex v0.163.0\nsession id: '+sid+'\n'
        body=header+'tokens used\n100\n'+header+'ERROR: hit your usage limit\ntokens used\n100\n'+header+'tokens used\n150\n'
        with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as temp:
            path=Path(temp)/'fake.log';path.write_text(body);row=measure.parse_codex(path)
            self.assertEqual(row['tokens'],150);self.assertEqual(row['legacy_sum_run_counters'],350)
            path.write_text('OpenAI Codex v0.163.0\ntokens used\n900\n')
            row=measure.parse_codex(path);self.assertEqual(row['tokens'],0);self.assertEqual(row['unattributed_summaries'],1)

    def sample(self,tokens,at):
        return dict(as_of=at,codex_inventory_undated=[dict(lane='fake',tokens=tokens,counter_basis='session-high-water-v1',sessions=[dict(session_id='01a158d8-0164-7930-8a64-46abdb35109f',tokens=tokens)],legacy_sum_run_counters=tokens*3,repeated_counter_overcount=tokens*2)])

    def test_comparable_rates_merge_ratio_and_unknown_baseline(self):
        first=self.sample(100,'2026-10-25T20:00:00Z');last=self.sample(500,'2026-10-25T22:00:00Z')
        pr=dict(number=1,headRefName='codex/fake-fix',state='MERGED',mergedAt='2026-10-25T21:00:00Z')
        value=token_audit.build(last,[first],[pr],['fake'])
        self.assertEqual(value['rows'][0]['observed_tokens_per_hour'],200)
        self.assertEqual(value['observed_tokens_per_merge'],400)
        self.assertEqual(value['rows'][0]['matched_merges_24h'],[1])
        old=copy.deepcopy(first);old['codex_inventory_undated'][0].pop('counter_basis')
        self.assertIsNone(token_audit.build(last,[old],[pr],['fake'])['observed_counter_delta'])

    def test_session_copy_dedup_reset_and_branch_boundary(self):
        first=self.sample(100,'2026-10-25T20:00:00Z');last=self.sample(50,'2026-10-25T22:00:00Z')
        pr=dict(number=2,headRefName='codex/fakery-fix',state='MERGED',mergedAt='2026-10-25T21:00:00Z')
        value=token_audit.build(last,[first],[pr],['fake'])
        self.assertIsNone(value['observed_counter_delta']);self.assertEqual(value['rows'][0]['matched_merges_24h'],[])
        last['codex_inventory_undated'].append(copy.deepcopy(last['codex_inventory_undated'][0]))
        self.assertEqual(token_audit.counters(last),token_audit.counters(self.sample(50,last['as_of'])))

    def test_missing_or_reset_counter_is_unknown(self):
        first=self.sample(100,'2026-10-25T20:00:00Z');last=self.sample(150,'2026-10-25T22:00:00Z')
        self.assertEqual(measure.codex_counter_delta(last['codex_inventory_undated'],first['codex_inventory_undated']),50)
        self.assertIsNone(measure.codex_counter_delta([],first['codex_inventory_undated']))
        self.assertIsNone(measure.codex_counter_delta(first['codex_inventory_undated'],last['codex_inventory_undated']))

    def test_latest_same_endpoint_revision_uses_actual_observation(self):
        old=self.sample(900,'2026-10-25T20:00:00Z');corrected=self.sample(100,old['as_of'])
        corrected['codex_inventory_observed_at']='2026-10-25T20:30:00Z'
        last=self.sample(400,'2026-10-25T21:00:00Z');last['codex_inventory_observed_at']=last['as_of']
        value=token_audit.build(last,[old,corrected],[],['fake'])
        self.assertEqual(value['observed_counter_delta'],300)
        self.assertEqual(value['rows'][0]['observed_tokens_per_hour'],600)
        self.assertIsNone(value['observed_tokens_per_merge'])

if __name__=='__main__':unittest.main()
