"""Codex: outcome-metric controls; no live files, models or builds."""
import unittest
import iteration

class Controls(unittest.TestCase):
    def fixture(self):
        m=dict(as_of='2026-10-26T12:00:00Z',windows=[dict(bb2_job_hours_per_merge=12,merged_prs=[dict(number=12,mergedAt='2026-10-26T10:00:00Z')])])
        def event(at,body):return dict(at='2026-10-26T'+at+'Z',source='pr-gate',body=body)
        a=dict(events=[event('06:00:00','MERGE1 #12 queued for gate'),event('07:00:00','GATE_FAIL #123: unrelated'),event('08:00:00','GATE_PASS #12: proof complete'),event('10:00:00','MERGE1 #12 head=abcdef merge_rc=0')])
        return m,a
    def test_explicit_queue_and_first_outcome(self):
        m,a=self.fixture();v=iteration.targets(m,a,'')
        self.assertEqual(v['queue_to_merge']['p50_hours'],4)
        self.assertEqual(v['first_observed_pass']['fraction'],1)
        # Including PR-number prefixes would incorrectly blame #123 on #12.
        a['events'].insert(2,dict(at='2026-10-26T07:30:00Z',source='pr-gate',body='GATE_FAIL #12: real first failure'))
        self.assertEqual(iteration.targets(m,a,'')['first_observed_pass']['fraction'],0)
    def test_missing_queue_never_invents_latency(self):
        m,a=self.fixture();a['events']=a['events'][1:]
        self.assertIsNone(iteration.targets(m,a,'')['queue_to_merge']['p95_hours'])
    def test_percentile_and_drought_boundaries(self):
        self.assertEqual(iteration.quantile([1,3],.95),2.9)
        m,a=self.fixture();v=iteration.targets(m,a,'')
        self.assertEqual(v['longest_drought_hours'],22)

if __name__=='__main__':unittest.main()
