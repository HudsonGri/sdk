"""Bounded synthetic regression checks; no builds or external service calls. Codex."""
import json
import tempfile
import unittest
from pathlib import Path

import measure as m


class MeasurementTests(unittest.TestCase):
    def test_admission_percentiles_separate_queue_from_preparation(self):
        jobs=[{'schema_version':1,'admission_backend':'bb2','wall':100,
               'admission':{'wait_seconds':x},'admission_start':'2026-10-23T00:00:00Z',
               'exec_start':'2026-10-23T00:01:00Z'} for x in range(20)]
        result=m.receipt_metrics(jobs)
        self.assertEqual(60,result['admission_overhead_p95_seconds'])
        self.assertEqual(9.5,result['admission_phase_coverage']['slot_wait_p50_seconds'])
        self.assertEqual(19,result['admission_phase_coverage']['slot_wait_p95_seconds'])

    def test_target_receipts_are_counted_once_and_never_verdicts(self):
        row={'target_store':[{'status':'miss','clone_seconds':99}],
             'remote_receipt':{'target_store':[{'version':'v2','status':'hit','clone_seconds':2,'local_wait_seconds':3}]}}
        result=m.compute_metrics([row])
        self.assertEqual(1,result['target_store_requests'])
        self.assertEqual({'hit':1},result['target_store_states'])
        self.assertEqual({'v2':1},result['target_store_generations'])
        self.assertEqual(2,result['target_store_clone_seconds'])
        self.assertEqual(1,result['target_store_local_wait_samples'])
        self.assertEqual(3,result['target_store_local_wait_seconds'])
    def parse(self, text, parser):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'fixture.log'
            p.write_text(text, encoding='utf-8')
            return parser(p)

    def test_codex_run_totals_not_cumulative_double_count(self):
        result = self.parse('tokens used\n999\nOpenAI Codex v1.0\ntokens used\n100\ntokens used\n120\nOpenAI Codex v1.0\ntokens used\n30\nOpenAI Codex v1.0\n', m.parse_codex)
        self.assertEqual(result['tokens'], 150)  # summing every summary would incorrectly give 250
        self.assertEqual(result['completed_runs'], 2)
        self.assertEqual(result['runs'], 3)

    def test_claude_partial_messages_and_repository_boundary(self):
        def record(cwd, output, key='one'):
            return json.dumps({'cwd': cwd, 'timestamp':'2026-10-22T12:00:00Z', 'message': {
                'id':key, 'usage':{'input_tokens':2,'output_tokens':output,'cache_read_input_tokens':20}}})
        data = '\n'.join([record('Q:\\sdk', 2), record('Q:\\sdk', 7), record('Q:/unrelated',100,'other'),
                          record('Q:/wt/unregistered',100,'unknown'), 'not json'])
        result = self.parse(data, m.parse_claude)
        self.assertEqual(len(result['messages']), 1)
        self.assertEqual(sum(result['messages'][0]['usage'].values()), 29)

    def test_gate_wall_and_feature_phases_are_separate(self):
        gate = self.parse('GATE_PR_WALL seconds=100 remote_rc=0\nGATE_WALL seconds=80 test_rc=0\nGATE_FEATURES_WALL seconds=20 rc=0\n', m.parse_gate)
        self.assertEqual(gate['seconds'], 100)  # not 200: phases are nested
        self.assertEqual(gate['feature_seconds'], [20])

    def test_occupancy_unions_overlapping_jobs(self):
        start, end = m.dt('2026-10-22T00:00:00Z'), m.dt('2026-10-22T01:00:00Z')
        jobs = [{'at':'2026-10-22T00:40:00Z','wall':1800}, {'at':'2026-10-22T00:50:00Z','wall':1800}]
        result = m.intervals(jobs, start, end)
        self.assertAlmostEqual(result['busy_hours_inferred'], 2/3)
        self.assertEqual(result['peak_jobs_inferred'], 2)
        self.assertIsNone(result['saturated_hours'])

    def test_gate_cost_join_exact_sha_and_window_boundaries(self):
        start, end = m.dt('2026-10-22T00:00:00Z'), m.dt('2026-10-23T00:00:00Z')
        jobs = [{'at':'2026-10-22T12:00:00Z','wall':360,'tkey':'sdk/pr-gate-a',
                 'tree':'abc123abc000 data','cmd':'bash gate.sh','cwd':'/mnt/q/wt/pr-gate','rc':1},
                {'at':'2026-10-22T12:00:00Z','wall':720,'tkey':'sdk/pr-gate-b',
                 'tree':'deadbeef000 data','cmd':'bash gate.sh','cwd':'/mnt/q/wt/pr-gate','rc':0}]
        alerts = [{'at':'2026-10-22T12:00:00Z','event':'GATE_FAIL','prs':[12],
                   'log':'12-abc123abc.log','clippy':True,'p2':False},
                  {'at':'2026-10-22T12:00:00Z','event':'GATE_BASELINE','prs':[12],
                   'log':'12-abc123abc.log','clippy':False,'p2':False}]
        prs = [{'number':12,'createdAt':'2026-10-22T10:00:00Z','mergedAt':'2026-10-22T13:00:00Z'},
               {'number':13,'createdAt':'2026-10-22T10:00:00Z','mergedAt':'2026-10-23T00:00:00Z'}]
        result = m.window('fixture',start,end,prs,{},jobs,jobs,[],alerts,[])
        self.assertEqual(result['merged_count'],1)
        cost = result['waste']['clippy_ratchet_failure']
        self.assertAlmostEqual(cost['bb2_job_hours'],.1)  # unrelated SHA must not add .2 hours
        self.assertEqual(cost['matched_jobs'],1)
        self.assertIsNone(cost['tokens'])
        deferred = result['waste']['baseline_deferred']
        self.assertEqual(deferred['events'],1)
        self.assertAlmostEqual(deferred['bb2_job_hours'],.1)

    def test_source_cache_avoids_reparsing_unchanged_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'fixture.log'
            path.write_text('x',encoding='utf-8')
            cache, signatures = {}, []
            def parse(p):
                return {'length':p.stat().st_size}
            self.assertEqual(m.cached_file(path,parse,cache,signatures),{'length':1})
            def must_not_run(p):
                raise AssertionError('unchanged input reparsed')
            self.assertEqual(m.cached_file(path,must_not_run,cache,signatures),{'length':1})
            path.write_text('xx',encoding='utf-8')
            self.assertEqual(m.cached_file(path,parse,cache,signatures),{'length':2})

    def test_verdict_join_does_not_charge_future_retry_or_probe(self):
        job={'at':'2026-10-23T09:00:00.355Z','wall':600,'lane':'pr-gate',
             'tree':'abcdef1234 other','gate_log':'Q:/lanes/pr-gate/12-abcdef123.log',
             'phase':'pr-gate'}
        rows=[job,dict(job,at='2026-10-23T09:05:00Z',wall=300),
              dict(job,at='2026-10-23T09:00:00.500Z',phase='command',wall=20)]
        alert={'at':'2026-10-23T09:00:00Z','log':'12-abcdef123.log'}
        self.assertEqual(m.gate_verdict_jobs([alert],rows),[job])
        later=dict(alert,at='2026-10-23T09:05:05Z')
        self.assertEqual(m.gate_verdict_jobs([later],rows),[rows[1]])
        self.assertEqual(m.gate_verdict_jobs([alert,alert,later],rows),rows[:2])

    def test_disappeared_source_does_not_abort_or_claim_complete_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'gone.output'
            errors = []
            self.assertIsNone(m.collect_file(path, m.parse_claude, {}, [], errors))
            self.assertEqual(len(errors), 1)
            self.assertIn('coverage incomplete', errors[0])

    def test_same_endpoint_revision_keeps_previous_cycle_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'metrics.jsonl'
            old = {'as_of':'2026-10-22T22:44:31Z','marker':'baseline'}
            latest = {'as_of':'2026-10-23T02:19:14Z','marker':'revision'}
            path.write_text(json.dumps(old)+'\n'+json.dumps(latest)+'\n',encoding='utf-8')
            self.assertEqual(m.cycle_baseline(latest,m.dt(latest['as_of']),path),old)

    def test_receipts_without_legacy_key_are_still_sdk_jobs(self):
        job = {'repo':'sdk','schema_version':1,'wall':30,'termination':'infra'}
        self.assertTrue(m.is_sdk_job(job))
        self.assertFalse(m.is_sdk_job({'repo':'other'}))
        result = m.receipt_metrics([job])
        self.assertEqual(result['terminal_receipts'], 1)
        self.assertEqual(result['terminations'], {'infra':1})
        self.assertEqual(result['execution_samples'],0)

    def test_admission_tail_lanes_and_cancelled_execution(self):
        jobs = [
            {'schema_version':1,'lane':'a','termination':'command','wall':100,
             'admission_start':'2026-10-23T01:00:00Z','exec_start':'2026-10-23T01:00:20Z',
             'end':'2026-10-23T01:01:40Z'},
            {'schema_version':1,'cwd':'Q:\\wt\\b','termination':'cancelled','wall':400,
             'admission_start':'2026-10-23T01:00:00Z','exec_start':'2026-10-23T01:05:00Z',
             'end':'2026-10-23T01:06:40Z'},
            {'schema_version':1,'lane':'a','termination':'cancelled','wall':10,
             'admission_start':'2026-10-23T01:01:00Z','exec_start':'2026-10-23T01:00:00Z'}]
        result=m.receipt_metrics(jobs)
        self.assertEqual(result['admission_samples'],2)
        self.assertAlmostEqual(result['admission_overhead_hours'],320/3600)
        self.assertAlmostEqual(result['admission_overhead_lane_hours']['b'],300/3600)
        self.assertEqual(result['admission_overhead_p90_seconds'],300)
        self.assertEqual(result['admission_overhead_max_seconds'],300)
        self.assertEqual(result['admission_overhead_top_jobs'][0]['lane'],'b')
        self.assertAlmostEqual(result['execution_termination_hours']['cancelled'],100/3600)

    def test_new_phase_receipts_and_nonexecution_retries(self):
        job={'schema_version':1,'wall':20,'termination':'infra','tree':'abc command',
             'cmd':'build','admission_backend':'bb2','cache':'not-started',
             'admission':{'wait_seconds':'0.00005'},'at':'2026-10-23T01:00:00Z'}
        rows=[dict(job),dict(job),dict(job,admission={'wait_seconds':'NaN'})]
        result=m.receipt_metrics(rows)['admission_phase_coverage']
        self.assertEqual(result['receipts'],3)
        self.assertEqual(result['cache_states'],{'not-started':3})
        self.assertEqual(result['slot_wait_samples'],2)
        self.assertAlmostEqual(result['slot_wait_hours'],0.0001/3600)
        start,end=m.dt('2026-10-23T00:00:00Z'),m.dt('2026-10-24T00:00:00Z')
        alert={'event':'GATE_PREPARATION_INFRA','at':job['at'],'prs':[12],
               'log':None,'clippy':False,'p2':False}
        result=m.window('fixture',start,end,[],{},rows,rows,[],[alert],[])['waste']
        self.assertEqual(result['infra_conflict_rebase']['events'],1)
        self.assertEqual(result['duplicate_tree_command_candidates']['extra_jobs'],0)
        self.assertEqual(result['validation_loop_candidates']['jobs_beyond_two'],0)

    def test_remote_receipt_is_one_execution_and_admission_is_separate(self):
        job = {'repo':'sdk','schema_version':1,'wall':100,'termination':'command',
               'admission_start':'2026-10-23T01:00:00Z','exec_start':'2026-10-23T01:00:20Z',
               'end':'2026-10-23T01:01:40Z','remote_receipt':{'end':'2026-10-23T01:01:30Z'}}
        result = m.receipt_metrics([job])
        self.assertEqual(result['terminal_receipts'],1)
        self.assertAlmostEqual(result['known_execution_hours'],70/3600)
        self.assertAlmostEqual(result['admission_overhead_hours'],20/3600)
        self.assertAlmostEqual(result['admission_overhead_lane_hours']['unknown'],20/3600)
        self.assertEqual(result['admission_overhead_p90_seconds'],20)
        self.assertEqual(result['admission_overhead_top_jobs'][0]['seconds'],20)
        self.assertAlmostEqual(result['execution_termination_hours']['command'],70/3600)
        start,end=m.dt('2026-10-23T01:00:00Z'),m.dt('2026-10-23T02:00:00Z')
        occupancy=m.intervals([job,{'schema_version':1,'at':'2026-10-23T01:30:00Z','wall':1000}],start,end)
        self.assertAlmostEqual(occupancy['busy_hours_inferred'],70/3600)


    def test_compute_one_remote_copy_and_partial_coverage(self):
        stats={'stats':{'cache_hits':{'counts':{'Rust':3}},'cache_misses':{'counts':{'Rust':1}},'compilations':1}}
        rows=[{'phase':'gate','compute':{'attributed_cpu_seconds':9999},
               'remote_receipt':{'compute':{'attributed_cpu_seconds':3600,'sccache':stats}}},
              {'phase':'lane','compute':{'attributed_cpu_seconds':float('nan')}},{}]
        result=m.compute_metrics(rows)
        self.assertEqual(result['receipts'],2)
        self.assertEqual(result['cpu_samples'],1)
        self.assertEqual(result['attributed_core_hours'],1)
        self.assertEqual(result['core_hours_by_phase'],{'gate':1})
        self.assertEqual(result['cache_hit_rate'],.75)
        self.assertEqual(result['compiler_executions'],1)
        self.assertIsNone(m.compute_metrics([{}])['attributed_core_hours'])


if __name__ == '__main__':
    unittest.main()
