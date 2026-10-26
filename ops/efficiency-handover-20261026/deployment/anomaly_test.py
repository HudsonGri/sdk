"""Codex: deterministic positive and negative controls for every anomaly threshold."""
import copy
import datetime as D
from pathlib import Path
import tempfile
import unittest
import anomaly as a


class Checks(unittest.TestCase):
    def setUp(self):
        self.now=D.datetime(2026,10,25,18,30,tzinfo=D.timezone.utc)
        self.data=dict(at=self.now.isoformat(),events=[self.event(0,'lane-supervisor','HEARTBEAT lanes=2 running=1 idle=1'),
            self.event(3600,'pr-gate','MERGE1 #1 head=abc merge_rc=0')],queued=[2],holds=[],lanes=[],disks=[],gate_walls=[])

    def event(self,seconds,source,body):
        return dict(at=(self.now-D.timedelta(seconds=seconds)).isoformat(),source=source,body=body)

    def kinds(self):return {r['kind'] for r in a.evaluate(self.data)}

    def test_merge_drought(self):
        self.assertNotIn('MERGE_DROUGHT',self.kinds())
        self.data['events'][1]=self.event(7201,'pr-gate','MERGE1 #1 merge_rc=0')
        self.assertIn('MERGE_DROUGHT',self.kinds())
        self.data['queued']=[];self.assertNotIn('MERGE_DROUGHT',self.kinds())

    def test_usage_limit_two_lane_threshold_and_exact_alert(self):
        self.data['usage_limit']={'lanes':[{'lane':'one'}]}
        self.assertNotIn('USAGE_LIMIT',self.kinds())
        self.data['usage_limit']['lanes'].append({'lane':'two'})
        self.assertIn('USAGE_LIMIT',self.kinds())
        with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as directory:
            path=Path(directory)/'alerts.log'
            lines=a.append_alerts(self.data,[r for r in a.evaluate(self.data)if r['kind']=='USAGE_LIMIT'],path)
            self.assertIn('efficiency ANOMALY USAGE_LIMIT lanes=2',lines[0])
            self.assertEqual(a.append_alerts(self.data,a.evaluate(self.data),path),[])

    def test_last_six_failure_rate(self):
        for i in range(6):self.data['events'].append(self.event(60-i,'pr-gate',f'GATE_{"FAIL" if i<3 else "PASS"} #{100+i}: x'))
        self.assertNotIn('GATE_FAILURE_RATE',self.kinds())
        self.data['events'][-1]['body']='GATE_FAIL #105: x'
        self.assertIn('GATE_FAILURE_RATE',self.kinds())

    def test_cross_pr_findings_and_tests(self):
        self.data['events'] += [self.event(100,'pr-gate','GATE_FAIL #11: clippy: +1 rust/a.rs: unused argument; log=a'),
                                self.event(50,'pr-gate','GATE_FAIL #11+12: clippy: +1 rust/a.rs: unused argument; log=b')]
        self.assertNotIn('CROSS_PR_FINDING',self.kinds())
        self.data['events'][-1]['body']='GATE_FAIL #13: clippy: +1 rust/a.rs: unused argument; log=b'
        self.assertIn('CROSS_PR_FINDING',self.kinds())
        self.data['events'] += [self.event(90,'pr-gate','GATE_FAIL #21: new failures vs main (abc): module::case log=a'),
                               self.event(40,'pr-gate','GATE_FAIL #22: new failures vs main (abc): module::case log=b')]
        self.assertIn('CROSS_PR_TEST',self.kinds())
        self.data['events'][-1]['body']='GATE_FAIL #21+22: new failures vs main (abc): module::case log=b'
        self.assertNotIn('CROSS_PR_TEST',self.kinds())

    def test_untouched(self):
        self.assertNotIn('UNTOUCHED_FILE_FINDING',self.kinds())
        self.data['untouched_findings']=[dict(pr=2,path='x.rs',head='a',base='b',touched_count=1)]
        self.assertIn('UNTOUCHED_FILE_FINDING',self.kinds())

    def test_hold_threshold(self):
        self.data['holds']=[dict(name='freeze',age_seconds=900)]
        self.assertNotIn('GATE_HOLD',self.kinds())
        self.data['holds'][0]['age_seconds']=901
        self.assertIn('GATE_HOLD',self.kinds())

    def test_failed_route_requires_matching_successful_delivery(self):
        self.data['events'].append(self.event(100,'pr-gate','ROUTE_ERROR #1504 GATE_FAIL clippy verdict extraction failed:'))
        self.assertIn('ROUTE_FAILURE',self.kinds())
        self.data['events'].extend([self.event(50,'pr-gate','MERGE1 #1504 queued for gate'),
                                   self.event(40,'pr-gate','ROUTED #15040 GATE_FAIL lane=fake'),
                                   self.event(30,'pr-gate','ROUTED #1504 GATE_PASS lane=fake')])
        self.assertIn('ROUTE_FAILURE',self.kinds())
        self.data['events'].append(self.event(20,'pr-gate','ROUTED #1504 GATE_FAIL lane=fake'))
        self.assertNotIn('ROUTE_FAILURE',self.kinds())
        self.data['events'].append(self.event(10,'pr-gate','ROUTE_ERROR head=abc1504def GATE_FAIL bad'))
        self.assertNotIn('ROUTE_FAILURE',self.kinds())
        self.data['events'].append(self.event(86401,'pr-gate','ROUTE_ERROR #9 GATE_FAIL stale'))
        self.assertNotIn('ROUTE_FAILURE',self.kinds())

    def test_lane_threshold(self):
        for state in ['IDLE','BLOCKED','STUCK']:
            self.data['lanes']=[dict(lane='fake',state=state,age_seconds=3601)]
            self.assertIn('LANE_'+state,self.kinds())
            self.data['lanes'][0]['age_seconds']=3600
            self.assertNotIn('LANE_'+state,self.kinds())
        self.data['lanes']=[dict(lane='fake',state='RUNNING',age_seconds=9000)]
        self.assertFalse(any(x.startswith('LANE_') for x in self.kinds()))

    def test_heartbeat_and_activity(self):
        self.assertNotIn('SUPERVISOR_STALE',self.kinds())
        self.assertNotIn('SUPERVISOR_HEARTBEAT',self.kinds())
        self.data['events'][0]=self.event(601,'lane-supervisor','HEARTBEAT lanes=2 running=1 idle=1')
        self.assertIn('SUPERVISOR_STALE',self.kinds())
        self.assertIn('SUPERVISOR_HEARTBEAT',self.kinds())
        self.data['events'][0]=self.event(0,'lane-supervisor','HEARTBEAT lanes=0 running=0 idle=0')
        self.assertIn('ZERO_LANES',self.kinds())

    def test_disks(self):
        for host in ['bb2','ivar']:
            self.data['disks']=[dict(host=host,path='/',free_pct=15)]
            self.assertNotIn('DISK_PRESSURE',self.kinds())
            self.data['disks'][0]['free_pct']=14.99
            self.assertIn('DISK_PRESSURE',self.kinds())

    def test_slow_gate(self):
        self.data['gate_walls']=[dict(file=str(i),seconds=n) for i,n in enumerate([100,100,150])]
        self.assertNotIn('GATE_SLOW',self.kinds())
        self.data['gate_walls'][-1]['seconds']=151
        self.assertIn('GATE_SLOW',self.kinds())

    def test_disk_host_transport_and_unavailable(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        result=SimpleNamespace(returncode=0,stdout='Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test 1000 800 200 80% /\n')
        with patch.object(a.subprocess,'run',return_value=result) as call:
            disks,error=a.remote_disks('ivar')
            self.assertIsNone(error);self.assertEqual(disks[0]['free_pct'],20)
            self.assertEqual(call.call_args.args[0][0],'C:/Windows/System32/OpenSSH/ssh.exe')
            self.assertNotIn('wsl',call.call_args.args[0]) # Old WSL-only shape fails this contract.
            a.remote_disks('bb2');self.assertEqual(call.call_args.args[0][:3],['wsl','-d','Ubuntu'])
        with patch.object(a.subprocess,'run',return_value=SimpleNamespace(returncode=255,stdout='')):
            disks,error=a.remote_disks('ivar');self.assertEqual(disks,[]);self.assertIn('rc=255',error)

    def test_marker_boundary_and_append_idempotency(self):
        self.assertEqual(a.prs('head=abc1495def #14950 #1495x #1497+1499:'),[1495,1497,1499,14950])
        self.assertEqual(a.prs('head=abc1495def'),[])
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'alerts.log';path.write_bytes(b'prior\n')
            anomalies=[dict(kind='TEST',key='fake',evidence='line\nline'+'x'*4000,suspected_cause='fake')]
            self.assertEqual(len(a.append_alerts(self.data,anomalies,path)),1)
            first=path.read_bytes();self.assertTrue(first.startswith(b'prior\n'))
            self.assertEqual(len(first.splitlines()),2)
            self.assertEqual(a.append_alerts(self.data,anomalies,path),[])
            self.assertEqual(path.read_bytes(),first)


if __name__=='__main__':unittest.main()
