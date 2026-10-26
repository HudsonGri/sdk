"""Codex: isolated machinery regressions; fake lane, fake codex, scratch alerts only."""
import importlib.util, json, os, re, shutil, subprocess, tempfile, time, unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('controller',HERE/'fleet-supervisor.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
spec=importlib.util.spec_from_file_location('drainer',HERE/'fleet-steer.py')
d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)

class Machinery(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix='fleet-',dir='D:/lane-scratch/efficiency')
  self.root=Path(self.tmp.name);self.gate=self.root/'gate';self.gate.mkdir()
  self.alerts=self.root/'alerts.log';self.alerts.write_text('')
  (self.root/'active-lanes.txt').write_text('fake')
  (self.root/'fake.last.md').write_text('LANE_STATUS: DONE\n')
  (self.root/'fake.log').write_text('session id: fake\n')
  self.now=time.time();self.sent=[]
  self.s=m.Supervisor(self.root,self.alerts,self.gate,clock=lambda:self.now,dispatch=lambda *args:self.sent.append(args))
 def tearDown(self):self.tmp.cleanup()
 def runpass(self,rows=None):self.s.pass_once([] if rows is None else rows);return self.alerts.read_text()
 def final(self,value,age=20):
  path=self.root/'fake.last.md';path.write_text(value);os.utime(path,(self.now-age,self.now-age))
 def test_newline_and_zero_registry(self):
  # Original while-read predicate skips the unterminated record.
  self.assertFalse((self.root/'active-lanes.txt').read_bytes().endswith(b'\n'))
  self.assertIn('HEARTBEAT lanes=1 running=0 idle=1',self.runpass())
  (self.root/'active-lanes.txt').write_text('');self.assertIn('ZERO_LANES',self.runpass())
 def test_completion_receipt_against_mtime_false_death(self):
  self.final('LANE_STATUS: DONE\n',400)
  os.utime(self.root/'fake.log',(self.now,self.now))
  self.assertGreater((self.root/'fake.log').stat().st_mtime,(self.root/'fake.last.md').stat().st_mtime+120)
  receipt=self.root/'lane-state/fake.completed';receipt.write_text(str(int((self.root/'fake.last.md').stat().st_mtime)))
  self.assertNotIn('REINVOKE fake DIED',self.runpass())
  receipt.unlink();self.assertIn('REINVOKE fake DIED',self.runpass())
 def test_case_collision(self):
  (self.root/'supervise').mkdir()
  self.assertFalse((self.root/'SUPERVISE').is_file()) # Actual case-insensitive Windows fixture.
  # Entry point's distinct control file cannot collide with the old directory.
  (self.root/'lane-supervisor.enabled').touch()
  self.assertIn('HEARTBEAT',self.runpass())
  self.assertIn("control=root/'lane-supervisor.enabled'",(HERE/'fleet-supervisor.py').read_text())
 def test_locale_and_invalid_regex(self):
  self.final('LANE_STATUS: STANDBY DOES_NOT_MATCH\n')
  self.assertIn('HEARTBEAT',self.runpass())
  self.final('LANE_STATUS: STANDBY [\n')
  self.assertIn('INVALID_STANDBY',self.runpass())
  self.assertIn("['grep','-aE','--',pattern]",(HERE/'fleet-supervisor.py').read_text())
  self.assertNotIn("'grep','-P'",(HERE/'fleet-supervisor.py').read_text())
 def test_sha_and_prefix_rejected_actual_gate_news(self):
  self.final('LANE_STATUS: WAITING_GATE #1495\n')
  stamp=m.D.datetime.fromtimestamp(self.now,m.D.timezone.utc).isoformat()
  false=f'{stamp} pr-gate GATE_FAIL 1495abcdef\n'
  self.assertTrue(re.search(r'GATE_FAIL #?(1495)([^0-9]|$)',false)) # Old false positive.
  self.alerts.write_text(false+f'{stamp} pr-gate GATE_FAIL #14950\n')
  self.assertNotIn('REINVOKE fake GATE_NEWS',self.runpass())
  with self.alerts.open('a') as f:f.write(f'{stamp} pr-gate GATE_PASS #1497+1495\n')
  self.assertIn('REINVOKE fake GATE_NEWS',self.runpass())
 def test_hold_alert_and_no_unsafe_release(self):
  hold=self.gate/'preparation-hold.json';hold.write_text('{"proof":"missing"}')
  os.utime(hold,(self.now-1000,self.now-1000))
  self.assertNotIn('ANOMALY GATE_HOLD',(HERE/'before/lane-supervisor2.sh').read_text())
  self.assertIn('ANOMALY GATE_HOLD',self.runpass());self.assertTrue(hold.exists())
  hold.unlink();self.alerts.write_text('');self.assertNotIn('GATE_HOLD',self.runpass())
 def test_background_ownership_orphan_and_pid_reuse(self):
  self.final('LANE_STATUS: WORKING\n',400)
  (self.root/'lane-state/fake.completed').write_text(str(int((self.root/'fake.last.md').stat().st_mtime)))
  rows=[dict(pid=111,parent=1,birth=self.now-50,roots=['fake']),dict(pid=222,parent=111,birth=self.now-40)]
  self.assertIn('running=1',self.runpass(rows));self.assertFalse(self.sent)
  self.assertIn('WAITING_JOB fake',self.runpass(rows[1:]));self.assertFalse(self.sent) # Orphan retains birth-fenced owner.
  rows[1]['birth']=self.now+1 # PID reuse must not suppress work.
  self.assertIn('JOB_ENDED fake',self.runpass(rows[1:]));self.assertTrue(self.sent)
 def test_probe_unknown_never_dispatches_and_stale_locks(self):
  self.s.pass_once(None);self.assertFalse(self.sent);self.assertIn('PROBE_UNAVAILABLE',self.alerts.read_text())
  lock=self.root/'inbox/fake.lock';lock.mkdir();os.utime(lock,(self.now-500,self.now-500))
  self.assertIn('CLEARED_DEAD_INBOX_LOCK',self.runpass());self.assertFalse(lock.exists())
  lock.mkdir();(lock/'unknown').touch();os.utime(lock,(self.now-500,self.now-500))
  self.assertIn('INBOX_CUSTODY',self.runpass());self.assertTrue(lock.exists())
 def test_fake_codex_inherited_pipe_negative_and_file_positive(self):
  # Same fake producer, actual OS pipes/files. No Codex API call.
  script=self.root/'fake-codex.py';child=self.root/'child.py'
  child.write_text('import sys,time\nfrom pathlib import Path\np=Path(sys.argv[1]);end=time.monotonic()+3\nwhile not p.exists() and time.monotonic()<end:time.sleep(.005)\n')
  script.write_text('import subprocess,sys\nfrom pathlib import Path\nPath(sys.argv[1]).write_text("LANE_STATUS: DONE\\n")\np=subprocess.Popen([sys.executable,sys.argv[2],sys.argv[3]],stdout=sys.stdout,stderr=sys.stderr)\nPath(sys.argv[4]).write_text(str(p.pid))\n')
  import sys,ctypes,threading
  kernel=ctypes.WinDLL('kernel32',use_last_error=True)
  kernel.OpenProcess.restype=ctypes.c_void_p
  kernel.WaitForSingleObject.argtypes=[ctypes.c_void_p,ctypes.c_ulong]
  kernel.CloseHandle.argtypes=[ctypes.c_void_p]
  for pipe in (True,False):
   release=self.root/('release-'+str(pipe));pidfile=self.root/('pid-'+str(pipe))
   with (self.root/'file.log').open('wb') as output:
    process=subprocess.Popen([sys.executable,str(script),str(self.root/'fake.last.md'),str(child),str(release),str(pidfile)],stdout=subprocess.PIPE if pipe else output,stderr=subprocess.STDOUT)
    process.wait(timeout=2)
    handle=kernel.OpenProcess(0x100000,False,int(pidfile.read_text()))
    self.assertTrue(handle)
    try:
     # Parent has ended; descendant is still live with its inherited writer.
     self.assertEqual(kernel.WaitForSingleObject(handle,0),258)
     if pipe:
      eof=threading.Event()
      def read_pipe():process.stdout.read();eof.set()
      reader=threading.Thread(target=read_pipe,daemon=True);reader.start()
      self.assertFalse(eof.wait(.02)) # Old pipe reader is still blocked.
    finally:
     release.touch();self.assertEqual(kernel.WaitForSingleObject(handle,2000),0);kernel.CloseHandle(handle)
    if pipe:reader.join(1);self.assertTrue(eof.is_set());process.stdout.close()
  self.assertNotRegex((HERE/'steer.sh').read_text(),r'codex exec[^\n]*\|\s*tee')
  self.assertIn('stdout=stdout,stderr=stdout',(HERE/'fleet-steer.py').read_text())
  self.assertIn('completed.write_text',(HERE/'fleet-steer.py').read_text())
 def test_native_powershell_owner_birth_and_unknown(self):
  stamp=m.D.datetime.fromtimestamp(self.now-60,m.D.timezone.utc).isoformat()
  rows=[dict(Name='codex.exe',CommandLine='codex exec -o Q:/cx/fake.last.md',ProcessId=11111,ParentProcessId=1,CreationDate=stamp),dict(Name='wsl.exe',CommandLine='wsl build-slot',ProcessId=22222,ParentProcessId=11111,CreationDate=stamp)]
  snap=self.root/'processes.json'
  env={**os.environ,'FLEET_ROOT':str(self.root)}
  def probe(mode):
   snap.write_text(json.dumps(rows))
   result=subprocess.run(['powershell','-NoProfile','-File',str(HERE/'lane-running.ps1'),'-lane','fake','-Mode',mode,'-ProcessSnapshot',str(snap)],env=env,capture_output=True,timeout=4)
   if result.returncode==2:print(result.stderr.decode(errors='replace'))
   return result.returncode
  self.assertEqual(probe('Running'),0)
  rows=rows[1:];self.assertEqual(probe('Busy'),0)
  rows[0]['CreationDate']=m.D.datetime.fromtimestamp(self.now+60,m.D.timezone.utc).isoformat()
  self.assertEqual(probe('Busy'),1)
 def test_actual_drainer_fake_codex_completion(self):
  binpath=self.root/'bin';binpath.mkdir()
  import sys
  (binpath/'fake.py').write_text('import sys\nfrom pathlib import Path\na=sys.argv[1:];Path(a[a.index("-o")+1]).write_text("LANE_STATUS: DONE\\n")\nprint("fake-codex-final")\n')
  (binpath/'fake.cmd').write_text('@echo off\n"'+sys.executable+'" "'+str(binpath/'fake.py')+'" %*\n')
  (self.root/'inbox/fake.md').write_text('## Steer fake\ncontinue\n')
  shutil.copyfile(HERE/'steer.sh',self.root/'steer.sh')
  shutil.copyfile(HERE/'fleet-steer.py',self.root/'fleet-steer.py')
  (self.root/'lane-running.ps1').write_text('exit 1\n')
  env={**os.environ,'FLEET_ROOT':self.root.as_posix(),'FLEET_ALERTS':self.alerts.as_posix(),'FLEET_WORKSPACE':self.root.as_posix(),'FLEET_CODEX':(binpath/'fake.cmd').as_posix()}
  with (self.root/'drainer.out').open('wb') as output:
   process=subprocess.Popen(['C:/Program Files/Git/usr/bin/bash.exe','--noprofile','--norc','-x',(self.root/'steer.sh').as_posix(),'fake'],env=env,stdin=subprocess.DEVNULL,stdout=output,stderr=output)
   try:process.wait(timeout=4)
   except subprocess.TimeoutExpired:
    # Only the live Popen root we created and its fixture descendants; no fleet PID lookup.
    subprocess.run(['taskkill','/PID',str(process.pid),'/T','/F'],capture_output=True,timeout=2)
    process.wait(timeout=2)
    self.fail('actual drainer timed out: '+(self.root/'drainer.out').read_text())
  self.assertEqual(process.returncode,0,(self.root/'drainer.out').read_text())
  self.assertTrue((self.root/'lane-state/fake.completed').exists())
  self.assertIn('TURN_ENDED fake',self.alerts.read_text())
  self.assertIn('fake-codex-final',(self.root/'fake.log').read_text())
  self.assertFalse((self.root/'inbox/fake.drainer').exists())
 def test_status_instruction_is_not_a_final_status(self):
  self.assertEqual(m.status('Please end with LANE_STATUS: BLOCKED.\nLANE_STATUS: WORKING\n')[0],'WORKING')
 def test_drainer_queue_handoff_and_birth_custody(self):
  from unittest.mock import patch
  with patch.dict(os.environ,{'FLEET_ROOT':str(self.root),'FLEET_ALERTS':str(self.alerts)}):
   first=d.Drainer('fake');second=d.Drainer('fake')
   self.assertTrue(first.claim());self.assertFalse(second.claim())
   self.assertIsNone(first.next_batch()) # Empty decision and release under inbox lock.
   self.assertTrue(second.claim());first.release();self.assertTrue(second.owner.exists())
   second.release()
  owner=self.root/'inbox/fake.drainer';owner.mkdir();(owner/'owner.json').write_text(json.dumps(dict(pid=333,birth=self.now-30)))
  row=dict(pid=333,parent=1,birth=self.now-30,role='process')
  self.assertIn('DRAINER_CUSTODY',self.runpass([row]));self.assertTrue(owner.exists())
  row['birth']=self.now # Recycled PID does not retain ownership.
  self.assertIn('CLEARED_DEAD_DRAINER',self.runpass([row]));self.assertFalse(owner.exists())
 def test_drainer_launch_refusal_restores_original_batch(self):
  from unittest.mock import patch
  (self.root/'inbox/fake.md').write_text('original steer\n')
  with patch.dict(os.environ,{'FLEET_ROOT':str(self.root),'FLEET_ALERTS':str(self.alerts),'FLEET_CODEX':'never-real'}):
   dr=d.Drainer('fake')
   def run(command,**kwargs):
    if command[0]=='powershell':return subprocess.CompletedProcess(command,1)
    raise OSError('fixture launch refusal')
   with patch.object(d.subprocess,'run',side_effect=run):
    with self.assertRaises(OSError):dr.run()
   self.assertEqual(dr.pending.read_text(),'original steer\n');self.assertFalse(dr.owner.exists())
 def test_drainer_transient_orders_retry_before_new_steer(self):
  from unittest.mock import patch
  (self.root/'inbox/fake.md').write_bytes(b'old\n');turns=[]
  with patch.dict(os.environ,{'FLEET_ROOT':str(self.root),'FLEET_ALERTS':str(self.alerts),'FLEET_CODEX':'never-real','STEER_BACKOFF':'1'}):
   dr=d.Drainer('fake')
   def run(command,**kwargs):
    if command[0]=='powershell':return subprocess.CompletedProcess(command,1)
    turns.append(kwargs['stdin'].read())
    if len(turns)==1:
     kwargs['stdout'].write(b'ERROR: at capacity\n');dr.pending.write_bytes(b'new\n')
    else:(self.root/'fake.last.md').write_text('LANE_STATUS: DONE\n')
    return subprocess.CompletedProcess(command,1 if len(turns)==1 else 0)
   with patch.object(d.subprocess,'run',side_effect=run),patch.object(d.time,'sleep'):
    self.assertEqual(dr.run(),0)
   self.assertEqual(turns,[b'old\n',b'old\nnew\n']);self.assertFalse(dr.owner.exists())
 def test_drainer_unknown_probe_never_launches_codex(self):
  from unittest.mock import patch
  (self.root/'inbox/fake.md').write_bytes(b'pending\n')
  with patch.dict(os.environ,{'FLEET_ROOT':str(self.root),'FLEET_ALERTS':str(self.alerts),'FLEET_CODEX':'never-real'}):
   dr=d.Drainer('fake')
   with patch.object(d.subprocess,'run',return_value=subprocess.CompletedProcess([],2))as run,patch.object(d.time,'sleep',side_effect=OSError('fixture stop')):
    with self.assertRaises(OSError):dr.run()
   self.assertEqual(run.call_count,1);self.assertIn('PROCESS_PROBE',self.alerts.read_text());self.assertEqual(dr.pending.read_bytes(),b'pending\n')
 def test_provider_quota_preserves_batch_and_suppresses_retry(self):
  from unittest.mock import patch
  (self.root/'inbox/fake.md').write_bytes(b'old\n');turns=[]
  with patch.dict(os.environ,{'FLEET_ROOT':str(self.root),'FLEET_ALERTS':str(self.alerts),'FLEET_CODEX':'never-real'}):
   dr=d.Drainer('fake')
   def run(command,**kwargs):
    if command[0]=='powershell':return subprocess.CompletedProcess(command,1)
    turns.append(kwargs['stdin'].read());dr.pending.write_bytes(b'new\n')
    kwargs['stdout'].write('ERROR: Error running remote compact task: You\u2019ve hit your usage limit.\n'.encode())
    return subprocess.CompletedProcess(command,1)
   with patch.object(d.subprocess,'run',side_effect=run),patch.object(d.time,'sleep')as sleep:
    self.assertEqual(dr.run(),75);self.assertEqual(dr.run(),75);sleep.assert_not_called()
   self.assertEqual(turns,[b'old\n']);self.assertEqual(dr.pending.read_bytes(),b'old\nnew\n')
   self.assertEqual(json.loads(dr.provider_hold.read_text())['kind'],'usage-limit')
   self.assertFalse(dr.owner.exists());self.assertIn('PROVIDER_QUOTA fake',self.alerts.read_text())
   self.assertFalse(d.QUOTA.search('ERROR: at capacity\n')) # Short overload retains normal backoff.
   self.assertFalse(d.QUOTA.search('discussion: hit your usage limit\n'))
 def test_provider_hold_blocks_died_and_queued_dispatch_until_recovery(self):
  hold=self.root/'lane-state/fake.provider-hold.json';hold.write_text('{"kind":"usage-limit"}')
  (self.root/'inbox/fake.md').write_text('preserved\n')
  self.assertIn('PROVIDER_BLOCKED fake',self.runpass());self.assertFalse(self.sent)
  self.assertEqual(self.runpass().count('PROVIDER_BLOCKED fake'),1);self.assertFalse(self.sent)
  hold.unlink();self.assertIn('STARTED_DRAINER fake',self.runpass());self.assertTrue(self.sent)
 def test_usage_alert_threshold_footer_and_recovery(self):
  from fleet_usage import terminal_quota
  quota='ERROR: You have hit your usage limit.\ntokens used\n49,641,187\n'
  self.assertTrue(terminal_quota(quota))
  self.assertFalse(terminal_quota(quota+'OpenAI Codex v0.163.0\nnew work\n'))
  self.assertFalse(terminal_quota('discussion: hit your usage limit\n'))
  (self.root/'active-lanes.txt').write_text('fake\nsecond')
  (self.root/'fake.log').write_text(quota)
  self.assertNotIn('efficiency ANOMALY USAGE_LIMIT',self.runpass())
  (self.root/'second.log').write_text(quota)
  self.assertIn('efficiency ANOMALY USAGE_LIMIT lanes=2',self.runpass())
  self.assertEqual(self.runpass().count('efficiency ANOMALY USAGE_LIMIT'),1)
  (self.root/'second.log').write_text('successful work\ntokens used\n1\n')
  self.runpass();(self.root/'second.log').write_text(quota)
  self.assertEqual(self.runpass().count('efficiency ANOMALY USAGE_LIMIT'),2)
 def test_usage_scan_excludes_live_roots_and_stale_logs(self):
  from fleet_usage import scan
  (self.root/'fake.log').write_text('ERROR: hit your usage limit\n')
  self.assertEqual(len(scan(self.root,['fake'],self.now)['lanes']),1)
  self.assertEqual(scan(self.root,['fake'],self.now,['fake'])['lanes'],[])
  os.utime(self.root/'fake.log',(self.now-21601,self.now-21601))
  self.assertEqual(scan(self.root,['fake'],self.now)['lanes'],[])

if __name__=='__main__':
 start=time.monotonic();r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Machinery))
 receipt=dict(author='Codex',tests=r.testsRun,failures=len(r.failures),errors=len(r.errors),seconds=time.monotonic()-start,status='PASS' if r.wasSuccessful() else 'FAIL',bb2_jobs_started=0,live_files_changed=False,scope='controller integration and OS descriptor regression; staged Bash source contract; deployment smoke still required')
 (HERE/'self-test-result.json').write_text(json.dumps(receipt,indent=2)+'\n')
 raise SystemExit(not r.wasSuccessful())
