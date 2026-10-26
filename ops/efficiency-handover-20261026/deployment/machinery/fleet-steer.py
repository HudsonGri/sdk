"""Codex: staged native inbox drainer; no tee and no per-step MSYS forks."""
import contextlib,datetime as D,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
DEFAULT='C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx'
TRANSIENT=re.compile(r'^ERROR:.*(?:at capacity|overloaded|rate limit|too many requests|429|502|503|stream disconnected|stream error|connection reset|timed out|401 Unauthorized|token_revoke|invalidated oauth)',re.I|re.M)
QUOTA=re.compile(r'^ERROR:.*(?:hit your usage limit|usage limit (?:reached|exceeded)|insufficient (?:credits|quota))',re.I|re.M)

def process_birth():
 import ctypes
 kernel=ctypes.WinDLL('kernel32',use_last_error=True);stamp=[ctypes.c_ulonglong()for _ in range(4)]
 kernel.GetProcessTimes.argtypes=[ctypes.c_void_p]+[ctypes.POINTER(ctypes.c_ulonglong)]*4
 if not kernel.GetProcessTimes(ctypes.c_void_p(-1),*(ctypes.byref(s)for s in stamp)):raise OSError('process birth unavailable')
 return stamp[0].value/10000000-11644473600

class Drainer:
 def __init__(self,lane):
  if not re.fullmatch(r'[a-z0-9][a-z0-9-]*',lane):raise ValueError('invalid lane')
  self.lane=lane;self.root=Path(os.environ.get('FLEET_ROOT',DEFAULT));self.inbox=self.root/'inbox';self.inbox.mkdir(exist_ok=True)
  (self.root/'lane-state').mkdir(exist_ok=True);self.pending=self.inbox/(lane+'.md');self.lock=self.inbox/(lane+'.lock');self.owner=self.inbox/(lane+'.drainer');self.nonce=uuid.uuid4().hex
  self.alerts=Path(os.environ.get('FLEET_ALERTS',str(Path.home()/'.codex-fleet/alerts.log')))
  self.provider_hold=self.root/'lane-state'/(lane+'.provider-hold.json')
 def alert(self,message):
  with self.alerts.open('a',encoding='utf-8')as f:f.write(D.datetime.now(D.timezone.utc).isoformat().replace('+00:00','Z')+' lane-supervisor '+message+'\n')
 @contextlib.contextmanager
 def locked(self):
  start=time.monotonic()
  while True:
   try:self.lock.mkdir();break
   except FileExistsError:
    if time.monotonic()-start>30:raise TimeoutError('inbox lock remains owned')
    time.sleep(.1)
  try:yield
  finally:self.lock.rmdir()
 def release(self):
  try:
   receipt=self.owner/'owner.json'
   if json.loads(receipt.read_text())['nonce']!=self.nonce:return
   receipt.unlink();self.owner.rmdir()
  except FileNotFoundError:pass
 def claim(self,steer=None):
  with self.locked():
   if steer:
    with self.pending.open('a',encoding='utf-8')as f:f.write('\n## Steer ('+Path(steer).name+', '+D.datetime.now(D.timezone.utc).isoformat()+')\n\n'+Path(steer).read_text())
   try:self.owner.mkdir()
   except FileExistsError:return False
   (self.owner/'owner.json').write_text(json.dumps(dict(nonce=self.nonce,pid=os.getpid(),birth=process_birth())))
  return True
 def next_batch(self):
  with self.locked():
   if not self.pending.exists() or not self.pending.stat().st_size:
    self.pending.unlink(missing_ok=True);self.release();return None
   batch=self.inbox/(self.lane+'.batch.'+self.nonce+'.md');self.pending.replace(batch);return batch
 def requeue(self,batch):
  body=batch.read_bytes()+(self.pending.read_bytes()if self.pending.exists()else b'');temp=self.pending.with_suffix('.md.new');temp.write_bytes(body);temp.replace(self.pending);self.discard(batch)
 def discard(self,path):
  try:path.unlink(missing_ok=True)
  except PermissionError:self.alert('FILE_RETAINED '+self.lane+' owned background descriptor may remain: '+path.name)
 def run(self,steer=None):
  if not self.claim(steer):return 0
  batch=None;delivered=False
  try:
   if self.provider_hold.exists():return 75 # Pending steers stay queued until explicit provider recovery.
   sid=None
   with (self.root/(self.lane+'.log')).open(errors='replace')as log:
    for line in log:
     found=re.search(r'^session id:\s+(\S+)',line)
     if found:sid=found[1];break
   if not sid:raise ValueError('missing session id')
   backoff=int(os.environ.get('STEER_BACKOFF','300'))
   while True:
    batch=self.next_batch()
    if batch is None:return 0
    delivered=False
    while True:
     probe=subprocess.run(['powershell','-NoProfile','-File',str(self.root/'lane-running.ps1'),self.lane,'-Mode','Busy'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30).returncode
     if probe==1:break
     if probe!=0:self.alert('ANOMALY PROCESS_PROBE '+self.lane+' step=lane-running recovery="powershell -NoProfile -File '+str(self.root/'lane-running.ps1')+' '+self.lane+'" dispatch_held')
     time.sleep(60)
    exe=os.environ.get('FLEET_CODEX')or shutil.which('codex')or shutil.which('codex.exe')
    if not exe:raise FileNotFoundError('Codex executable missing; set FLEET_CODEX')
    last=self.root/(self.lane+'.last.md');completed=self.root/'lane-state'/(self.lane+'.completed');completed.unlink(missing_ok=True)
    output=self.inbox/(self.lane+'.run.'+self.nonce+'.log');started=time.time_ns()
    command=[exe,'exec','-s','danger-full-access','-c','model_reasoning_effort=high','-C',os.environ.get('FLEET_WORKSPACE','Q:/sdk'),'-o',str(last),'resume',sid,'-']
    with batch.open('rb')as stdin,output.open('wb')as stdout:
     rc=subprocess.run(command,stdin=stdin,stdout=stdout,stderr=stdout,cwd=os.environ.get('FLEET_WORKSPACE','Q:/sdk')).returncode
    delivered=True
    valid=last.exists() and last.stat().st_mtime_ns>=started
    if valid:completed.write_text(str(int(last.stat().st_mtime)))
    self.alert('TURN_ENDED '+self.lane+' rc='+str(rc)+' completed='+str(int(valid)))
    with (self.root/(self.lane+'.log')).open('ab')as log,output.open('rb')as source:shutil.copyfileobj(source,log)
    with output.open('rb')as log:log.seek(max(0,output.stat().st_size-4000));tail=log.read().decode(errors='replace')
    if QUOTA.search(tail):
     delivered=False # Even a receipt-write failure must preserve this refused batch.
     recovery="After provider capacity is restored, Remove-Item -LiteralPath '"+str(self.provider_hold)+"'; then bash steer.sh "+self.lane
     receipt=dict(kind='usage-limit',at=D.datetime.now(D.timezone.utc).isoformat(),recovery=recovery)
     temporary=self.provider_hold.with_suffix('.json.new');temporary.write_text(json.dumps(receipt));temporary.replace(self.provider_hold)
     with self.locked():self.requeue(batch)
     batch=None;self.discard(output)
     self.alert('PROVIDER_QUOTA '+self.lane+' step=codex-resume batch_preserved dispatch_held recovery="'+recovery+'"')
     return 75
    if TRANSIENT.search(tail):
     with self.locked():self.requeue(batch)
     batch=None;self.discard(output);time.sleep(backoff);backoff=min(1800,backoff*2)
    else:
     self.discard(batch);batch=None;self.discard(output);backoff=int(os.environ.get('STEER_BACKOFF','300'))
  finally:
   with self.locked():
    if batch is not None and batch.exists() and not delivered:self.requeue(batch)
    self.release()

if __name__=='__main__':
 try:raise SystemExit(Drainer(sys.argv[1]).run(sys.argv[2]if len(sys.argv)>2 else None))
 except (OSError,ValueError,subprocess.SubprocessError)as e:
  print('STEER_REFUSED step=native-drainer recovery="inspect preserved inbox and native probe; rerun bash steer.sh <lane>" '+type(e).__name__,file=sys.stderr);raise SystemExit(2)
