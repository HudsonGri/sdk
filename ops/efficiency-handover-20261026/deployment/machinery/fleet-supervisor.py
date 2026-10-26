"""Codex: sole staged supervisor controller; no build/test/gate verdict authority.

One read-only process snapshot per pass. Calls the existing single inbox drainer.
Completion receipts, birth-fenced child ownership and literal #PR parsing replace
mtime-only death inference, speculative WORKING resumes and SHA substring news.
"""
import argparse
import datetime as D
import json
import os
from pathlib import Path
import re
import subprocess
import time
from fleet_usage import scan as scan_usage

DEFAULT=Path('C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx')
STATUS_RULE='End with one LANE_STATUS: WORKING, WAITING_GATE #N, STANDBY <alert ERE>, BLOCKED <owner/reason>, or DONE.'


def load(path,default):
    try:return json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError,ValueError):return default


def atomic(path,value):
    temp=path.with_suffix(path.suffix+'.tmp')
    with temp.open('w',encoding='utf-8') as stream:
        json.dump(value,stream);stream.flush();os.fsync(stream.fileno())
    os.replace(temp,path)


def status(text):
    rows=re.findall(r'^\s*LANE_STATUS:\s*([A-Z_]+)([^\r\n]*)',text,re.M)
    return (rows[-1][0],rows[-1][1].strip()) if rows else ('WORKING','no final status')


def pr_numbers(text):
    return {int(n) for group in re.findall(r'#([0-9]+(?:\+[0-9]+)*)(?=[^0-9+]|$)',text) for n in group.split('+')}


def parse_events(text):
    events=[]
    for line in text.splitlines():
        fields=line.split(' ',2)
        if len(fields)!=3:continue
        try:when=D.datetime.fromisoformat(fields[0].replace('Z','+00:00')).timestamp()
        except ValueError:continue
        events.append(dict(at=when,source=fields[1],body=fields[2],line=line))
    return events


def activity(lane,rows,previous,last_mtime,now):
    """Rows contain metadata and owner labels only, never command/env bodies."""
    bypid={r['pid']:r for r in rows}
    owned={int(pid):birth for pid,birth in previous.items()
           if int(pid) in bypid and bypid[int(pid)]['birth']==birth}
    roots=[r for r in rows if lane in r.get('roots',[])]
    for row in rows:
        if lane in row.get('roots',[]) or lane in row.get('work',[]):owned[row['pid']]=row['birth']
    changed=True
    while changed:
        changed=False
        for row in rows:
            if row['parent'] in owned and row['pid'] not in owned and row['birth']>=owned[row['parent']]:
                owned[row['pid']]=row['birth'];changed=True
    running=any(last_mtime is None or row['birth']>last_mtime or now-last_mtime<=120 for row in roots)
    root_ids={r['pid'] for r in roots}
    busy=any(pid not in root_ids and bypid[pid].get('role') not in ['probe','drainer','supervisor'] for pid in owned)
    drainer=any(lane in row.get('drainers',[]) for row in rows)
    return dict(running=running,busy=busy,drainer=drainer,owners={str(k):v for k,v in owned.items()})


def process_snapshot(lanes):
    # PowerShell filters labels in memory; output has only process identity/ownership.
    script=r'''$ErrorActionPreference='Stop'; $rows=@(Get-CimInstance Win32_Process); $lanes=ConvertFrom-Json $env:FLEET_PROBE_LANES; $out=@();
foreach($p in $rows) {
 $roots=@();$work=@();$drainers=@(); $role='process';
 if($p.CommandLine -match 'lane-(running|busy)\.ps1|fleet-supervisor\.py'){$role='probe'}
 foreach($l in $lanes) {
  $e=[regex]::Escape($l)
  if($p.Name -notmatch '^(bash|sh|powershell|pwsh)\.exe$' -and $p.CommandLine -match 'codex' -and $p.CommandLine -match '\bexec\b' -and $p.CommandLine -match ('-o\s+[^\r\n]*[/\\]'+$e+'\.last\.md(?:["\s]|$)')){$roots+=,$l}
  if($p.CommandLine -match '(bs\.ps1|build-slot|buildbox|cargo|ssh|wsl)' -and $p.CommandLine -match ('[/\\](?:wt|lane-scratch)[/\\]'+$e+'(?:[/\\"\s]|$)')){$work+=,$l}
  if($p.Name -match '^(bash|sh)\.exe$' -and $p.CommandLine -match ('steer\.sh[" ]+'+$e+'(?:\s|$)')){$drainers+=,$l;$role='drainer'}
  if($p.Name -match '^python(?:w|[0-9.]*)?\.exe$' -and $p.CommandLine -match ('fleet-steer\.py[" ]+'+$e+'(?:\s|$)')){$drainers+=,$l;$role='drainer'}
 }
 $birth=([datetime]$p.CreationDate).ToUniversalTime(); $seconds=($birth-[datetime]'1970-01-01T00:00:00Z').TotalSeconds;
 $out+=,@{pid=[int]$p.ProcessId;parent=[int]$p.ParentProcessId;birth=$seconds;roots=@($roots);work=@($work);drainers=@($drainers);role=$role}
}; ConvertTo-Json -Depth 5 -Compress -InputObject @($out)'''
    result=subprocess.run(['powershell','-NoProfile','-Command',script],env={**os.environ,'FLEET_PROBE_LANES':json.dumps(lanes)},
                          text=True,capture_output=True,timeout=20)
    if result.returncode:raise RuntimeError('process query failed rc='+str(result.returncode))
    return json.loads(result.stdout)


class Supervisor:
    def __init__(self,root,alerts,gate,clock=time.time,dispatch=None):
        self.root=Path(root);self.alerts=Path(alerts);self.gate=Path(gate);self.clock=clock
        self.state=self.root/'lane-state';self.inbox=self.root/'inbox'
        self.state.mkdir(exist_ok=True);self.inbox.mkdir(exist_ok=True)
        self.dispatch=dispatch or self.start_drainer

    def alert(self,text):
        stamp=D.datetime.fromtimestamp(self.clock(),D.timezone.utc).isoformat().replace('+00:00','Z')
        with self.alerts.open('a',encoding='utf-8') as stream:stream.write(stamp+' lane-supervisor '+text.replace('\n',' ')+'\n')

    def usage_alert(self,lanes,rows,now):
        running=[lane for lane in lanes if any(lane in row.get('roots',[])for row in (rows or []))]
        result=scan_usage(self.root,lanes,now,running)
        refused=result['lanes'];path=self.state/'usage-limit-alert.json';previous=load(path,{})
        # Alert once on threshold crossing or changed affected lanes, reset after recovery.
        key=','.join(sorted(r['lane']for r in refused))if len(refused)>=2 else ''
        if key and previous.get('lanes')!=key:
            stamp=D.datetime.fromtimestamp(now,D.timezone.utc).isoformat().replace('+00:00','Z')
            with self.alerts.open('a',encoding='utf-8')as stream:
                stream.write(stamp+' efficiency ANOMALY USAGE_LIMIT lanes='+str(len(refused))+' names='+key+'; last turns refused; preserve batches and restore provider capacity\n')
        if key!=previous.get('lanes',''):atomic(path,dict(lanes=key,at=now))
        return result

    def start_drainer(self,lane,steer=None):
        command=[os.environ.get('FLEET_BASH','C:/Program Files/Git/usr/bin/bash.exe'),
                 (self.root/'steer.sh').as_posix(),lane]
        if steer:command.append(str(steer))
        with (self.root/('inbox-'+lane+'.drain.out')).open('ab') as output:
            subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,
                             close_fds=True,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))

    def steer(self,lane,reason,message,state):
        now=self.clock()
        if now-state.get('auto',0)<900:return False
        path=self.state/(lane+'.steer.md');path.write_text(message+' '+STATUS_RULE+'\n',encoding='utf-8')
        state.update(auto=now,autostart=now)
        atomic(self.state/(lane+'.supervisor.json'),state)
        self.alert('REINVOKE '+lane+' '+reason);self.dispatch(lane,path)
        return True

    def pass_once(self,rows):
        now=self.clock()
        tokens=(self.root/'active-lanes.txt').read_text().split() # EOF/newline independent.
        lanes=list(dict.fromkeys(x for x in tokens if re.fullmatch('[a-z0-9-]+',x)))
        if len(lanes)!=len(set(tokens)):self.alert('ANOMALY INVALID_LANE registry contains invalid names')
        if not lanes:self.alert('ANOMALY ZERO_LANES active-lanes.txt parsed zero lanes')
        self.usage_alert(lanes,rows,now)
        events=parse_events(self.alerts.read_text(encoding='utf-8',errors='replace'))
        counts=dict(running=0,idle=0,waiting_job=0)
        if rows is None:
            self.alert('ANOMALY PROBE_UNAVAILABLE process snapshot failed; dispatch held')
            self.alert(f'HEARTBEAT lanes={len(lanes)} running=0 idle=0 unknown={len(lanes)}')
            return counts
        for lane in lanes:
            path=self.state/(lane+'.supervisor.json');state=load(path,{})
            final=self.root/(lane+'.last.md');log=self.root/(lane+'.log')
            mtime=final.stat().st_mtime if final.exists() else None
            act=activity(lane,rows,state.get('owners',{}),mtime,now);state['owners']=act['owners']
            atomic(path,state)
            if act['running']:counts['running']+=1;continue
            counts['idle']+=1
            if act['busy']:
                counts['waiting_job']+=1
                if not state.get('waiting_job'):self.alert('WAITING_JOB '+lane+' live birth-fenced owned child/client')
                state['waiting_job']=True;atomic(path,state);continue
            if state.pop('waiting_job',False):
                self.alert('JOB_ENDED '+lane);state['auto']=0;atomic(path,state)
            provider_hold=self.state/(lane+'.provider-hold.json')
            if provider_hold.exists():
                key=provider_hold.stat().st_mtime_ns
                counts['provider_blocked']=counts.get('provider_blocked',0)+1
                if state.get('provider_hold')!=key:
                    self.alert('PROVIDER_BLOCKED '+lane+' usage-limit receipt; pending batch retained, restore provider capacity then explicitly clear '+str(provider_hold))
                    state['provider_hold']=key;atomic(path,state)
                continue
            if state.pop('provider_hold',None) is not None:
                state['auto']=0;atomic(path,state);self.alert('PROVIDER_HOLD_CLEARED '+lane)
            drainer=self.inbox/(lane+'.drainer')
            if drainer.exists() and not act['drainer']:
                ownerfile=drainer/'owner.json';owner=load(ownerfile,{})
                if owner and list(drainer.iterdir())==[ownerfile]:
                    peer=next((r for r in rows if r['pid']==owner.get('pid')),None)
                    if peer is None or abs(peer.get('birth',0)-owner.get('birth',0))>.01:
                        ownerfile.unlink()
                try:drainer.rmdir();self.alert('CLEARED_DEAD_DRAINER '+lane)
                except OSError:self.alert('ANOMALY DRAINER_CUSTODY '+lane+' nonempty/unknown lock retained');continue
            if act['drainer']:
                runlogs=list(self.inbox.glob(lane+'.run.*.log'))
                if runlogs and now-max(r.stat().st_mtime for r in runlogs)>900:
                    self.alert('HUNG_DRAINER '+lane+' quiet>15min; owned child query clear')
                continue
            lock=self.inbox/(lane+'.lock')
            if lock.exists() and now-lock.stat().st_mtime>300:
                # Only remove an empty stale lock after both owners are absent.
                # Unknown contents remain an explicit custody hold.
                try:lock.rmdir();self.alert('CLEARED_DEAD_INBOX_LOCK '+lane)
                except OSError:self.alert('ANOMALY INBOX_CUSTODY '+lane+' nonempty/unknown lock retained');continue
            if (self.inbox/(lane+'.md')).exists() and (self.inbox/(lane+'.md')).stat().st_size:
                self.dispatch(lane);self.alert('STARTED_DRAINER '+lane);continue
            if not log.exists():self.alert('ANOMALY MISSING_LANE_LOG '+lane);continue
            if mtime is None:
                self.steer(lane,'DIED','No final receipt exists. Inspect original turn failure, then continue.',state);continue
            completed=self.state/(lane+'.completed')
            completed_key=completed.read_text().strip() if completed.exists() else ''
            if completed_key!=str(int(mtime)) and log.stat().st_mtime>mtime+120:
                self.steer(lane,'DIED','No matching completion receipt and log advanced>120s. Inspect original failure.',state);continue
            kind,detail=status(final.read_text(encoding='utf-8',errors='replace')[-32768:])
            if kind in ['WAITING_GATE','STANDBY','DONE','BLOCKED']:
                state['quick']=0;state.pop('stuck',None)
            elif state.get('autostart') and state.get('autokey')!=mtime and mtime>=state['autostart']:
                state['autokey']=mtime
                state['quick']=state.get('quick',0)+1 if mtime-state['autostart']<600 else 0
            atomic(path,state)
            if kind in ['DONE','BLOCKED']:
                if state.get('reported')!=mtime:
                    state['reported']=mtime;atomic(path,state);self.alert('LANE_'+kind+' '+lane+': '+detail)
                continue
            if state.get('quick',0)>=4:
                if state.get('stuck')!=mtime:
                    state['stuck']=mtime;atomic(path,state);self.alert('LANE_STUCK '+lane+' four short WORKING turns; progress unknown')
                continue
            since=[r for r in events if r['at']>mtime and r['source']!='lane-supervisor']
            if kind=='WAITING_GATE':
                wanted=pr_numbers(detail)
                if not wanted:self.steer(lane,'NO_PR','WAITING_GATE needs a literal #PR.',state);continue
                news=[r['line'] for r in since if r['source']=='pr-gate' and
                      re.match(r'(GATE_[A-Z_]+|MERGE1|ROUTED) ',r['body']) and
                      wanted.intersection(pr_numbers(r['body'])) and 'queued for gate' not in r['body']]
                if news and state.get('news')!=mtime:
                    state['news']=mtime;state['auto']=0
                    self.steer(lane,'GATE_NEWS','Gate event: '+'; '.join(news[-3:]),state)
                elif not news and now-mtime>5400:
                    self.steer(lane,'GATE_RECHECK','No gate news90min. Inspect exact PR/queue/baseline state; preserve proof.',state)
            elif kind=='STANDBY':
                pattern=detail
                if len(pattern)>1 and pattern[0]==pattern[-1] and pattern[0] in "\"'":pattern=pattern[1:-1]
                if not pattern:self.steer(lane,'NO_PATTERN','STANDBY needs an alert ERE.',state);continue
                # ERE, binary tolerant C locale; no grep-P/PCRE locale dependency.
                result=subprocess.run(['grep','-aE','--',pattern],input='\n'.join(r['line'] for r in since),
                    text=True,capture_output=True,env={**os.environ,'LC_ALL':'C'},timeout=5)
                if result.returncode>1:self.alert('ANOMALY INVALID_STANDBY '+lane+' ERE rejected');continue
                if result.stdout and state.get('news')!=mtime:
                    state['news']=mtime;state['auto']=0;self.steer(lane,'EVENT',result.stdout[-1800:],state)
            else:self.steer(lane,'IDLE','WORKING turn ended; continue remaining scope after original proof inspection.',state)
        self.alert('HEARTBEAT lanes='+str(len(lanes))+' '+' '.join(f'{k}={v}' for k,v in counts.items()))
        for name in ['preparation-hold.json','freeze.json']:
            hold=self.gate/name
            if hold.exists() and now-hold.stat().st_mtime>900:
                self.alert('ANOMALY GATE_HOLD '+name+' >15min; typed owner recovery needed; qualification fence retained')
        return counts


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--once',action='store_true')
    args=parser.parse_args();root=Path(os.environ.get('FLEET_ROOT',str(DEFAULT)))
    alerts=Path(os.environ.get('FLEET_ALERTS',str(Path.home()/'.codex-fleet/alerts.log')))
    gate=Path(os.environ.get('FLEET_GATE_STATE','Q:/lanes/pr-gate'))
    control=root/'lane-supervisor.enabled';control.touch(exist_ok=True)
    supervisor=Supervisor(root,alerts,gate)
    while control.is_file():
        lanes=(root/'active-lanes.txt').read_text().split()
        try:rows=process_snapshot([l for l in lanes if re.fullmatch('[a-z0-9-]+',l)])
        except (OSError,ValueError,RuntimeError,subprocess.TimeoutExpired):rows=None
        supervisor.pass_once(rows)
        if args.once:break
        time.sleep(180)


if __name__=='__main__':main()
