"""Codex: bounded SDK lane-validation census; ledger and job logs only, no builds.

Full ledger identity includes uploaded-input hashes. Repetition is exposure, never
permission to omit a proof. Script names and log flags are descriptive metadata.
"""
import argparse,collections as C,datetime as D,hashlib,json,math,re,shlex,subprocess,time
from pathlib import Path

def date(s):
 return D.datetime.fromisoformat(s.replace('Z','+00:00'))

def tier(r):
 lane=r.get('lane') or str(r.get('cwd','')).replace('\\','/').split('/')[-1]
 if lane=='pr-gate' or re.search(r'\bpr-gate\b',r.get('tkey','')):return 'gate'
 if lane=='main-gate' or re.search(r'\bmain-gate\b',r.get('tkey','')):return 'main_gate'
 return 'lane_validation'

def metadata(r,text):
 cmd=str(r.get('cmd')or'');tags=set();scopes=set();stages=[];failures=[];binaries=set();suite_summaries=0
 # Read command metadata, never script bodies, environment files or credentials.
 scripts=sorted(set(re.findall(r'(?<![\w/-])([.\w-]+\.(?:py|sh|ps1))\b',cmd)))[:8]
 label=' '.join(scripts).lower()
 for tag,pattern in [('qualification',r'qualif|proof|self.?test|negative|baseline|control|conformance'),('windows',r'windows|win-qual|xwin'),('macos',r'macos|mac-live|darwin')]:
  if re.search(pattern,label):tags.add(tag+'_script_name')
 if re.search(r'baseline|main|before|original',label):tags.add('reference_script_name')
 # Flags inside retained logs are observations, not authoritative execution plans.
 for source,body in [('command',cmd),('log',text)]:
  if not re.search(r'\bcargo\b|\bxwin\b',body):continue
  for flag,tag in [('--workspace','workspace'),('--all-features','all_features'),('--all-targets','all_targets')]:
   if re.search(re.escape(flag)+r'(?=[\s\"\',\]]|$)',body):tags.add(tag+'_'+source)
  for scope in re.findall(r'(?:\s-p\s+|--package[=\s]+)[\"\']?([a-zA-Z0-9_-]+)',body):scopes.add(scope)
 for line in text.splitlines():
  binary=re.search(r'Running\s+(?:unittests\s+)?(.+?)\s+\(([^\n]*)\)',line)
  if binary:binaries.add(binary[2].replace('\\','/').rsplit('/',1)[-1][:160])
  if line.startswith('test result:'):suite_summaries+=1
  if re.match(r'^error(?:\[E\d+\]|:)|^test .+FAILED$|^(?:AssertionError|RuntimeError|Error):',line):
   # Keep only bounded diagnostics, never raw commands or arbitrary JSON payloads.
   if 'Command ' in line or 'Command failed' in line:continue
   if re.match(r'^(AssertionError|RuntimeError|Error):',line):
    failures.append(line.split(':',1)[0]+': diagnostic retained in original SDK log');continue
   failures.append(re.sub(r'\b\d+(?:\.\d+)?\b','#',line[:500]))
  if not line.startswith('{'):continue
  try:v=json.loads(line)
  except ValueError:continue
  if not isinstance(v,dict):continue
  seconds=v.get('seconds_with_compile',v.get('seconds'))
  name=next((v[k] for k in ['name','stage','label','kind'] if isinstance(v.get(k),str)),None)
  if name is None and 'seconds_with_compile'in v:name='compilation-inclusive'
  if name and isinstance(seconds,(int,float)) and math.isfinite(seconds) and seconds>=0:
   stages.append(dict(name=name[:160],seconds=seconds,rc=v.get('exit',v.get('rc',v.get('returncode')))))
 kind=('workspace' if 'workspace_command'in tags else 'all_features' if 'all_features_command'in tags else 'qualification' if 'qualification_script_name'in tags else 'platform' if tags&{'windows_script_name','macos_script_name'} else 'opaque_or_focused')
 return dict(scripts=scripts,tags=sorted(tags),scopes=sorted(scopes),kind=kind,stages=sorted(stages,key=lambda s:-s['seconds'])[:8],failure_signatures=sorted(set(failures))[:6],observed_test_binaries=len(binaries),observed_suite_summaries=suite_summaries)

def collect(start,end,ledger='/home/var/log/buildbox/jobs.jsonl',logroot='/home/var/log/buildbox'):
 rows=[];coverage=C.Counter();lo,hi=date(start),date(end)
 with open(ledger)as stream:
  for line in stream:
   try:r=json.loads(line)
   except ValueError:continue
   if r.get('repo')!='sdk' and not str(r.get('tkey','')).startswith('sdk/'):continue
   try:at=date(r['at'])
   except (KeyError,ValueError,TypeError):continue
   if not lo<=at<=hi:continue
   wall=float(r.get('wall')or 0)
   if wall<0 or not math.isfinite(wall):continue
   ident=str(r.get('log_id')or'');text='';truncated=False
   if tier(r)=='lane_validation' and re.fullmatch(r'[A-Za-z0-9_-]+',ident):
    path=Path(logroot)/(ident+'.log')
    try:
     size=path.stat().st_size
     with path.open('rb')as f:
      head=f.read(65536)
      if size>65536:f.seek(max(65536,size-65536));tail=f.read(65536)
      else:tail=b''
     text=(head+b'\n'+tail).decode('utf-8',errors='replace');truncated=size>131072
     coverage['logs_read']+=1;coverage['bytes_read']+=len(head)+len(tail);coverage['truncated_logs']+=truncated
    except OSError:coverage['missing_logs']+=1
   full=str(r.get('tree')or'');cmd=str(r.get('cmd')or'')
   executed=not r.get('schema_version') or bool(r.get('exec_start'))
   row=dict(at=r['at'],lane=r.get('lane') or str(r.get('cwd','')).replace('\\','/').split('/')[-1],tier=tier(r),log_id=ident,wall_hours=wall/3600,rc=r.get('rc'),termination=r.get('termination'),executed=executed,proof_replay=r.get('execution_kind')=='existing-verdict-replay',tree_hash=hashlib.sha256(full.encode()).hexdigest() if full else None,identity=hashlib.sha256((full+'\n'+cmd).encode()).hexdigest() if full and cmd else None,log_partial=truncated)
   row.update(metadata(r,text));rows.append(row)
 return dict(rows=sorted(rows,key=lambda r:date(r['at'])),coverage=dict(coverage))

def repeat_groups(rows):
 groups=C.defaultdict(list)
 for r in rows:
  if r.get('identity') and r['executed'] and not r['proof_replay']:groups[r['identity']].append(r)
 result=[]
 for identity,v in groups.items():
  if len(v)<2:continue
  # Generic assertion labels are insufficient to establish the same failure.
  failure_groups=C.Counter(tuple(r['failure_signatures'])for r in v if r['rc'] not in (None,0) and r['failure_signatures'] and not any('diagnostic retained'in s for s in r['failure_signatures']))
  same_failure_runs=max(failure_groups.values(),default=0)
  result.append(dict(identity=identity,lanes=sorted(set(r['lane']for r in v)),runs=len(v),hours=sum(r['wall_hours']for r in v),extra_hours=sum(r['wall_hours']for r in v[1:]),hours_beyond_two=sum(r['wall_hours']for r in v[2:]),same_failure_runs=same_failure_runs,scripts=v[0]['scripts'],log_ids=[r['log_id']for r in v],all_pass=all(r['rc']==0 for r in v),expected_control_candidate=any('qualification_script_name'in r['tags']for r in v)))
 return sorted(result,key=lambda r:-r['extra_hours'])

def summarize(value,start,end):
 allrows=value['rows'];rows=[r for r in allrows if r['tier']=='lane_validation'];lanes={};kinds={};tags={}
 def aggregate(group):return dict(jobs=len(group),hours=sum(r['wall_hours']for r in group),failed_jobs=sum(r['rc']not in(None,0)for r in group),failed_hours=sum(r['wall_hours']for r in group if r['rc']not in(None,0)))
 for lane in sorted(set(r['lane']for r in rows)):lanes[lane]=aggregate([r for r in rows if r['lane']==lane])
 for kind in sorted(set(r['kind']for r in rows)):kinds[kind]=aggregate([r for r in rows if r['kind']==kind])
 for tag in sorted(set(t for r in rows for t in r['tags'])):tags[tag]=aggregate([r for r in rows if tag in r['tags']])
 repeats=repeat_groups(rows);cross=C.defaultdict(set)
 for r in allrows:
  if r['tree_hash'] and r['executed'] and not r['proof_replay']:cross[r['tree_hash']].add(r['tier'])
 scripts=C.defaultdict(list);failtests=C.defaultdict(list)
 for r in rows:
  scripts[(r['lane'],' '.join(r['scripts'])or r['kind'])].append(r)
  if r['rc']not in(None,0):
   for finding in r['failure_signatures']:
    if finding.startswith('test ') and finding.endswith('FAILED'):failtests[(r['lane'],finding)].append(r)
 script_groups=[dict(lane=key[0],script_metadata=key[1],**aggregate(v),distinct_input_identities=len(set(r['identity']for r in v)),log_ids=[r['log_id']for r in v])for key,v in scripts.items()]
 failed_tests=[dict(lane=key[0],finding=key[1],**aggregate(v),distinct_input_identities=len(set(r['identity']for r in v)),log_ids=[r['log_id']for r in v],hours_after_two=sum(r['wall_hours']for r in v[2:]))for key,v in failtests.items()if len(v)>2]
 return dict(author='Codex',start=start,as_of=end,total_sdk_job_hours=sum(r['wall_hours']for r in allrows),lane_validation=aggregate(rows),per_lane=dict(sorted(lanes.items(),key=lambda x:-x[1]['hours'])),exclusive_kinds=kinds,overlapping_tags=tags,script_groups=sorted(script_groups,key=lambda r:-r['hours']),repeated_failed_tests=sorted(failed_tests,key=lambda r:-r['hours']),broad_suite_log_candidates=aggregate([r for r in rows if r['observed_test_binaries']>=10]),repeat_groups=repeats,repeat_extra_hours=sum(r['extra_hours']for r in repeats),hours_beyond_two=sum(r['hours_beyond_two']for r in repeats),cross_tier_tree_groups=sum(len(v)>1 for v in cross.values()),coverage=value['coverage'],rows=rows,limitations=['Job-hours are summed wrapper wall time, not exclusive box occupancy or CPU-hours.','Full ledger tree plus command is necessary but insufficient proof custody; mutable external inputs, scope, platforms, toolchain and controls still require qualification.','Script-name classes and bounded-log flags do not prove executed scope. Opaque scope is reported, never treated as redundant.','Expected negative controls and distinct required platforms must remain; repeated failures are candidates, not automatic skips.','Repeated-failed-test groups across changing inputs can overlap; hours after two is exposure, never an avoidable-cost sum.','Failure after two is observed only when retained bounded diagnostics match; unknown is not success.'])

def render(v):
 lines=['# Lane validation — Codex','',f"Endpoint {v['as_of']}; {v['lane_validation']['hours']:.6f} of {v['total_sdk_job_hours']:.6f} SDK job-hours ({100*v['lane_validation']['hours']/v['total_sdk_job_hours']:.2f}%). No bb2 jobs started.",'','| Lane | Jobs | Job-hours | Failed job-hours |','|---|---:|---:|---:|']
 for lane,r in list(v['per_lane'].items())[:20]:lines.append(f"| {lane} | {r['jobs']} | {r['hours']:.3f} | {r['failed_hours']:.3f} |")
 lines+=['','Exclusive kinds (command flags take precedence; script-name classes remain descriptive):','']
 for kind,r in v['exclusive_kinds'].items():lines.append(f"- {kind}: {r['jobs']} jobs / {r['hours']:.6f} jobh.")
 lines+=['','Overlapping tags, do not sum:','']
 for tag,r in v['overlapping_tags'].items():lines.append(f"- {tag}: {r['jobs']} jobs / {r['hours']:.6f} jobh.")
 lines+=['',f"Exact full-tree/command repetitions: {len(v['repeat_groups'])} groups; {v['repeat_extra_hours']:.6f} extra jobh; {v['hours_beyond_two']:.6f} jobh after two. These are investigation ceilings, not measured savings. Cross-tier same-tree groups: {v['cross_tier_tree_groups']} (not permission to reuse gate proof).",'', '| Repeated identity | Lanes | Runs | Extra jobh | Same-failure runs |','|---|---|---:|---:|---:|']
 for r in v['repeat_groups'][:12]:lines.append(f"| {r['identity'][:16]} | {', '.join(r['lanes'])} | {r['runs']} | {r['extra_hours']:.4f} | {r['same_failure_runs']} |")
 lines+=['','Repeated script metadata (changed inputs remain distinct):','']
 for r in v['script_groups'][:12]:lines.append(f"- {r['lane']} / `{r['script_metadata']}`: {r['jobs']} jobs / {r['hours']:.6f}jobh / {r['distinct_input_identities']} input identities.")
 lines+=['',f"Broad-suite log candidates (at least10 observed test binary labels): {v['broad_suite_log_candidates']}. Partial log observations do not establish full workspace scope.",'','Same failing test observed more than twice (overlapping changed-input investigations):','']
 for r in v['repeated_failed_tests'][:8]:lines.append(f"- {r['lane']} / `{r['finding']}`: {r['jobs']} failed jobs / {r['hours']:.6f}jobh; after two {r['hours_after_two']:.6f}jobh; {r['distinct_input_identities']} input identities.")
 lines+=['',f"Coverage: {v['coverage']}",'']+['- '+s for s in v['limitations']]
 return '\n'.join(lines)+'\n'

def main():
 p=argparse.ArgumentParser();p.add_argument('--collect-start');p.add_argument('--collect-end');a=p.parse_args()
 if a.collect_start:
  print(json.dumps(collect(a.collect_start,a.collect_end)));return
 root=Path(__file__).resolve().parent;m=json.loads((root/'latest.json').read_text());start,end=m['windows'][0]['start'],m['as_of'];clock=time.monotonic()
 source=Path(__file__).read_text();prefix='import sys\nsys.argv='+repr(['lane_validation.py','--collect-start',start,'--collect-end',end])+'\n'
 r=subprocess.run(['wsl','-d','Ubuntu','-e','python3','-'],input=prefix+source,text=True,encoding='utf-8',capture_output=True,check=True,timeout=45)
 v=summarize(json.loads(r.stdout),start,end);v['runtime_seconds']=time.monotonic()-clock
 expected=m['windows'][0]['bb2_tier_hours']['lane_validation'];assert abs(v['lane_validation']['hours']-expected)<1e-7,(v['lane_validation']['hours'],expected)
 for name,text in [('lane-validation.json',json.dumps(v,indent=2)+'\n'),('lane-validation.md',render(v))]:
  path=root/name;tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(text,encoding='utf-8');tmp.replace(path)
 print(json.dumps({k:v[k]for k in ['as_of','lane_validation','exclusive_kinds','repeat_extra_hours','hours_beyond_two','broad_suite_log_candidates','coverage','runtime_seconds']}))

if __name__=='__main__':main()
