"""Bounded bb2 multiplier report; SDK data only, no builds. Author: Codex."""
import collections, datetime, json, pathlib, statistics, subprocess, tomllib
ROOT=pathlib.Path(__file__).parent

def command(args):
 return subprocess.check_output(args,text=True,encoding='utf-8')

def proof():
 rows=[]
 for filename in ['cache-before.log','cache-after-qualified.log']:
  for line in (ROOT/filename).read_text(errors='replace').splitlines():
   if line.startswith('EFFICIENCY_CACHE_CASE '):
    r=json.loads(line.split(' ',1)[1]); before=r.pop('before')['stats'];after=r.pop('after')['stats']
    count=lambda s,k:sum(s[k]['counts'].values())
    r.update(log=filename,hits=count(after,'cache_hits')-count(before,'cache_hits'),misses=count(after,'cache_misses')-count(before,'cache_misses'),compiler_executions=after['compilations']-before['compilations'])
    rows.append(r)
 return rows

def deps():
 text=command(['git','-C','Q:/sdk','show','origin/main:Cargo.lock'])
 packages=tomllib.loads(text)['package']; versions=collections.defaultdict(set); incoming=collections.Counter()
 for pkg in packages:
  versions[pkg['name']].add(pkg['version'])
  for entry in pkg.get('dependencies',[]):incoming[entry.split()[0]]+=1
 return sorted([{'crate':name,'versions':sorted(v),'lockfile_dependency_edges':incoming[name]} for name,v in versions.items() if len(v)>1],key=lambda x:(-x['lockfile_dependency_edges'],-len(x['versions']),x['crate']))

def host():
 code="""import json,os,pathlib
m=dict(line.split(':',1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines())
r={'at':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),'logical_cpus':os.cpu_count(),'load':os.getloadavg(),'mem_kib':{k:int(m[k].split()[0]) for k in ['MemTotal','MemAvailable']},'pressure':{k:pathlib.Path('/proc/pressure/'+k).read_text() for k in ['cpu','memory','io']},'admit':json.loads(pathlib.Path('/mnt/bb/admit/daemon.json').read_text())}
r['admit']={k:v for k,v in r['admit'].items() if k in ['limits','caps','source_sha256']}
print(json.dumps(r))
"""
 # stdin avoids nested shell quoting and never reads environment/credential bodies.
 r=subprocess.run(['wsl','-d','Ubuntu','-e','ssh','-o','BatchMode=yes','bb2','python3 -'],input=code,text=True,capture_output=True,check=True)
 return json.loads(r.stdout)

if __name__=='__main__':
 rows=proof(); baseline=[x['wall_seconds'] for x in rows if x['label'].startswith('baseline-')]; deployed=[x['wall_seconds'] for x in rows if x['label'].startswith('deployed-')]
 out={'author':'Codex','at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'proof':rows,'baseline_mean_seconds':statistics.mean(baseline),'cross_job_mean_seconds':statistics.mean(deployed),'cross_job_multiplier':statistics.mean(baseline)/statistics.mean(deployed),'steady_repeat_multiplier':baseline[-1]/deployed[-1],'duplicate_versions':deps(),'host':host(),'limitations':['Fixture is a2400-function library, not a production whole-build benchmark.','CPU telemetry covers waited payload children plus owned cache daemon/awaited descendants; preparation/export/unawaited children are not covered.','Existing ledgers cannot reconstruct historical CPU-hours, crate-feature duplicates, evictions or incremental hit rates.','Disk/cache contents and other worktrees are not inspected. No sizing/provider action.']}
 (ROOT/'compute-results.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
 print(json.dumps({k:out[k] for k in ['baseline_mean_seconds','cross_job_mean_seconds','cross_job_multiplier','steady_repeat_multiplier']}));print('duplicate crate names',len(out['duplicate_versions']));print(json.dumps(out['duplicate_versions'][:10]));print(json.dumps(out['host']))
