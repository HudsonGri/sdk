"""Codex: passive throughput checks; only live mutation is append-only ANOMALY alerts.

Run after measure.py every cycle. Unavailable evidence is explicit, never healthy.
No builds, credential files, cloud worktrees, lane steering or gate state mutation.
"""
import argparse
import collections
import concurrent.futures
import datetime as D
import hashlib
import json
import os
from pathlib import Path
import posixpath
import re
import statistics
import subprocess
from machinery.fleet_usage import scan as scan_usage

ROOT = Path(__file__).resolve().parent
CX = Path('C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx')
LANES = Path('Q:/lanes')
ALERTS = Path.home() / '.codex-fleet/alerts.log'
UTC = D.timezone.utc


def date(value):
    try:
        return D.datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)
    except (ValueError, TypeError, AttributeError):
        return None


def read(path, default=None):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return default


def write(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    temp.replace(path)


def events(text):
    rows = []
    for line in text.splitlines():
        parts = line.split(' ', 2)
        when = date(parts[0]) if parts else None
        if when and len(parts) == 3:
            rows.append({'at': when.isoformat(), 'source': parts[1], 'body': parts[2]})
    return sorted(rows, key=lambda r: r['at'])


def prs(text):
    # Require the # marker; a decimal inside a SHA is never a PR reference.
    found = re.findall(r'#([0-9]+(?:\+[0-9]+)*)(?=[^0-9+]|$)', text)
    return sorted({int(n) for group in found for n in group.split('+')})


def evaluate(data):
    now = date(data['at'])
    rows = [r for r in data['events'] if date(r['at']) <= now]
    anomalies = []
    def add(kind, key, evidence, cause):
        anomalies.append(dict(kind=kind, key=key, evidence=evidence, suspected_cause=cause))
    refused=data.get('usage_limit',{}).get('lanes',[])
    if len(refused)>=2:
        add('USAGE_LIMIT','fleet',f"lanes={len(refused)} last turns ended with usage-limit: "+','.join(r['lane']for r in refused),
            'provider capacity exhausted; coordinator restores capacity, preserve refused batches and hold automatic retries')
    merges = [r for r in rows if r['source'] == 'pr-gate'
              and r['body'].startswith('MERGE1 ') and 'merge_rc=0' in r['body']]
    last_merge = date(merges[-1]['at']) if merges else None
    if data['queued'] and (last_merge is None or (now-last_merge).total_seconds() > 7200):
        add('MERGE_DROUGHT', 'queued', f"last successful merge={last_merge}; queued={data['queued']}",
            'baseline misattribution/holds or gate dispatch blocked; preserve exact main/candidate proof')
    outcomes = [r for r in rows if r['source'] == 'pr-gate' and re.match(
        r'GATE_(PASS|FAIL|CONFLICT|INFRA|PREPARATION_INFRA|REBASE|MERGECOMMIT|CLOSINGKEYWORD) #', r['body'])]
    recent = outcomes[-6:]
    nonpasses = sum(not r['body'].startswith('GATE_PASS ') for r in recent)
    if len(recent) == 6 and nonpasses > 3:
        add('GATE_FAILURE_RATE', 'last6', f'{nonpasses}/6 nonpassing outcomes; ' +
            '; '.join(r['body'].split(' ', 2)[0]+' '+str(prs(r['body'])) for r in recent),
            'inspect baseline custody, conflicts and actual candidate errors; no verification waiver')
    # Only recent failures; disjoint batches establish distinct PRs, not independent root causes.
    signatures = collections.defaultdict(list)
    failures = [r for r in outcomes if r['body'].startswith('GATE_FAIL ') and
                (now-date(r['at'])).total_seconds() <= 86400]
    for row in failures:
        for finding in re.findall(r'clippy: \+\d+ ([^;]+)', row['body']):
            signatures[finding.strip()].append(row)
    for finding, group in signatures.items():
        pairs = [(a,b) for i,a in enumerate(group) for b in group[i+1:]
                 if prs(a['body']) and prs(b['body']) and set(prs(a['body'])).isdisjoint(prs(b['body']))]
        if pairs:
            a,b=pairs[-1]
            add('CROSS_PR_FINDING', hashlib.sha256(finding.encode()).hexdigest()[:16],
                f"PRs {prs(a['body'])} and {prs(b['body'])}: {finding[:350]}",
                'likely inherited main finding or comparator custody/scope mismatch; not proof of innocence')
    test_groups = collections.defaultdict(list)
    for row in failures:
        match = re.search(r'new failures vs main \([^)]+\): (.+?)(?: log=|$)', row['body'])
        if match:
            for test in match[1].split():
                test_groups[test].append(row)
    for name, group in test_groups.items():
        if any(set(prs(a['body'])).isdisjoint(prs(b['body'])) for a in group for b in group
               if prs(a['body']) and prs(b['body'])):
            add('CROSS_PR_TEST', name, f'{name}: '+str([prs(r['body']) for r in group]),
                'possibly inherited main test failure; compare exact source/scope/host before attribution')
    for finding in data.get('untouched_findings', []):
        add('UNTOUCHED_FILE_FINDING', str(finding['pr'])+':'+finding['path'],
            json.dumps(finding, sort_keys=True),
            'possible inherited main finding or legitimate reverse-dependency regression; qualify comparison')
    route_errors = {}
    for row in rows:
        if row['source'] != 'pr-gate' or not row['body'].startswith('ROUTE_ERROR '):
            continue
        if (now-date(row['at'])).total_seconds() > 86400:
            continue
        verdict = re.match(r'ROUTE_ERROR #[0-9]+(?:\+[0-9]+)* (GATE_[A-Z_]+)(?:\s|$)', row['body'])
        if verdict:
            for number in prs(row['body']):
                route_errors[(number,verdict[1])] = row
    for (number,verdict), error in route_errors.items():
        recovered = any(row['source'] == 'pr-gate' and date(row['at']) > date(error['at'])
                        and re.match(r'ROUTED #[0-9]+(?:\+[0-9]+)* '+re.escape(verdict)+r'(?:\s|$)',row['body'])
                        and number in prs(row['body']) for row in rows)
        if not recovered:
            add('ROUTE_FAILURE', f'{number}:{verdict}', error['at']+' '+error['body'][:900],
                'failure dispatch/parser rejected the verdict; preserve the original failing step and deliver typed owner recovery before equivalent requeue')
    for hold in data.get('holds', []):
        if hold['age_seconds'] > 900:
            add('GATE_HOLD', hold['name'], json.dumps(hold),
                'held work needs typed owner recovery; do not release an unqualified baseline/head')
    supervisor = [r for r in rows if r['source'] == 'lane-supervisor']
    if not supervisor or (now-date(supervisor[-1]['at'])).total_seconds() > 600:
        add('SUPERVISOR_STALE', 'activity', 'last activity='+str(supervisor[-1]['at'] if supervisor else None),
            'supervisor stopped, blocked on probe, or alert sink unavailable')
    hearts = [r for r in supervisor if r['body'].startswith('HEARTBEAT ')]
    if not hearts or (now-date(hearts[-1]['at'])).total_seconds() > 600:
        add('SUPERVISOR_HEARTBEAT', 'heartbeat', 'last heartbeat='+str(hearts[-1]['at'] if hearts else None),
            'heartbeat contract not deployed or supervisor stalled; recent actions do not replace heartbeat')
    elif re.search(r'\blanes=0(?:\D|$)', hearts[-1]['body']):
        add('ZERO_LANES', 'heartbeat', hearts[-1]['body'], 'empty/malformed lane registry or failed lane discovery')
    for lane in data.get('lanes', []):
        if lane['state'] in ('IDLE','BLOCKED','STUCK') and lane['age_seconds'] > 3600:
            add('LANE_'+lane['state'], lane['lane'], json.dumps(lane),
                'owner dependency or dispatch/status failure; coordinator reassignment/recovery needed')
    for disk in data.get('disks', []):
        if disk.get('free_pct',100) < 15:
            add('DISK_PRESSURE', disk['host']+':'+disk['path'], json.dumps(disk),
                'cache/targets/job lifecycle may consume disk; inspect bounded category sizes before admission')
    walls = data.get('gate_walls', [])
    if len(walls) >= 3:
        median = statistics.median(r['seconds'] for r in walls)
        for row in walls:
            if row['seconds'] > median*1.5:
                add('GATE_SLOW', row['file'], f"{row['file']} {row['seconds']}s >1.5x median {median}s; mode={row.get('mode')}",
                    'phase/scope/input differences or wasted rebuild; compare matching scopes before optimization')
    return anomalies


def remote_disks(host):
    try:
        # Native df on either Linux or macOS; no user/config/environment file reads.
        transport=(['wsl','-d','Ubuntu','-e','ssh'] if host=='bb2'
                   else ['C:/Windows/System32/OpenSSH/ssh.exe'])
        result = subprocess.run(transport+['-o','BatchMode=yes',
            '-o','ConnectTimeout=5',host,'df -Pk / /mnt/bb' if host=='bb2' else 'df -Pk / /System/Volumes/Data'],
            text=True, capture_output=True, timeout=12)
        disks=[]
        for line in result.stdout.splitlines()[1:]:
            columns=line.split()
            if len(columns)>=6 and columns[1].isdigit() and columns[3].isdigit():
                disks.append(dict(host=host,path=columns[-1],total=int(columns[1])*1024,
                                  free=int(columns[3])*1024,free_pct=100*int(columns[3])/int(columns[1])))
        return disks, None if disks else f'{host} disk probe unavailable rc={result.returncode}'
    except (OSError,subprocess.TimeoutExpired) as error:
        return [], f'{host} disk probe unavailable: {type(error).__name__}'


def collect(now):
    data={'author':'Codex','at':now.isoformat(),'events':events(ALERTS.read_text(encoding='utf-8',errors='replace')),
          'queued':[],'holds':[],'lanes':[],'disks':[],'gate_walls':[],'untouched_findings':[],'coverage':[]}
    for name in ['pending.txt','active.txt','held-pending.txt','baseline-pending.txt']:
        path=LANES/'pr-gate'/name
        if path.exists():
            members=[int(x) for x in path.read_text().split() if x.isdigit()]
            data['queued'].extend(members)
            age=now.timestamp()-path.stat().st_mtime
            if members and name in ['held-pending.txt','baseline-pending.txt']:
                data['holds'].append(dict(name=name,prs=members,age_seconds=age))
    data['queued']=sorted(set(data['queued']))
    for name in ['preparation-hold.json','freeze.json']:
        path=LANES/'pr-gate'/name
        if path.exists():
            data['holds'].append(dict(name=name,age_seconds=now.timestamp()-path.stat().st_mtime))
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for disks,error in pool.map(remote_disks,['bb2','ivar']):
            data['disks'].extend(disks)
            if error:data['coverage'].append(error)
    # Metadata only. Running state uses a bounded process query, not the mutating lane-running.ps1.
    active=(CX/'active-lanes.txt').read_text().split()
    running=[]
    ps="$r=@(); Get-CimInstance Win32_Process | Where-Object { $_.Name -notmatch '^(bash|sh|powershell|pwsh)\\.exe$' -and $_.CommandLine -match 'codex.*exec.*-o\\s+[^ ]*[\\\\/]([^ /\\\\]+)\\.last\\.md' } | ForEach-Object { if ($_.CommandLine -match '-o\\s+[^ ]*[\\\\/]([^ /\\\\]+)\\.last\\.md') { $r += $Matches[1] } }; ConvertTo-Json -Compress -InputObject @($r)"
    try:
        result=subprocess.run(['powershell','-NoProfile','-Command',ps],text=True,capture_output=True,timeout=15)
        if result.returncode==0:running=json.loads(result.stdout or '[]')
        else:data['coverage'].append('running-lane process query failed; idle classification withheld')
    except (OSError,ValueError,subprocess.TimeoutExpired):
        data['coverage'].append('running-lane process query unavailable; idle classification withheld')
    for lane in active:
        if not re.fullmatch(r'[a-z0-9-]+',lane):continue
        path=CX/(lane+'.last.md')
        if not path.exists():continue
        text=path.read_text(encoding='utf-8',errors='replace')[-32768:]
        matches=re.findall(r'^LANE_STATUS:\s*([A-Z_]+)([^\r\n]*)',text,re.M)
        kind,detail=matches[-1] if matches else ('WORKING','no final status')
        state='RUNNING' if lane in running else ('IDLE' if kind=='WORKING' else kind)
        if any('process query' in x for x in data['coverage']) and state=='IDLE':state='UNKNOWN'
        data['lanes'].append(dict(lane=lane,state=state,detail=detail[:250],age_seconds=max(0,now.timestamp()-path.stat().st_mtime)))
    data['usage_limit']=scan_usage(CX,active,now.timestamp(),running)
    if data['usage_limit']['unavailable']:data['coverage'].append('Usage-limit logs unavailable: '+','.join(data['usage_limit']['unavailable']))
    cache=read(ROOT/'source-cache.json',{})
    for value in cache.values():
        if not isinstance(value,dict):continue
        row=value.get('value',{})
        if isinstance(row,dict) and row.get('pr_wall') and date(row.get('at')) and (now-date(row['at'])).total_seconds()<=86400:
            data['gate_walls'].append(dict(file=row['file'],seconds=row['pr_wall'][-1],mode=row.get('mode')))
    # Resolve touched paths ONLY from the exact routed failed head and pinned gate base.
    for event in data['events']:
        if event['source']!='pr-gate' or not event['body'].startswith('GATE_FAIL ') or (now-date(event['at'])).total_seconds()>86400:continue
        group=prs(event['body'])
        if len(group)!=1:continue
        n=group[0]
        route=next((r for r in data['events'] if r['source']=='pr-gate' and r['body'].startswith('ROUTED #'+str(n)+' ')
                    and 0<=(date(r['at'])-date(event['at'])).total_seconds()<180),None)
        log=re.search(r'log=([^ ]+)',event['body']);head=re.search(r'\bhead=([a-f0-9]{40})\b',route['body']) if route else None
        parsed=next((v.get('value',{}) for k,v in cache.items() if isinstance(v,dict) and log and k.replace('\\','/')==log[1].replace('\\','/')), {})
        base=parsed.get('scope_base')
        if not head or not base:continue
        try:
            result=subprocess.run(['git','-C','Q:/sdk','diff','--name-only',base+'...'+head[1]],capture_output=True,text=True,timeout=10)
            if result.returncode:continue
            touched=set(result.stdout.splitlines())
            for path in re.findall(r'clippy: \+\d+ ([A-Za-z0-9_./-]+\.rs):',event['body']):
                normalized=posixpath.normpath(path)
                if normalized not in touched:
                    data['untouched_findings'].append(dict(pr=n,head=head[1],base=base,path=normalized,touched_count=len(touched)))
        except (OSError,subprocess.TimeoutExpired):data['coverage'].append(f'#{n} exact diff unavailable')
    return data


def append_alerts(data, anomalies, path=ALERTS):
    # One line per anomaly per cycle; same cycle rerun is idempotent without a racy sidecar watermark.
    previous=path.read_text(encoding='utf-8',errors='replace') if path.exists() else ''
    lines=[]
    for item in anomalies:
        fingerprint=hashlib.sha256((data['at']+item['kind']+item['key']).encode()).hexdigest()[:16]
        if 'id='+fingerprint in previous:continue
        separator=' 'if item['kind']=='USAGE_LIMIT'else ': '
        message=f"{data['at']} efficiency ANOMALY {item['kind']}{separator}{item['evidence']}; suspected cause: {item['suspected_cause']}"
        lines.append(message.replace('\n',' ').replace('\r',' ')[:2360]+'; id='+fingerprint)
    if lines:
        # O_APPEND, one write per bounded line, never truncate an active shared stream.
        fd=os.open(path,os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
        try:
            for line in lines:os.write(fd,(line+'\n').encode('utf-8'))
            os.fsync(fd)
        finally:os.close(fd)
    return lines


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle',type=int,required=True)
    parser.add_argument('--no-append',action='store_true')
    args=parser.parse_args()
    data=collect(D.datetime.now(UTC));anomalies=evaluate(data)
    lines=[] if args.no_append else append_alerts(data,anomalies)
    # Event stream is already durable; keep this snapshot compact and metadata-only.
    data['anomalies']=anomalies;data['alert_lines_appended']=len(lines)
    write(ROOT/f'anomalies-cycle-{args.cycle}.json',data)
    print(json.dumps({'at':data['at'],'anomalies':len(anomalies),'appended':len(lines),
                      'kinds':dict(collections.Counter(a['kind'] for a in anomalies)),'coverage':data['coverage']}))


if __name__=='__main__':main()
