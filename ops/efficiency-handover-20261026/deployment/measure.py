"""Read-only SDK efficiency measurement. Author: Codex.

No builds, credentials, cloud paths, source worktrees or remote mutations.
Outputs and derived caches stay beside this script. Python 3.12, stdlib + gh + WSL.
Run: python Q:/lanes/efficiency/measure.py --date 2026-10-22
Repeated identical source snapshots do not append history. --offline reuses GitHub cache.
"""
import argparse
import collections as C
import datetime as D
import hashlib
import json
import math
import re
import statistics as S
import subprocess
import time
from pathlib import Path

UTC = D.timezone.utc
ROOT = Path(__file__).resolve().parent
SCRATCH = Path(r'C:\Users\varun\AppData\Local\Temp\claude\Q--sdk\c4cf6543-01de-41fc-994d-e3b8096f4e8d\scratchpad')
LANES = Path('Q:/lanes')
EXCLUDED = {485, 509, 677, 770, 796, 1025, 984}
SDK_WORKTREES = {'q:/sdk'}


def dt(value):
    try:
        if isinstance(value, (int, float)):
            return D.datetime.fromtimestamp(value, UTC)
        return D.datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def iso(value):
    return value.isoformat().replace('+00:00', 'Z')


def command(args, timeout=60):
    p = subprocess.run(args, capture_output=True, encoding='utf-8', errors='replace', timeout=timeout)
    if p.returncode:
        raise RuntimeError(f'{args[0]} failed (rc={p.returncode}); stderr withheld')
    return p.stdout


def read_json(path, default):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return default


def write_json(path, data):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    temp.replace(path)


def source_lines(path):
    with path.open(encoding='utf-8', errors='replace') as stream:
        yield from stream


def cached_file(path, parser, cache, signatures):
    stat = path.stat()
    key = str(path)
    sig = [stat.st_size, stat.st_mtime_ns]
    signatures.append([key, sig])
    old = cache.get(key, {})
    if old.get('sig') == sig:
        return old['value']
    result = parser(path)
    cache[key] = {'sig': sig, 'value': result}
    return result


def collect_file(path, parser, cache, signatures, errors, cutoff=None):
    try:
        if cutoff and dt(path.stat().st_mtime) < cutoff:
            return None
        return cached_file(path, parser, cache, signatures)
    except FileNotFoundError:
        errors.append(f'Source disappeared during scan: {path.name}; coverage incomplete')
        return None


def is_sdk_job(job):
    return job.get('repo') == 'sdk' or str(job.get('tkey', '')).startswith('sdk/')


def cycle_baseline(latest, end, history_path=None):
    if not latest or (dt(latest.get('as_of')) and dt(latest['as_of']) < end):
        return latest
    history_path = history_path or ROOT / 'metrics.jsonl'
    if history_path.exists():
        for line in reversed(history_path.read_text(encoding='utf-8').splitlines()):
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if dt(row.get('as_of')) and dt(row['as_of']) < end:
                return row
    return {}


def compute_metrics(jobs):
    """One owned-server counter set per job, never sum local and remote copies."""
    rows=[]
    for job in jobs:
        compute=(job.get('remote_receipt') or {}).get('compute') or job.get('compute')
        if isinstance(compute,dict):rows.append((job,compute))
    cpu=C.Counter(); samples=C.Counter(); hits=misses=compilations=0; cache_samples=0
    for job,row in rows:
        value=row.get('attributed_cpu_seconds')
        if isinstance(value,(int,float)) and math.isfinite(value) and value>=0:
            phase=job.get('phase') or job.get('label') or 'unclassified'
            cpu[phase]+=value/3600;samples[phase]+=1
        stats=(row.get('sccache') or {}).get('stats')
        if isinstance(stats,dict):
            cache_samples+=1
            for key in ['cache_hits','cache_misses']:
                count=sum(v for v in (stats.get(key,{}).get('counts') or {}).values()
                          if isinstance(v,(int,float)) and math.isfinite(v) and v>=0)
                if key=='cache_hits':hits+=count
                else:misses+=count
            compilations+=stats.get('compilations',0)
    target_rows=[]
    for job in jobs:
        target_rows.extend((job.get('remote_receipt') or {}).get('target_store') or job.get('target_store') or [])
    return {'receipts':len(rows),'cpu_samples':sum(samples.values()),
            'attributed_core_hours':sum(cpu.values()) if samples else None,
            'core_hours_by_phase':dict(cpu),'samples_by_phase':dict(samples),
            'sccache_samples':cache_samples,'cache_hits':hits,'cache_misses':misses,
            'cache_hit_rate':hits/(hits+misses) if hits+misses else None,
            'compiler_executions':compilations,
            'target_store_requests':len(target_rows),
            'target_store_states':dict(C.Counter(x.get('status','unknown') for x in target_rows)),
            'target_store_generations':dict(C.Counter(x.get('version','unknown') for x in target_rows)),
            'target_store_clone_seconds':sum(x.get('clone_seconds',0) for x in target_rows),
            'target_store_publish_seconds':sum(x.get('publish_seconds',0) for x in target_rows),
            'target_store_wait_seconds':sum(x.get('wait_seconds',0) for x in target_rows),
            'target_store_local_wait_samples':sum('local_wait_seconds' in x for x in target_rows),
            'target_store_local_wait_seconds':sum(x.get('local_wait_seconds',0) for x in target_rows),
            'note':'Partial coverage: waited child tree plus owned sccache CPU. Unawaited descendants and killed jobs can be absent. Phase is job label, not compile/test breakdown. No extrapolation to uninstrumented jobs.'}


def receipt_metrics(jobs):
    rows = [j for j in jobs if j.get('schema_version')]
    admission, execution = [], []
    admission_lanes = C.Counter()
    admission_examples = []
    execution_terminations = C.Counter()
    phase_rows = [j for j in rows if 'admission_backend' in j]
    slot_waits = []
    for j in phase_rows:
        try:
            value = float((j.get('admission') or {}).get('wait_seconds'))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value >= 0:
            slot_waits.append(value)
    for j in rows:
        start, admit = dt(j.get('exec_start')), dt(j.get('admission_start'))
        finish = dt((j.get('remote_receipt') or {}).get('end') or j.get('end'))
        if start and admit and start >= admit:
            seconds = (start-admit).total_seconds()
            admission.append(seconds)
            lane = j.get('lane') or str(j.get('cwd', '')).replace('\\', '/').split('/')[-1]
            admission_lanes[lane or 'unknown'] += seconds / 3600
            admission_examples.append({'lane': lane or 'unknown', 'seconds': seconds,
                                       'termination': j.get('termination', 'unknown'),
                                       'at': j.get('at')})
        if start and finish and finish >= start:
            seconds = (finish-start).total_seconds()
            execution.append(seconds)
            execution_terminations[j.get('termination', 'unknown')] += seconds / 3600
    return {'terminal_receipts': len(rows), 'legacy_rows': len(jobs)-len(rows),
        'with_exec_start': sum(bool(j.get('exec_start')) for j in rows),
        'with_pr_link': sum(bool(j.get('pr_number') or j.get('pr_numbers')) for j in rows),
        'terminations': dict(C.Counter(j.get('termination','unknown') for j in rows)),
        'termination_wrapper_hours': {k:sum(float(j['wall']) for j in rows if j.get('termination')==k)/3600
                                    for k in sorted({j.get('termination','unknown') for j in rows})},
        'execution_kind': dict(C.Counter(j.get('execution_kind') or 'unspecified' for j in rows)),
        'known_execution_hours': sum(execution)/3600, 'execution_samples': len(execution),
        'admission_overhead_hours': sum(admission)/3600, 'admission_samples': len(admission),
        'admission_overhead_median_seconds': S.median(admission) if admission else None,
        'admission_overhead_p90_seconds': sorted(admission)[min(len(admission)-1, int(.9*len(admission)))] if admission else None,
        'admission_overhead_p95_seconds': sorted(admission)[min(len(admission)-1, int(.95*len(admission)))] if admission else None,
        'admission_overhead_max_seconds': max(admission, default=None),
        'admission_overhead_lane_hours': dict(admission_lanes.most_common()),
        'admission_overhead_top_jobs': sorted(admission_examples, key=lambda x: -x['seconds'])[:10],
        'execution_termination_hours': dict(execution_terminations),
        'admission_phase_coverage': {'receipts':len(phase_rows),
            'backends':dict(C.Counter(j.get('admission_backend') for j in phase_rows)),
            'cache_states':dict(C.Counter(str(j.get('cache','unknown')) for j in phase_rows)),
            'slot_wait_samples':len(slot_waits),
            'slot_wait_hours':sum(slot_waits)/3600 if slot_waits else None,
            'slot_wait_p50_seconds':S.median(slot_waits) if slot_waits else None,
            'slot_wait_p95_seconds':sorted(slot_waits)[min(len(slot_waits)-1,int(.95*len(slot_waits)))] if slot_waits else None,
            'slot_wait_max_seconds':max(slot_waits,default=None)},
        'note': 'Admission overhead includes sync/probes/queue; not pure queue time. Nested remote receipts are not additional jobs. CPU counters have separate partial coverage.'}


def codex_counter_delta(current,previous):
    def inventory(rows):
        result={}
        for row in rows:
            for session in row.get('sessions',[]):
                sid=session['session_id'];result[sid]=max(result.get(sid,0),session['tokens'])
        return result
    now,old=inventory(current),inventory(previous)
    if not old.keys()<=now.keys()or any(now[sid]<value for sid,value in old.items()):return None
    return sum(value-old.get(sid,0)for sid,value in now.items())


def parse_codex(path):
    # Resumed sessions print cumulative counters, even on quota refusals.
    # Keep one high-water mark per session, never sum repeated resume footers.
    runs = []
    current = None
    pending = False
    for line in source_lines(path):
        line = line.strip()
        if re.fullmatch(r'OpenAI Codex v[^ ]+', line):
            if current is not None:
                runs.append(current)
            current = {'tokens': 0, 'complete': False, 'session_id': None}
        if current is None:
            continue
        if current['session_id'] is None:
            match=re.fullmatch(r'session id:\s+([0-9a-f-]{36})',line)
            if match:current['session_id']=match[1]
        if pending:
            if re.fullmatch(r'[\d,]+', line):
                current['tokens'] = max(current['tokens'], int(line.replace(',', '')))
                current['complete'] = True
            pending = False
        if line == 'tokens used':
            pending = True
    if current is not None:
        runs.append(current)
    sessions={};unattributed=0
    for run in runs:
        if not run['session_id']:
            unattributed+=int(run['complete']);continue
        sid=run['session_id'];row=sessions.setdefault(sid,dict(session_id=sid,tokens=0,runs=0,summaries=0))
        row['tokens']=max(row['tokens'],run['tokens']);row['runs']+=1;row['summaries']+=int(run['complete'])
    total=sum(s['tokens']for s in sessions.values());legacy=sum(r['tokens']for r in runs)
    return {'lane':path.stem,'tokens':total,'counter_basis':'session-high-water-v1',
            'sessions':list(sessions.values()),'legacy_sum_run_counters':legacy,
            'repeated_counter_overcount':legacy-total,'unattributed_summaries':unattributed,
            'completed_runs':sum(r['complete']for r in runs),'runs':len(runs)}


def parse_claude(path):
    messages = {}
    final_tokens = None
    for line in source_lines(path):
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if not isinstance(obj, dict):
            continue
        cwd = str(obj.get('cwd', '')).replace('\\', '/').lower()
        if cwd and not any(cwd == base or cwd.startswith(base + '/') for base in SDK_WORKTREES):
            continue
        if obj.get('subagent_tokens') is not None:
            final_tokens = obj['subagent_tokens']
        msg = obj.get('message', {})
        if not isinstance(msg, dict) or not msg.get('usage') or not obj.get('timestamp'):
            continue
        if not cwd:
            continue  # do not silently attribute unknown repositories
        u = msg['usage']
        fields = ['input_tokens', 'output_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens']
        usage = {k: int(u.get(k, 0) or 0) for k in fields}
        key = msg.get('id') or obj.get('uuid')
        if not key:
            continue
        old = messages.get(key)
        if old:
            usage = {k: max(usage[k], old['usage'][k]) for k in fields}
        messages[key] = {'id': key, 'at': obj['timestamp'], 'usage': usage,
                         'lane': obj.get('agentId') or path.stem,
                         'branch': obj.get('gitBranch')}
    return {'messages': list(messages.values()), 'final_subagent_tokens': final_tokens}


def parse_gate(path):
    # Only extract timings/identities, never retain arbitrary log bodies.
    out = {'file': path.name, 'at': iso(dt(path.stat().st_mtime)), 'pr_wall': [],
           'wall': [], 'feature_seconds': [], 'phase_seconds': {}, 'tree': None,
           'scope_head': None, 'scope_base': None, 'mode': None, 'infra_evidence': [], 'retry_elapsed_lower_bound': 0}
    for line in source_lines(path):
        for marker, field in [('GATE_PR_WALL', 'pr_wall'), ('GATE_WALL', 'wall'), ('GATE_FEATURES_WALL', 'feature_seconds')]:
            m = re.search(r'\b' + marker + r' seconds=([\d.]+)', line)
            if m:
                out[field].append(float(m[1]))
        m = re.search(r'GATE_FEATURES feature matrix: .*? in ([\d.]+)s', line)
        if m:
            out['feature_seconds'].append(float(m[1]))
        m = re.search(r'\b(GATE_[A-Z_]+_WALL) seconds=([\d.]+)', line)
        if m:
            out['phase_seconds'].setdefault(m[1], []).append(float(m[2]))
        m = re.search(r'BUILD_SLOT_TREE=([a-f0-9 ]+)', line)
        if m:
            out['tree'] = m[1].strip()
        if line.startswith('GATE_SCOPE '):
            try:
                scope = json.loads(line[len('GATE_SCOPE '):])
                out.update(scope_head=scope.get('head'), scope_base=scope.get('base'), mode=scope.get('mode'))
            except ValueError:
                pass
        for pattern, label in [(r'spot.*(reclaim|preempt)|reclaim.*spot', 'spot reclaim'),
                               (r'bb2.*unreachable|no build box reachable|vm-unreachable', 'unreachable'),
                               (r'ssh.*(rc.?255|timed out|reset by peer)', 'ssh failure')]:
            if re.search(pattern, line, re.I) and label not in out['infra_evidence']:
                out['infra_evidence'].append(label)
        m = re.search(r'retrying.*\((\d+)/\d+s\)', line)
        if m:
            out['retry_elapsed_lower_bound'] = max(out['retry_elapsed_lower_bound'], int(m[1]))
    out['seconds'] = sum(out['pr_wall'] or out['wall'])
    return out


def github(start, offline, errors):
    cache_path = ROOT / 'github-cache.json'
    saved = read_json(cache_path, {})
    if offline:
        return saved
    try:
        prs = json.loads(command(['gh', 'pr', 'list', '--repo', 'acyclic-labs/sdk', '--state', 'all',
                                  '--search', f'updated:>={start.date()}', '--limit', '500', '--json',
                                  'number,title,createdAt,mergedAt,closedAt,updatedAt,state,headRefName']))
        prs = [p for p in prs if p['number'] not in EXCLUDED]
        reviews = dict(saved.get('reviews', {}))
        prior_updates = {p['number']: p.get('updatedAt') for p in saved.get('prs', [])}
        changed_prs = [p for p in prs if str(p['number']) not in reviews or prior_updates.get(p['number']) != p.get('updatedAt')]
        for i in range(0, len(changed_prs), 15):
            parts = []
            for p in changed_prs[i:i + 15]:
                n = p['number']
                parts.append(f'p{n}:pullRequest(number:{n})' + '{reviews(first:100){totalCount nodes{submittedAt state}} reviewThreads(first:100){totalCount nodes{isResolved comments(first:100){nodes{body createdAt}}}}}')
            query = 'query{repository(owner:"acyclic-labs",name:"sdk"){' + ' '.join(parts) + '}}'
            data = json.loads(command(['gh', 'api', 'graphql', '-f', 'query=' + query]))
            for key, value in data.get('data', {}).get('repository', {}).items():
                if not value:
                    continue
                threads = value['reviewThreads']
                reviews[key[1:]] = {'rounds': value['reviews']['totalCount'],
                                    'submitted': value['reviews']['nodes'], 'threads': threads['totalCount'],
                                    'unresolved': sum(not t['isResolved'] for t in threads['nodes']),
                                    'p1': sum(any(re.search(r'\bP1\b', c['body']) for c in t['comments']['nodes']) for t in threads['nodes']),
                                    'p2': sum(any(re.search(r'\bP2\b', c['body']) for c in t['comments']['nodes']) for t in threads['nodes'])}
        result = {'at': iso(D.datetime.now(UTC)), 'prs': prs, 'reviews': reviews,
                  'possibly_truncated': len(prs) >= 500}
        write_json(cache_path, result)
        return result
    except (RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError) as exc:
        errors.append(f'GitHub refresh incomplete: {type(exc).__name__}; using previous cache if present')
        return saved


def intervals(jobs, start, end):
    events = []
    for j in jobs:
        b = dt((j.get('remote_receipt') or {}).get('end') or j.get('end') or j.get('at'))
        if not b:
            continue
        a = dt(j.get('exec_start'))
        if not a:
            if j.get('schema_version'):
                continue  # terminal pre-execution/replay receipt is not remote execution
            a = b - D.timedelta(seconds=float(j.get('wall', 0)))
        a, b = max(a, start), min(b, end)
        if b > a:
            events.extend([(a, 1), (b, -1)])
    counts = C.defaultdict(float)
    last, active = start, 0
    for at, delta in sorted(events):
        counts[active] += (at - last).total_seconds()
        active += delta
        last = at
    counts[active] += (end - last).total_seconds()
    return {'idle_hours_inferred': counts[0] / 3600,
            'busy_hours_inferred': sum(v for k, v in counts.items() if k > 0) / 3600,
            'concurrency_hours_inferred': {str(k): v / 3600 for k, v in sorted(counts.items()) if v},
            'peak_jobs_inferred': max(counts, default=0), 'saturated_hours': None,
            'note': 'Completed jobs only; receipt execution boundaries where present, legacy at/wall inferred; missing active jobs/capacity. Idle upper bound; saturation and CPU-hours unknown.'}


def tier(j):
    lane = j.get('lane') or str(j.get('cwd', '')).replace('\\', '/').split('/')[-1]
    if lane == 'pr-gate' or re.search(r'\bpr-gate\b', j.get('tkey', '')):
        return 'gate'
    if lane == 'main-gate' or re.search(r'\bmain-gate\b', j.get('tkey', '')):
        return 'main_gate'
    return 'lane_validation'


def gate_verdict_jobs(alerts, jobs):
    """Join each verdict to its latest completed attempt, not every equal tree.

    Fleet timestamps have second precision; allow the same second's receipt.
    Legacy receipts fall back to SHA only when no explicit gate log is present.
    """
    selected = set()
    attempts = [(i, job, dt(job.get('at'))) for i, job in enumerate(jobs)
                if tier(job) == 'gate' and (not job.get('phase') or job['phase'] == 'pr-gate')]
    for alert in alerts:
        filename = alert.get('log')
        at = dt(alert.get('at'))
        if not filename or not at:
            continue
        sha = filename.rsplit('-', 1)[-1].split('.')[0]
        candidates = []
        for i, job, finish in attempts:
            if not finish or not D.timedelta(seconds=-1) <= at-finish <= D.timedelta(hours=2):
                continue
            explicit = job.get('gate_log')
            if explicit:
                matches = str(explicit).replace('\\', '/').rsplit('/', 1)[-1] == filename
            else:
                matches = str(job.get('tree', '')).split(' ')[0].startswith(sha)
            if matches:
                candidates.append((finish, i))
        if candidates:
            selected.add(max(candidates)[1])
    return [job for i, job in enumerate(jobs) if i in selected]


def window(name, start, end, prs, reviews, jobs, all_jobs, gates, alerts, claude):
    def inside(value):
        at = dt(value)
        return at is not None and start <= at < end
    merged = [p for p in prs if inside(p.get('mergedAt'))]
    closed = [p for p in prs if p.get('state') == 'CLOSED' and inside(p.get('closedAt'))]
    hours = [(dt(p['mergedAt']) - dt(p['createdAt'])).total_seconds() / 3600 for p in merged]
    js = [j for j in jobs if inside(j.get('at'))]
    gs = [g for g in gates if inside(g['at'])]
    als = [a for a in alerts if inside(a['at'])]
    lane_tokens = C.defaultdict(lambda: C.Counter())
    for msg in claude:
        if inside(msg['at']):
            lane_tokens[msg['lane']].update(msg['usage'])
    tokens = sum(sum(u.values()) for u in lane_tokens.values())
    tiers = C.Counter()
    lanes = C.Counter()
    for j in js:
        tiers[tier(j)] += float(j.get('wall', 0)) / 3600
        lanes[str(j.get('cwd', '')).split('/')[-1]] += float(j.get('wall', 0)) / 3600
    by_file = {g['file']: g for g in gates}
    categories = {}
    predicates = {
        'infra_conflict_rebase': lambda a: a['event'] in ('GATE_INFRA', 'GATE_PREPARATION_INFRA', 'INFRA', 'GATE_CONFLICT', 'REBASE_NEEDED', 'GATE_REBASE'),
        'clippy_ratchet_failure': lambda a: a['event'] == 'GATE_FAIL' and a['clippy'],
        'batch_failure': lambda a: a['event'] == 'GATE_FAIL' and len(a['prs']) > 1,
        'all_gate_failure': lambda a: a['event'] == 'GATE_FAIL',
        'baseline_deferred': lambda a: a['event'] == 'GATE_BASELINE',
        'p2_blocked': lambda a: a.get('blocked', a['event'] == 'BLOCKED') and a['p2'],
        'review_thread_blocked': lambda a: a.get('blocked', False) and a.get('thread_count', 0) > 0,
    }
    for label, predicate in predicates.items():
        subset = [a for a in als if predicate(a)]
        files = sorted({a['log'] for a in subset if a['log']})
        seconds = sum(by_file[f]['seconds'] for f in files if f in by_file)
        matched_jobs = gate_verdict_jobs(subset, js)
        categories[label] = {'events': len(subset), 'prs': sorted({p for a in subset for p in a['prs']}),
                             'gate_wall_hours': seconds / 3600, 'tokens': None,
                             'logs': files, 'timed_logs': sum(f in by_file and by_file[f]['seconds'] > 0 for f in files),
                             'bb2_job_hours': sum(float(j['wall']) for j in matched_jobs) / 3600,
                             'matched_jobs': len(matched_jobs), 'ledger_matched_logs': sum(any(str(j.get('tree','')).split(' ')[0].startswith(f.rsplit('-',1)[-1].split('.')[0]) for j in matched_jobs) for f in files)}
    batch_events = [a for a in als if a['event']=='GATE_FAIL' and len(a['prs']) > 1]
    descendants = []
    for a in als:
        if a['event'] not in ('GATE_PASS', 'GATE_FAIL') or not a['log']:
            continue
        parents = [b for b in batch_events if dt(b['at']) < dt(a['at']) <= dt(b['at']) + D.timedelta(hours=2)
                   and set(a['prs']) < set(b['prs'])]
        if parents:
            descendants.append(a)
    child_jobs = gate_verdict_jobs(descendants, js)
    categories['batch_descendant_rerun_candidates'] = {'runs': len({a['log'] for a in descendants}),
        'bb2_job_hours': sum(float(j['wall']) for j in child_jobs)/3600, 'tokens': None,
        'note': 'Strict subset verdicts within 2h of failed batch; candidates, not proof all were avoidable. Overlaps other categories.'}
    conflict_sequences = C.defaultdict(list)
    for a in als:
        if a['event'] == 'GATE_CONFLICT':
            for p in a['prs']:
                conflict_sequences[p].append(a['at'])
    categories['repeated_conflict_candidates'] = {'repeated_events': sum(len(v)-1 for v in conflict_sequences.values()),
        'per_pr': {str(p): {'events':len(v), 'first':min(v), 'last':max(v),
                           'span_seconds':(dt(max(v))-dt(min(v))).total_seconds()}
                   for p,v in conflict_sequences.items() if len(v)>1},
        'tokens':None, 'bb2_hours':None,
        'note':'Repeated PR conflicts, not proof of identical head/base or actual Git conflicts; inspect diagnostic logs. Span is not CPU time.'}
    exact = C.defaultdict(list)
    tree_groups = C.defaultdict(list)
    for j in js:
        if j.get('execution_kind') == 'existing-verdict-replay':
            continue  # successful proof reuse is not a duplicate build
        if j.get('schema_version') and not j.get('exec_start'):
            continue  # admission-only attempts did not execute a build/test
        tree = j.get('tree')
        if tree:
            exact[(tree, j.get('cmd'))].append(j)
            tree_groups[tree].append(j)
    # A repeated failed command can be an intentional negative control; candidates only.
    repeats = [v for v in exact.values() if len(v) > 1]
    loops = [v for v in repeats if len(v) > 2]
    categories['duplicate_tree_command_candidates'] = {
        'groups': len(repeats), 'extra_jobs': sum(len(v) - 1 for v in repeats),
        'extra_wall_hours': sum(float(j['wall']) for v in repeats for j in v[1:]) / 3600, 'tokens': None,
        'examples': [{'lane': str(v[0].get('cwd','')).split('/')[-1], 'runs': len(v),
                      'extra_wall_hours': sum(float(j['wall']) for j in v[1:])/3600,
                      'identity_sha256': hashlib.sha256((v[0]['tree']+'\n'+v[0]['cmd']).encode()).hexdigest()[:16],
                      'all_pass': all(j.get('rc')==0 for j in v)}
                     for v in sorted(repeats, key=lambda v:-sum(float(j['wall']) for j in v[1:]))[:8]],
        'note': 'Exact full ledger tree + command; command may be an opaque script or intentional repeat. Not proven reusable verification.'}
    categories['validation_loop_candidates'] = {'groups': len(loops), 'jobs_beyond_two': sum(len(v) - 2 for v in loops),
        'wall_hours_beyond_two': sum(float(j['wall']) for v in loops for j in v[2:]) / 3600, 'tokens': None}
    categories['cross_tier_same_tree_candidates'] = {'groups': sum(len({tier(j) for j in v}) > 1 for v in tree_groups.values()), 'tokens': None, 'bb2_hours': None}
    heads = C.defaultdict(list)
    for g in gs:
        if g['scope_head'] and g['scope_base']:
            heads[(g['scope_base'], g['scope_head'])].append(g)
    categories['regate_same_base_head_candidates'] = {'groups': sum(len(v) > 1 for v in heads.values()),
        'extra_gate_wall_hours': sum(g['seconds'] for v in heads.values() for g in v[1:]) / 3600, 'tokens': None,
        'note': 'Same base+synthetic head; logs overwritten in place and gate version changes not observable.'}
    categories['abandoned_or_superseded'] = {'prs': [p['number'] for p in closed], 'tokens': None, 'bb2_hours': None}
    categories['discarded_long_jobs'] = {'failed_jobs_over_600s': sum(j.get('rc') != 0 and j['wall'] > 600 for j in js),
        'failed_job_wall_hours': sum(float(j['wall']) for j in js if j.get('rc') != 0 and j['wall'] > 600) / 3600,
        'discarded_hours': None, 'tokens': None, 'note': 'Failure does not prove discarded work; no PR/tree disposition join.'}
    categories['spot_reclaim'] = {'explicit_gate_logs': [g['file'] for g in gs if 'spot reclaim' in g['infra_evidence']],
        'unreachable_gate_logs': [g['file'] for g in gs if g['infra_evidence']], 'tokens': None, 'bb2_hours': None,
        'retry_elapsed_lower_bound_hours': sum(g['retry_elapsed_lower_bound'] for g in gs if g['infra_evidence'])/3600,
        'note': 'No causal outage accounting inferred from absence of completed jobs.'}
    categories['review'] = {'per_pr': {str(p['number']): reviews.get(str(p['number']), {}) for p in prs if inside(p.get('updatedAt'))},
        'tokens': None, 'bb2_hours': None, 'avoidable_p1': None,
        'note': 'Review submissions are a rounds proxy; P1 label alone does not establish self-review avoidability. Current thread snapshot, not historical blocking duration.'}
    categories['idle_lanes_with_work'] = {'lane_hours': None, 'tokens': None, 'bb2_hours': None,
        'note': 'Need coordinator dispatch/ready/queue events; absent completion events do not establish lane idleness.'}
    features = [x for g in gs for x in g['feature_seconds']]
    main_alerts = [a for a in als if a['event'] == 'MAIN_GATE']
    return {'name': name, 'start': iso(start), 'end': iso(end), 'merged_prs': merged, 'merged_count': len(merged),
        'open_to_merge_hours': {'median': S.median(hours) if hours else None, 'p90': sorted(hours)[min(len(hours)-1, int(.9*len(hours)))] if hours else None},
        'claude_tokens': tokens, 'claude_tokens_per_merge': tokens / len(merged) if merged else None,
        'claude_per_lane': dict(lane_tokens), 'codex_tokens': None, 'codex_tokens_per_merge': None,
        'bb2_jobs': len(js), 'bb2_job_hours': sum(tiers.values()), 'bb2_job_hours_per_merge': sum(tiers.values()) / len(merged) if merged else None,
        'bb2_tier_hours': dict(tiers), 'bb2_tier_hours_per_merge': {k:v/len(merged) if merged else None for k,v in tiers.items()},
        'bb2_lane_hours': dict(lanes), 'occupancy_all_repositories': intervals(all_jobs, start, end),
        'receipt_coverage': receipt_metrics(js),
        'compute':compute_metrics(js),
        'gate_logs': len(gs), 'gate_logged_hours': sum(g['seconds'] for g in gs) / 3600,
        'main_gate_alerts': len(main_alerts), 'main_gate_runs_per_merge': len(main_alerts) / len(merged) if merged else None,
        'batch_verdicts': dict(C.Counter(a['event'] for a in als if a['event'] in ('GATE_PASS','GATE_FAIL') and len(a['prs']) > 1)),
        'feature_matrix_seconds': {'samples': len(features), 'min': min(features) if features else None,
            'median': S.median(features) if features else None, 'max': max(features) if features else None, 'sum_hours': sum(features)/3600},
        'waste': categories}


def main():
    begin = time.perf_counter()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--date', default=D.datetime.now(UTC).date().isoformat())
    ap.add_argument('--as-of', help='Fixed UTC endpoint for reproducible historical snapshots')
    ap.add_argument('--offline', action='store_true')
    args = ap.parse_args()
    day = dt(args.date + 'T00:00:00Z')
    end = min(dt(args.as_of) if args.as_of else D.datetime.now(UTC), day + D.timedelta(days=1))
    if end <= day:
        ap.error('endpoint must follow the requested day start')
    latest = read_json(ROOT / 'latest.json', {})
    previous = cycle_baseline(latest, end)
    errors, signatures = [], []
    try:
        listing = command(['git', '-C', 'Q:/sdk', 'worktree', 'list', '--porcelain'])
        SDK_WORKTREES.update(line[9:].replace('\\', '/').lower().rstrip('/')
                             for line in listing.splitlines() if line.startswith('worktree '))
    except (RuntimeError, subprocess.TimeoutExpired):
        errors.append('SDK worktree metadata unavailable; only Q:/sdk Claude cwd accepted')
    cache_path = ROOT / 'source-cache.json'
    cache = read_json(cache_path, {})
    cache_version = hashlib.sha256(('v4' + '|'.join(sorted(SDK_WORKTREES))).encode()).hexdigest()
    if cache.get('_version') != cache_version:
        cache = {'_version': cache_version}
    for path,record in list(cache.items()):
        if path.endswith('.log') and isinstance(record,dict):
            value=record.get('value',{})
            if isinstance(value,dict) and 'completed_runs'in value and value.get('counter_basis')!='session-high-water-v1':del cache[path]
    codex = [v for p in (SCRATCH / 'cx').glob('*.log')
             if re.fullmatch(r'[a-z0-9][a-z0-9-]*',p.stem)
             and (v := collect_file(p, parse_codex, cache, signatures, errors)) is not None]
    codex_observed_at=D.datetime.now(UTC)
    messages = {}
    task_count = 0
    for p in (SCRATCH.parent / 'tasks').glob('*.output'):
        task_count += 1
        value = collect_file(p, parse_claude, cache, signatures, errors, end-D.timedelta(days=2))
        if value is None:
            continue
        for m in value['messages']:
            old = messages.get(m['id'])
            if old:
                m = dict(m, usage={k:max(v, old['usage'].get(k, 0)) for k,v in m['usage'].items()})
            messages[m['id']] = m
    gates = []
    for folder in ('pr-gate', 'main-gate'):
        for p in (LANES / folder).glob('*.log'):
            if not re.match(r'^\d+(?:\+\d+)*-[a-f0-9]+\.log$' if folder == 'pr-gate' else r'^(?:ws-)?[a-f0-9]{7,40}\.log$', p.name):
                continue
            if folder == 'pr-gate' and any(int(n) in EXCLUDED for n in p.name.split('-')[0].split('+')):
                continue
            value = collect_file(p, parse_gate, cache, signatures, errors, end-D.timedelta(days=2))
            if value is not None:
                gates.append(value)
    alerts = []
    alert_path = Path.home() / '.codex-fleet' / 'alerts.log'
    if alert_path.exists():
        stat = alert_path.stat()
        signatures.append(['alerts', [stat.st_size, stat.st_mtime_ns]])
        for line in source_lines(alert_path):
            # SDK-only service emitters; ignore mixed fleet lane output entirely.
            m = re.match(r'^(\S+) (pr-gate|main-gate) (\S+)\s+(.*)', line)
            if not m:
                continue
            at, service, event, text = m.groups()
            numbers = [int(x) for match in re.findall(r'#(\d+(?:\+\d+)*)', text) for x in match.split('+')]
            if any(n in EXCLUDED for n in numbers):
                continue
            log = re.search(r'log=Q:/lanes/(?:pr-gate|main-gate)/([^\s]+)', text)
            thread_counts = [int(x) for x in re.findall(r'threads=(\d+)', text)]
            alerts.append({'at': at, 'event': event, 'prs': sorted(set(numbers)), 'log': log[1] if log else None,
                           'clippy': 'clippy' in text.lower(), 'p2': bool(re.search(r'\bP2\b', text)),
                           'blocked': 'BLOCKED' in text, 'thread_count': max(thread_counts, default=0)})
    else:
        errors.append('Fleet alerts missing')
    jobs, all_jobs = [], []
    # Filter remotely before returning; no non-SDK paths/commands are retained.
    remote = '''import json
for l in open('/home/var/log/buildbox/jobs.jsonl'):
 try: d=json.loads(l)
 except ValueError: continue
 if d.get('repo')=='sdk' or str(d.get('tkey','')).startswith('sdk/'):
  print(json.dumps(d))
 else:
  print(json.dumps({k:d.get(k) for k in ('at','wall')}))
'''
    try:
        data = command(['wsl', '-e', 'python3', '-c', remote])
        for line in data.splitlines():
            try:
                j = json.loads(line)
            except ValueError:
                continue
            all_jobs.append(j)
            if is_sdk_job(j):
                jobs.append(j)
        signatures.append(['ledger', hashlib.sha256(data.encode()).hexdigest()])
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        errors.append(f'WSL job ledger unavailable: {type(exc).__name__}')
    gh = github(end - D.timedelta(days=2), args.offline, errors)
    signatures.append(['github', hashlib.sha256(json.dumps({k:v for k,v in gh.items() if k!='at'}, sort_keys=True).encode()).hexdigest()])
    write_json(cache_path, cache)
    prs = gh.get('prs', [])
    reviews = gh.get('reviews', {})
    windows = [window('last_24h', end-D.timedelta(hours=24), end, prs, reviews, jobs, all_jobs, gates, alerts, messages.values()),
               window('today_utc', day, end, prs, reviews, jobs, all_jobs, gates, alerts, messages.values())]
    prev_end = dt(previous.get('as_of'))
    if prev_end and prev_end < end:
        windows.append(window('since_last_cycle', prev_end, end, prs, reviews, jobs, all_jobs, gates, alerts, messages.values()))
    old_codex = {x['lane']: x for x in previous.get('codex_inventory_undated', [])}
    compatible=previous and all(x.get('counter_basis')=='session-high-water-v1'for x in previous.get('codex_inventory_undated',[]))
    codex_delta = {x['lane']:codex_counter_delta([x],[old_codex[x['lane']]]if x['lane']in old_codex else [])for x in codex}if compatible else None
    previous_observation=dt(previous.get('codex_inventory_observed_at'))or prev_end
    observation_hours=(codex_observed_at-previous_observation).total_seconds()/3600 if compatible and previous_observation else None
    observed_rates={lane:value/observation_hours if value is not None else None for lane,value in codex_delta.items()}if codex_delta is not None and observation_hours else None
    fleet_delta=codex_counter_delta(codex,previous.get('codex_inventory_undated',[]))if compatible else None
    signatures.append(['algorithm', hashlib.sha256(Path(__file__).read_bytes()).hexdigest()])
    fingerprint = hashlib.sha256(json.dumps(sorted(signatures, key=lambda x:x[0]), sort_keys=True).encode()).hexdigest()
    result = {'schema': 1, 'author': 'Codex', 'as_of': iso(end), 'requested_date_utc': args.date,
        'fingerprint': fingerprint, 'windows': windows, 'codex_inventory_undated': codex,
        'codex_newly_observed_since_cycle': codex_delta, 'claude_task_files': task_count,
        'codex_inventory_observed_at':iso(codex_observed_at),
        'codex_observation_hours':observation_hours,'codex_observed_tokens_per_lane_hour':observed_rates,
        'codex_observed_tokens_per_merge':fleet_delta/windows[-1]['merged_count']if fleet_delta is not None and windows[-1]['merged_count']else None,
        'claude_unique_messages': len(messages), 'errors': errors, 'github_as_of': gh.get('at'),
        'runtime_seconds': round(time.perf_counter()-begin, 3),
        'limitations': ['Codex resumed counters are session-cumulative, deduplicated by session high-water. Completed summaries lack trustworthy run timestamps: inventory is not24h/account usage; deltas are newly observed counters, not necessarily tokens spent in the interval. Old sum-of-resumes inventories are superseded, not comparable.',
            'Claude gross tokens include cache reads/creation, deduplicated by message ID using component-wise maximum; not dollars or directly comparable to Codex summaries. Coordinator usage outside task files unavailable.',
            'Claude per-agent identity is available, but a reliable agent-to-lane mapping is absent. Branch main is shared by many agents and must not be treated as one lane.',
            'Per-lane tokens are supplied; PR attribution is unknown without run-to-PR/tree events. Tokens per merge are fleet window ratios, not PR lifecycle costs.',
            'Gate times use terminal alert time where available, otherwise filesystem mtime (proxy); overwritten same-name logs lose earlier runs.',
            'Waste categories overlap. Gate logged wall includes orchestration, is not CPU time and is not additive to ledger job-hours.',
            'Receipt execution/admission timestamps are reported separately; admission includes sync/probes, not just queue. New compute counters have explicit partial coverage; legacy CPU-hours and saturation remain unknown.',
            'SDK-tagged jobs only; older untagged records excluded rather than guessed. GitHub review pagination capped at 100 entries per PR. Closed-unmerged is not proof of wasted work.',
            'Claude task mtime filter retains two days around endpoint; old files modified later remain scanned. Historical backfills require retained sources.']}
    # Alert timestamps are more trustworthy than mutable log mtimes; re-evaluate.
    times = C.defaultdict(list)
    for a in alerts:
        if a['log'] and a['event'] in ('GATE_PASS', 'GATE_FAIL', 'GATE_INFRA', 'MAIN_GATE'):
            times[a['log']].append(a['at'])
    changed = False
    for g in gates:
        if times[g['file']]:
            g['at'] = max(times[g['file']])
            changed = True
    if changed:
        result['windows'] = [window(w['name'], dt(w['start']), dt(w['end']), prs, reviews, jobs, all_jobs, gates, alerts, messages.values()) for w in windows]
    if (latest.get('fingerprint') == fingerprint and latest.get('requested_date_utc') == args.date
            and (not args.as_of or latest.get('as_of') == iso(end))):
        print('No source changes; metrics and history unchanged.')
        return
    write_json(ROOT / 'latest.json', result)
    with (ROOT / 'metrics.jsonl').open('a', encoding='utf-8') as f:
        f.write(json.dumps(result, ensure_ascii=False) + '\n')
    write_markdown(result)
    print(json.dumps({'as_of': result['as_of'], 'seconds': result['runtime_seconds'], 'windows': [{k:w[k] for k in ('name','merged_count','bb2_jobs','bb2_job_hours','claude_tokens')} for w in result['windows']], 'errors': errors}))


def fmt(value):
    return 'unknown' if value is None else f'{value:,.3f}' if isinstance(value, float) else str(value)


def write_markdown(r):
    lines = ['# SDK efficiency metrics', '', f"Author: Codex. Endpoint: {r['as_of']}. Runtime: {r['runtime_seconds']} s. No bb2 jobs started.", '',
        f"Re-run: `python -X utf8 Q:/lanes/efficiency/measure.py --date {r['requested_date_utc']}` (`--offline` reuses GitHub metadata). History: `metrics.jsonl`; structured snapshot: `latest.json`.", '',
        'Progress denominator is all observed SDK merges, not a certified count of roadmap-advancing PRs; roadmap relevance needs coordinator classification. Verification unchanged.', '']
    for w in r['windows']:
        lines += [f"## {w['name']}: {w['start']} to {w['end']}", '', '| Measure | Value |', '|---|---:|',
            f"| Merged PRs | {w['merged_count']} |", f"| Median / p90 open-to-merge hours | {fmt(w['open_to_merge_hours']['median'])} / {fmt(w['open_to_merge_hours']['p90'])} |",
            f"| Claude gross tokens / merge | {w['claude_tokens']:,} / {fmt(w['claude_tokens_per_merge'])} |",
            '| Codex tokens in window / merge | unknown (undated summaries) |',
            f"| bb2 completed SDK job-hours / merge | {fmt(w['bb2_job_hours'])} / {fmt(w['bb2_job_hours_per_merge'])} |",
            f"| bb2 completed SDK jobs | {w['bb2_jobs']} |", f"| Main-gate alerts / merge | {w['main_gate_alerts']} / {fmt(w['main_gate_runs_per_merge'])} |",
            f"| Logged feature matrix seconds min / median / max | {fmt(w['feature_matrix_seconds']['min'])} / {fmt(w['feature_matrix_seconds']['median'])} / {fmt(w['feature_matrix_seconds']['max'])} |", '',
            '| bb2 tier | Job-hours | Job-hours / merge |', '|---|---:|---:|']
        for k,v in w['bb2_tier_hours'].items():
            lines.append(f"| {k} | {fmt(v)} | {fmt(w['bb2_tier_hours_per_merge'][k])} |")
        lines += ['', 'Terminal receipt coverage: `' + json.dumps(w['receipt_coverage']) + '`', '',
                  'Compute/cache coverage: `' + json.dumps(w['compute']) + '`', '']
        receipt=w['receipt_coverage']; waits=receipt['admission_phase_coverage']
        lines += [f"Admission overhead p50 / p95: {fmt(receipt['admission_overhead_median_seconds'])} / {fmt(receipt.get('admission_overhead_p95_seconds'))} s (sync/probes included). Measured slot wait p50 / p95: {fmt(waits.get('slot_wait_p50_seconds'))} / {fmt(waits.get('slot_wait_p95_seconds'))} s; {waits['slot_wait_samples']} samples.", '']
        occ = w['occupancy_all_repositories']
        lines += ['', f"Shared bb2 inferred idle upper bound: {fmt(occ['idle_hours_inferred'])} h; busy lower bound: {fmt(occ['busy_hours_inferred'])} h; peak observed overlapping completed jobs: {occ['peak_jobs_inferred']}. Saturation / CPU-hours: unknown.", '',
            '| Waste signal (overlapping) | Evidence / cost | Tokens |', '|---|---|---|']
        for label, value in w['waste'].items():
            brief = {k:v for k,v in value.items() if k not in ('logs','per_pr','note','tokens','prs','unreachable_gate_logs','explicit_gate_logs')}
            if 'prs' in value:
                brief['PR_count'] = len(value['prs'])
            lines.append(f"| {label} | {json.dumps(brief)} | unknown |")
        lines += ['', 'Per-PR review submissions / threads / P1 / P2 (current snapshot):', '', '| PR | Reviews | Threads | P1 | P2 |', '|---|---:|---:|---:|---:|']
        for n,v in w['waste']['review']['per_pr'].items():
            lines.append(f"| #{n} | {v.get('rounds','?')} | {v.get('threads','?')} | {v.get('p1','?')} | {v.get('p2','?')} |")
        lines += ['', 'Claude tokens by recorded agent (lane mapping unavailable; not inferred PR ownership):', '', '| Agent | Input | Output | Cache create | Cache read | Gross / fleet merge |', '|---|---:|---:|---:|---:|---:|']
        for lane,u in sorted(w['claude_per_lane'].items(), key=lambda x: -sum(x[1].values())):
            lines.append(f"| {lane} | {u.get('input_tokens',0):,} | {u.get('output_tokens',0):,} | {u.get('cache_creation_input_tokens',0):,} | {u.get('cache_read_input_tokens',0):,} | {fmt(sum(u.values()) / w['merged_count'] if w['merged_count'] else None)} |")
        lines += ['', 'Merged PR open-to-merge hours:', '', ', '.join(f"#{p['number']} ({(dt(p['mergedAt'])-dt(p['createdAt'])).total_seconds()/3600:.2f} h)" for p in w['merged_prs']), '']
    lines += ['## Codex session-counter inventory', '', 'Session high-water counters deduplicate resumed footers, including quota refusals. Copies named *.run.*.log are excluded. These counts cannot be assigned to today or one account; previous sum-of-resumes inventories are superseded. Active/incomplete runs may have spent additional tokens. Observation rates reflect reporting time, not exact spend time.', '', f"Observation interval {fmt(r.get('codex_observation_hours'))}h; newly observed tokens per fleet merge {fmt(r.get('codex_observed_tokens_per_merge'))}. Unknown until comparable corrected snapshots exist.", '', '| Lane | Completed runs | Session tokens | Newly observed since cycle | Observed tokens/h |', '|---|---:|---:|---:|---:|']
    for x in sorted(r['codex_inventory_undated'], key=lambda x:-x['tokens']):
        delta = (r['codex_newly_observed_since_cycle'] or {}).get(x['lane'])
        rate=(r.get('codex_observed_tokens_per_lane_hour')or{}).get(x['lane'])
        lines.append(f"| {x['lane']} | {x['completed_runs']} | {x['tokens']:,} | {fmt(delta)} | {fmt(rate)} |")
    proof=read_json(ROOT/'compute-results.json',{})
    if proof:
        lines += ['', '## Measured bb2 cache policy (Codex)', '',
            f"Ephemeral-target fixture: before {proof['baseline_mean_seconds']:.3f} s mean; separate-job reuse after {proof['cross_job_mean_seconds']:.3f} s mean ({proof['cross_job_multiplier']:.2f}x); steady repeat {proof['steady_repeat_multiplier']:.2f}x. Production total savings pending; no verification skipped.", '',
            'Deployed: bb-job.py central/nested policy, native build-slot and WSL buildbox.mjs remote payload use CARGO_INCREMENTAL=0; shared sccache remains enabled. Per-job owned CPU/cache counters now retained. Details: compute-results.json, cache-before.log, cache-after-qualified.log and validation.log.', '',
            'Historical CPU-hours, compile/test CPU split, per-crate duplicates and eviction rebuild cost remain unknown. Fixture child CPU is not full job CPU. Four bounded proof/validation jobs are recorded in findings.md; the routine collector itself starts none.']
    utilisation=read_json(ROOT/'utilisation.json',{})
    if utilisation:
        lines += ['', '## bb2 CPU sample', '', f"At {utilisation['at']}: CPU busy {utilisation['cpu_busy_percent']:.2f}%, I/O wait {utilisation['cpu_iowait_percent']:.2f}% over {utilisation['sample_seconds']:.2f} s; {utilisation['logical_cpus']} logical CPUs. This point sample is not a 24-hour saturation estimate."]
    cow=read_json(ROOT/'cow-results.json',{})
    if cow:
        lines += ['', '## Cargo target sharing (Codex)', '', cow['summary'], '', 'Details and gaps: findings.md; evidence: cow-results.json and cow-*.log. Tests always execute.']
    lines += ['', '## Coverage and uncertainty', ''] + ['- '+x for x in r['limitations']]
    lines += ['- '+x for x in r['errors']]
    (ROOT / 'metrics.md').write_text('\n'.join(lines)+'\n', encoding='utf-8')


if __name__ == '__main__':
    main()
