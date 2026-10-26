"""Codex: outcome targets and dependency schedule, using pinned local evidence only."""
import argparse,datetime as D,json,re,hashlib,math
from pathlib import Path
import anomaly as A

ROOT=Path(__file__).resolve().parent
MARKER='\n## Iteration outcome targets\n'
def read(name,default=None):
    try:return json.loads((ROOT/name).read_text(encoding='utf-8-sig'))
    except FileNotFoundError:return default
def quantile(values,p):
    if not values:return None
    values=sorted(values);i=(len(values)-1)*p;lo=math.floor(i);hi=math.ceil(i)
    return values[lo]+(values[hi]-values[lo])*(i-lo)
def stamp(v):return v.isoformat().replace('+00:00','Z')

def targets(m,a,index):
    end=A.date(m['as_of']);start=end-D.timedelta(hours=24)
    events=sorted((r for r in a['events']if A.date(r['at'])<=end),key=lambda r:A.date(r['at']))
    queues={};first={};merged={}
    for e in events:
        if e['source']!='pr-gate':continue
        if re.match(r'MERGE1 #\d+ queued for gate$',e['body']):
            for n in A.prs(e['body']):queues.setdefault(n,A.date(e['at']))
        if re.match(r'GATE_(PASS|FAIL|INFRA|CONFLICT|PREPARATION_INFRA) ',e['body']):
            for n in A.prs(e['body']):
                if n in queues and A.date(e['at'])>=queues[n]:first.setdefault(n,e)
        if e['body'].startswith('MERGE1 ')and 'merge_rc=0'in e['body']:
            for n in A.prs(e['body']):merged[n]=A.date(e['at'])
    latencies=[dict(pr=n,queued=stamp(queues[n]),merged=stamp(when),hours=(when-queues[n]).total_seconds()/3600)for n,when in merged.items()if n in queues and start<=when<=end]
    # Window boundary intervals are included; no idle-ready-work causality inferred.
    cuts=sorted({start,end,*[A.date(p['mergedAt'])for p in m['windows'][0]['merged_prs']]})
    drought=max(((b-a).total_seconds()/3600 for a,b in zip(cuts,cuts[1:])),default=24)
    observed=[e for e in first.values()if start<=A.date(e['at'])<=end]
    sent=[]
    for path in ROOT.glob('sent-steers-cycle-*.json'):
        sent.extend(read(path.name,[]))
    implemented=[];relay=[];follow=[]
    for s in sent:
        filename=Path(s['file']).name;after=A.date(s['at'])
        matches=[line for line in index.splitlines()if filename in line and re.search(r'\b(APPLIED|INSTALLED)\b',line)]
        for line in matches:
            dt=re.search(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z',line)
            when=A.date(dt[0])if dt else None
            if when and when>=after:
                item=dict(file=filename,hours=(when-after).total_seconds()/3600,evidence=line)
                (relay if re.search(r'relay|routed',line,re.I)else implemented).append(item)
        if after<end and not any(x['file']==filename for x in implemented):
            follow.append(dict(lane=s['lane'],file=filename,sent_at=s['at'],state='awaiting measured application; queued is not applied'))
    hours=[r['hours']for r in latencies];apply=[r['hours']for r in implemented]
    return dict(author='Codex',as_of=m['as_of'],queue_to_merge=dict(p50_hours=quantile(hours,.5),p95_hours=quantile(hours,.95),samples=len(hours),rows=latencies),job_hours_per_merge=m['windows'][0]['bb2_job_hours_per_merge'],longest_drought_hours=drought,first_observed_pass=dict(passed=sum(e['body'].startswith('GATE_PASS ')for e in observed),prs=len(observed),fraction=sum(e['body'].startswith('GATE_PASS ')for e in observed)/len(observed)if observed else None),steer_to_implemented=dict(p50_hours=quantile(apply,.5),p95_hours=quantile(apply,.95),samples=len(apply)),relays=relay,sent_followups=follow,limits=['First-observed PR outcome in retained alerts is not guaranteed lifetime first-pass or a head-specific pass rate.','Queue latency covers only explicit first-observed MERGE1 queued events with a successful merge; missing queues excluded.','Relay acknowledgement is separate from installation/application. Missing outcome is unknown, not zero latency.'])

def schedule(m,a):
    source=Path('Q:/lanes/design-audit/REDESIGN.md');text=source.read_text();end=A.date(m['as_of'])
    for n in range(1,8):assert f'| A{n} |'in text
    gh=read('github-cache.json',{}).get('prs',[])
    active=set((A.CX/'active-lanes.txt').read_text().split())
    nodes=[('SEC','b-authority','P1 security','none'),('STREAMS','streams','A1+A2+A3','none'),('ACTORS','b-runtime','A4','none'),('FS','fs-concurrency','A5','none'),('AUTH','b-authority','A6','none'),('WIRE','client-perf','A7','none'),('AGENT','sessions+b-runtime','wave2 agent subtree/inbox','wave1'),('APPROVALS','approvals','wave2 file approvals','wave1'),('PLUGINS','plugins','wave2 plugins/executor','wave1'),('CLIENTS','client-perf','wave2 GraphCoder/client deletions','wave1')]
    sketches=Path('Q:/lanes/redesign/api')
    merge_rate=m['windows'][0]['merged_count'];service=24/merge_rate if merge_rate else None
    rows=[]
    for i,(key,lane,scope,dependency)in enumerate(nodes):
        owners=lane.split('+');legacy=[dict(pr=p['number'],draft=p.get('isDraft'),title=p['title'])for p in gh if p['state']=='OPEN'and any(re.search(r'(?:^|/)'+re.escape(o)+r'(?:[-/]|$)',p['headRefName'])for o in owners)]
        # New redesign PR identity/readiness must come from owner evidence.
        conditional=stamp(end+D.timedelta(hours=service*(i+2)))if service else None
        rows.append(dict(id=key,lane=lane,scope=scope,depends_on=[]if dependency=='none'else ['STREAMS','ACTORS','FS','AUTH','WIRE'],state='owners active; readiness unverified'if all(o in active for o in owners)else 'owner activity unverified',api_sketch_present=[o for o in owners if(sketches/(o+'.md')).is_file()],pr=None,legacy_open_prs=legacy,forecast_merge=None,conditional_service_only_floor=conditional,top_blocker='Owner must identify source PR, proof-ready checkpoint and remaining dependency work; legacy PRs are not assumed redesign delivery.'))
    queued=sorted(a['queued'],key=lambda n:(n!=1509,n));queue_forecasts=[]
    for i,n in enumerate(queued):
        pr=next((p for p in gh if p['number']==n),{})
        queue_forecasts.append(dict(pr=n,forecast_merge=None,conditional_service_only_floor=stamp(end+D.timedelta(hours=service*(i+1)))if service else None,top_blocker='Draft/readiness and full gate proof must clear; existing owner controls queue order.'if pr.get('isDraft')else 'Full frozen-head gate/review proof and queue custody; inspect current held/baseline state.'))
    return dict(author='Codex',as_of=m['as_of'],source=str(source),source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),observed_merges_per_day=merge_rate,service_hours_per_merge=service,critical_path='exact-main input closure + gate-speed1509 -> Wave1 barrier (longest owner ETA unknown) -> Wave2 -> full release qualification/deletion lane',top_blocker='Shared pre-dispatch input-closure admission and verified queue drain; owner readiness/PR identities not yet supplied.',nodes=rows,queued_prs=queued,queue_forecasts=queue_forecasts,conditional_total_service_only_hours=service*11 if service else None,forecast_completion=None,limits=['No owner completion ETAs yet: unconditional merge dates and final delivery date are unknown.','Date floors assume proof-ready immediately, unchanged observed throughput and one gate prerequisite plus ten redesign PRs; coding, legacy predecessors, blocked reviews, security scope splits and final acceptance can add time.','Wave1 APIs are parallel as approved; Wave2 uses the full Wave1 barrier conservatively until owners prove narrower dependencies.','Legacy open PR ownership is branch-name metadata, not proof those PRs implement the new redesign scope.'])

def run(cycle):
    m=read('latest.json');a=read(f'anomalies-cycle-{cycle}.json');index=(ROOT/'steers/INDEX.md').read_text(errors='replace')
    value=targets(m,a,index);value['cycle']=cycle
    plan=schedule(m,a);previous=read('redesign-schedule.json',{});slips=[]
    for old in previous.get('nodes',[]):
        new=next((n for n in plan['nodes']if n['id']==old['id']),{})
        if old.get('forecast_merge')and new.get('forecast_merge')and(A.date(new['forecast_merge'])-A.date(old['forecast_merge'])).total_seconds()>86400:slips.append(dict(node=old['id'],before=old['forecast_merge'],after=new['forecast_merge']))
    plan['cycle']=cycle;plan['schedule_slips']=slips
    for name,data in [('iteration.json',value),('redesign-schedule.json',plan)]:
        (ROOT/name).write_text(json.dumps(data,indent=2)+'\n')
    q=value['queue_to_merge'];ap=value['steer_to_implemented'];fp=value['first_observed_pass']
    target_text=MARKER+f"\nAuthor: Codex. Endpoint{m['as_of']}. Queued-to-merged p50/p95 {q['p50_hours']}/{q['p95_hours']}h ({q['samples']} explicit pairs); jobh/merge {value['job_hours_per_merge']:.6f}; longest rolling24h drought {value['longest_drought_hours']:.3f}h. First-observed PR pass {fp['passed']}/{fp['prs']} ({fp['fraction']}); lifetime/head-first pass remains unqualified. Sent-steer to implemented p50/p95 {ap['p50_hours']}/{ap['p95_hours']}h ({ap['samples']} verified outcomes); relays excluded. Sources and coverage: iteration.json.\n\n"+'\n'.join('- '+x for x in value['limits'])+'\n'
    metrics=ROOT/'metrics.md';metrics.write_text(metrics.read_text().split(MARKER)[0]+target_text)
    lines=['# Redesign schedule - Codex','',f"Endpoint{m['as_of']}. Critical path: {plan['critical_path']}.",'',f"Top blocker: {plan['top_blocker']}",'',f"Observed service rate{merge_rate if (merge_rate:=plan['observed_merges_per_day'])else 'unknown'} merges/day; {plan['service_hours_per_merge']}h/merge. Eleven minimum service slots imply {plan['conditional_total_service_only_hours']}h after proof readiness. This is a conditional queue-only floor, not total delivery ETA. Owner readiness is missing; unconditional forecasts are unknown.",'','```mermaid','flowchart LR',' gate[Gate input closure and queue drain] --> wave1[Streams A1-A3; Actors A4; FS A5; Auth A6; Wire A7]',' wave1 --> agent[Agent subtree and inbox]',' wave1 --> approvals[File approvals]',' wave1 --> plugins[Plugins and executor]',' wave1 --> clients[GraphCoder and clients]',' security[P1 security now] --> auth[Auth A6]',' agent --> final[Release qualification and deletion]',' approvals --> final',' plugins --> final',' clients --> final','```','','|Scope|Lane/state|Redesign PR|Conditional service-only date floor|Top blocker / existing predecessors|','|---|---|---|---|---|']
    for row in plan['nodes']:lines.append(f"|{row['scope']}|{row['lane']}: {row['state']}|unknown|{row['conditional_service_only_floor']}|{row['top_blocker']} Legacy {[p['pr']for p in row['legacy_open_prs']]}|")
    lines+=['','|Queued PR|Conditional merge date if proof-ready now|Current top blocker|','|---|---|---|']
    for row in plan['queue_forecasts']:lines.append(f"|#{row['pr']}|{row['conditional_service_only_floor']}|{row['top_blocker']}|")
    lines+=['','Current queued PRs: '+str(plan['queued_prs'])+'. Owner checkpoint and PR identity requests sent directly; no queued/sent line counts as merged progress.','']+['- '+x for x in plan['limits']]
    (ROOT/'redesign-schedule.md').write_text('\n'.join(lines)+'\n')
    return dict(author='Codex',cycle=cycle,targets={k:value[k]for k in ['queue_to_merge','job_hours_per_merge','longest_drought_hours','first_observed_pass','steer_to_implemented']},schedule_slips=slips,bb2_jobs_started=0)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--cycle',type=int,required=True);print(json.dumps(run(p.parse_args().cycle)))
