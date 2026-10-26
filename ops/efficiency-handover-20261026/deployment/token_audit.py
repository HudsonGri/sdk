"""Codex: session-counter inventory, observation rates and explicit branch progress."""
import argparse,datetime as D,json,re,uuid
from pathlib import Path
import measure

ROOT=Path(__file__).resolve().parent

def counters(row):
    result={}
    for lane in row.get('codex_inventory_undated',[]):
        if lane.get('counter_basis')!='session-high-water-v1':continue
        for session in lane['sessions']:
            sid=session['session_id'];result[sid]=max(result.get(sid,0),session['tokens'])
    return result

def build(m,history,prs,active):
    end=measure.dt(m['as_of']);now=counters(m)
    prior=[r for r in history if measure.dt(r.get('as_of')) and measure.dt(r['as_of'])<end
           and r.get('codex_inventory_undated') and all(x.get('counter_basis')=='session-high-water-v1'for x in r['codex_inventory_undated'])]
    # Same endpoint may have corrected collector revisions. Prefer the latest
    # actual inventory observation, then the last durable revision on a tie.
    previous=max(enumerate(prior),key=lambda pair:(pair[1]['as_of'],pair[1].get('codex_inventory_observed_at',''),pair[0]))[1]if prior else None
    old=counters(previous)if previous else {}
    observed_end=measure.dt(m.get('codex_inventory_observed_at'))or end
    observed_start=(measure.dt(previous.get('codex_inventory_observed_at'))or measure.dt(previous['as_of']))if previous else None
    hours=(observed_end-observed_start).total_seconds()/3600 if observed_start else None
    rows=[];names=[x['lane']for x in m['codex_inventory_undated']]
    for item in m['codex_inventory_undated']:
        lane=item['lane'];starts=[];reset=False;delta=0
        for session in item['sessions']:
            try:
                value=uuid.UUID(session['session_id'])
                if value.version==7:starts.append(D.datetime.fromtimestamp(int(value.hex[:12],16)/1000,D.timezone.utc))
            except(ValueError,OverflowError,OSError):pass
            if session['session_id']in old and session['tokens']<old[session['session_id']]:reset=True
            delta+=max(0,session['tokens']-old.get(session['session_id'],0))
        age=(end-min(starts)).total_seconds()/3600 if starts else None
        matched=[]
        for pr in prs:
            branch=pr['headRefName'];owners=[n for n in names if re.search(r'(?:^|/)'+re.escape(n)+r'(?:[-/]|$)',branch)]
            if owners and lane==max(owners,key=len):matched.append(pr)
        merges=[p['number']for p in matched if p['mergedAt']and end-D.timedelta(hours=24)<=measure.dt(p['mergedAt'])<=end]
        opened=[p['number']for p in matched if p['state']=='OPEN']
        observed=delta if previous and not reset else None
        rows.append(dict(lane=lane,active=lane in active,session_tokens=item['tokens'],
            session_age_hours_proxy=age,lifetime_tokens_per_hour_proxy=item['tokens']/age if age and age>0 else None,
            observed_counter_delta=observed,observed_tokens_per_hour=observed/hours if observed is not None and hours else None,
            matched_merges_24h=merges,matched_open_prs=opened,
            overcount_removed=item['repeated_counter_overcount']))
    rows.sort(key=lambda r:-r['session_tokens'])
    total_delta=measure.codex_counter_delta(m['codex_inventory_undated'],previous['codex_inventory_undated'])if previous else None
    merge_count=sum(bool(p['mergedAt'])and observed_start<=measure.dt(p['mergedAt'])<=observed_end for p in prs)if previous else None
    return dict(author='Codex',as_of=m['as_of'],inventory_observed_at=measure.iso(observed_end),counter_basis='session-high-water-v1',rows=rows,
        unique_session_tokens=sum(now.values()),sessions=len(now),legacy_sum_run_counters=sum(x['legacy_sum_run_counters']for x in m['codex_inventory_undated']),
        previous_observation=previous['as_of']if previous else None,observation_hours=hours,
        previous_inventory_observed_at=measure.iso(observed_start)if observed_start else None,
        observed_counter_delta=total_delta,merges_in_observation_interval=merge_count,
        observed_tokens_per_merge=total_delta/merge_count if total_delta is not None and merge_count else None,
        operator_reported_account_recovery='2026-10-25T23:10:00Z; account identity and quota denominator unavailable',
        limits=['Counters cover SDK session histories, not one account or24h spend. Account recovery is coordinator-reported; no credential or account data accessed.',
                'UUIDv7 creation time gives only a session-lifetime wall-hour proxy, not active hours, current burn or account depletion rate.',
                'Observation deltas become available after two comparable corrected snapshots; completion/reporting lag prevents exact spend timing. Missing prior session counters count as newly observed, not proven interval spend.',
                'Branch names are explicit ownership proxies; unmatched PRs, shared work, unmerged proof and roadmap contribution are not assigned. Zero24h matches does not mean a session made no progress.',
                'Legacy resume sums and duplicated *.run.*.log copies overstate inventory. Corrections are accounting changes, not saved model tokens.'],bb2_jobs_started=0)

def render(v):
    text=f"# Token efficiency - Codex\n\nEndpoint {v['as_of']}. Unique session high-water inventory {v['unique_session_tokens']:,} tokens across{v['sessions']} sessions; comparable named-log legacy resume sum {v['legacy_sum_run_counters']:,}. Difference is accounting correction, not savings.\n\n"
    text+=f"Comparable previous corrected metric endpoint: {v['previous_observation']}; actual counter observations {v['previous_inventory_observed_at']} to {v['inventory_observed_at']}; observed interval{v['observation_hours']}h; newly observed counters{v['observed_counter_delta']}; tokens/merge{v['observed_tokens_per_merge']}. Unknown is not zero. Counter reporting time is not exact spend time; running turns can contain unreported usage.\n\n"
    text+='|Lane|Session tokens|Lifetime tokens/h proxy|Observed tokens/h|Branch-matched merges24h|Open PRs|\n|---|---:|---:|---:|---|---|\n'
    for r in v['rows']:
        fmt=lambda x:'unknown'if x is None else f'{x:,.0f}'
        text+=f"|{r['lane']}{''if r['active']else ' (inactive)'}|{r['session_tokens']:,}|{fmt(r['lifetime_tokens_per_hour_proxy'])}|{fmt(r['observed_tokens_per_hour'])}|{r['matched_merges_24h']}|{r['matched_open_prs']}|\n"
    observed=sorted([r for r in v['rows']if r['observed_counter_delta'] is not None],key=lambda r:-r['observed_counter_delta'])[:5]
    text+='\nCurrent observed increments (completion/reporting lag applies; these are review priorities, not proven waste):\n\n'
    for r in observed:
        text+=f"- {r['lane']}: {r['observed_counter_delta']:,} newly reported tokens; {r['observed_tokens_per_hour']:,.0f} tokens per observation-hour; matched merges24h {r['matched_merges_24h']}, open PRs {r['matched_open_prs']}. Inspect PR-head and retained proof changes before attributing waste; review/support lanes may contribute to other branches.\n"
    targets=[r for r in v['rows']if r['active']and not r['matched_merges_24h']][:3]
    text+='\nHistorical session exposure with no branch-matched24h merge (secondary context; not current burn):\n\n'
    for r in targets:
        proxy=r['lifetime_tokens_per_hour_proxy']
        saving=f"{proxy*.1:,.0f}tokens/h"if proxy else 'unknown'
        text+=f"- {r['lane']}: {r['session_tokens']:,} session tokens. Coordinator should require the next turn to produce one source/proof delta or a typed external blocker; suppress periodic resumes while blocked/owned jobs remain live and resume on the actual dependency event. Reuse the canonical retained evidence rather than restating old logs. A10% removable-share scenario against the lifetime-rate proxy is{saving}, not a measured forecast; preserve full required verification.\n"
    text+='\nCoordinator decision: checkpoint the next two productive turns of those lanes against newly observed session increments and actual PR head/proof changes. If>500k newly observed tokens bring neither, diagnose scope/log replay or external waiting before another broad investigation; this is a review trigger, not a verification cap or automatic cancellation. No model/account/fleet-size change proposed without measured progress/cost.\n\n'
    text+='\n'.join('- '+x for x in v['limits'])+'\n'
    return text

def main(cycle):
    m=measure.read_json(ROOT/'latest.json',{});history=[]
    for line in (ROOT/'metrics.jsonl').read_text(encoding='utf-8').splitlines():
        try:history.append(json.loads(line))
        except ValueError:continue
    gh=measure.read_json(ROOT/'github-cache.json',{});active=(measure.SCRATCH/'cx/active-lanes.txt').read_text().split()
    value=build(m,history,gh['prs'],active);value['cycle']=cycle
    measure.write_json(ROOT/'token-audit.json',value);(ROOT/'token-audit.md').write_text(render(value),encoding='utf-8')
    return dict(author='Codex',cycle=cycle,unique_session_tokens=value['unique_session_tokens'],sessions=value['sessions'],previous_observation=value['previous_observation'],top=[{k:r[k]for k in ['lane','session_tokens','lifetime_tokens_per_hour_proxy','matched_merges_24h']}for r in value['rows'][:5]])

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--cycle',type=int,required=True)
    print(json.dumps(main(p.parse_args().cycle)))
