"""Codex: refresh the detailed findings appendix from the current pinned audits."""
import argparse
import collections
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent
MARKER='\n## Current owner decisions and retained proof\n'

def read(name):
    return json.loads((ROOT/name).read_text())

def refresh(cycle):
    m=read('latest.json');a=read(f'anomalies-cycle-{cycle}.json');t=read('throughput.json')
    l=read('lane-validation.json');v=read(f'prior-fix-verification-cycle-{cycle}.json')
    assert l['as_of']==m['as_of']==t['as_of'] and t['cycle']==cycle
    extra=read(f'anomalies-extra-cycle-{cycle}.json')
    tags=l['overlapping_tags'];test=v['retained_machinery_test']
    attempt_path=ROOT/f'machinery/installed-cycle-{cycle}/receipt.json'
    activation=('Latest efficiency-authorized attempt '+read(f'machinery/installed-cycle-{cycle}/receipt.json')['status']+'. Live files restored after Windows active-script lock; automatic partial-publication rollback is qualified. 'if attempt_path.exists()else '')
    top=sum(row['hours'] for _,row in list(l['per_lane'].items())[:3])
    text=MARKER+'\nAuthor: Codex. All job-hours below are summed wrapper wall, not exclusive box occupancy or CPU-hours. The first legacy ranked exposure label represents all lane validation, not measured compilation-only waste.\n\n'
    text+='Top three compute multipliers (overlapping conditional scenarios; no realized savings claimed):\n\n'
    for n,key,label in [(1,'all_features_log','Canonical affected-crate/features with reverse dependencies and complete final gate/platform/negative proof; log flags do not prove execution scope'),(2,'qualification_script_name','Qualified private immutable warm compilation for changed-input qualification'),(3,'reference_script_name','Canonical reference/main artifact reuse with actual compile-input and complete comparison custody')]:
        r=tags.get(key,dict(jobs=0,hours=0))
        text+=f"{n}. {label}: {r['jobs']} jobs/{r['hours']:.6f}h; 10% removable-share scenario {r['hours']*.1:.6f}jobh/day.\n"
    text+=f"\nDirect all-features command exposure {tags.get('all_features_command',{}).get('hours',0):.6f}h; opaque-or-focused scope {l['exclusive_kinds']['opaque_or_focused']['hours']:.6f}h. Top three lanes total {top:.6f}h (10% scenario {top*.1:.6f}h). Exact full-tree/command duplicate ceiling {l['repeat_extra_hours']:.6f}h; after two {l['hours_beyond_two']:.6f}h; cross-tier groups {l['cross_tier_tree_groups']}. No demonstrated duplicate-platform/gate-proof reuse saving; use the existing cache/proof owner and preserve required platforms and proof-input identities. Same-failure observations trigger source/fixture inspection, never automatic cancellation or skip. Nonzero terminals include intentional negative controls.\n\n"
    text+=f"Current anomaly counts {dict(collections.Counter(r['kind']for r in a['anomalies']))}; {a['alert_lines_appended']} primary plus {extra['alert_lines_appended']} supplemental lines appended. Supervisor latest heartbeat {t['heartbeat_last']}. Reinvokes {sum(t['supervisor_reinvokes'].values())}, reasons {t['supervisor_reasons']}; these are not all wasted turns. Ownership and integrated idle time remain incomplete until live controller qualification.\n\n"
    for d in a['disks']:
        needed=max(0,d['total']*.15-d['free'])
        text+=f"- {d['host']}:{d['path']} {d['free_pct']:.3f}% free; {needed/2**30:.3f}GiB additional free space to reach15%. Same APFS root/Data views are not additive. Reclaimable ownership and cause require the existing bounded SDK-owner census.\n"
    text+=f"\nKEPT original five immutable controls: {v['status']}. Staged machinery hashes match: {all(v['staged_hash_checks'].values())}; retained {test['tests']}-test {test['status']}/{test['seconds']:.3f}s qualification; rerun this cycle={v['machinery_test_rerun']}. Live matches staged {v['live_matches_staged']}. {activation}Candidate remains staged until safe atomic publication/restart plus actual heartbeat and dispatch proof. Retained fixture proof is not measured fleet ROI. Original shared-parent target scheme remains REVERTED; current sharing stays ADJUSTED/incomplete, as the measured cache section records.\n\n"
    caps=[f"{name}:{line['line']} {line['text']}"for name,source in t['cache_policy']['sources'].items()for line in source['policy_lines']if 'SCCACHE_CACHE_SIZE'in line['text']]
    text+=f"Current cap-source observations {caps}. INDEX's19:02Z APPLIED cache entry means spec relayed, not installed; require actual current-server qualification before claiming benefit. The coordinator's heartbeat fix is verified by live alerts; native machinery deployment needs separate confirmation. Current steer dispositions and new owner actions are recorded in INDEX, not inferred from source or relay.\n\n"
    text+=('Zero bb2 experiment jobs. Canonical queue publication/wakeup applied; fleet publication attempted and completely rolled back, with no net live fleet changes. 'if attempt_path.exists()else 'Routine collection only; zero bb2 experiment jobs. ')+'Historical Claude gaps and waste-specific token attribution remain unknown. Conditional savings overlap and must not be summed.\n'
    token=t.get('token_audit')
    if token:
        text+='\n### Token spend and coordinator cuts\n\n'
        text+=f"Corrected unique session high-water inventory {token['unique_session_tokens']:,}; old named-log resume sum {token['legacy_sum_run_counters']:,}. Repeated footer accounting correction is not saved tokens. Comparable previous corrected baseline {token['previous_observation']}; observed tokens/merge {token['observed_tokens_per_merge']}. Account quota denominator and exact spend timing remain unknown. Full per-lane rates, explicit branch progress and limits are in token-audit.md/json.\n\n"
        text+=f"Actual observations {token['previous_inventory_observed_at']} to {token['inventory_observed_at']}: {token['observed_counter_delta']} newly reported tokens over {token['observation_hours']:.6f}h, {token['merges_in_observation_interval']} merges. Tokens/merge is undefined with zero merges.\n\n"
        for row in sorted([r for r in token['rows']if r['observed_counter_delta']is not None],key=lambda r:-r['observed_counter_delta'])[:3]:
            text+=f"- Current observed priority {row['lane']}: {row['observed_counter_delta']:,} newly reported tokens, {row['observed_tokens_per_hour']:,.0f}/observation-hour; merges24h {row['matched_merges_24h']}, open PRs {row['matched_open_prs']}. Require head/proof attribution before calling this waste, including review work on other branches.\n"
        text+='\nHistorical session exposure below is secondary context, not current account burn.\n\n'
        for row in [r for r in token['rows']if r['active']and not r['matched_merges_24h']][:3]:
            rate=row['lifetime_tokens_per_hour_proxy']
            rate_text=f"{rate:,.0f}tokens/h lifetime wall proxy; 10% scenario{rate*.1:,.0f}tokens/h"if rate else 'rate unknown'
            text+=f"- {row['lane']}: {row['session_tokens']:,} session tokens; zero explicit branch-matched24h merges, open PRs{row['matched_open_prs']}; {rate_text}. Review next two turns for a concrete PR-head/proof delta or typed blocker; suppress blocked/job-owned periodic retries. Proxy is not current account burn, historical useful work is not discarded and all original verification remains.\n"
        text+='\nTwo-lane usage-limit detection is implemented in anomaly tooling and the qualified staged supervisor; a cumulative token footer no longer masks refusal. Coordinator-reported account recovery and current active roots are observations, not proof of deployed machinery or fleet ROI.\n'
    path=ROOT/'findings.md';base=path.read_text(encoding='utf-8').split(MARKER)[0]
    path.write_text(base+text,encoding='utf-8')
    return dict(author='Codex',cycle=cycle,as_of=m['as_of'],appendix='updated',bb2_jobs_started=0)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--cycle',type=int,required=True)
    print(json.dumps(refresh(p.parse_args().cycle)))
