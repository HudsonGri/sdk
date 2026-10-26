"""Codex: end-to-end throughput from existing measured evidence, no new remote jobs."""
import argparse,collections,datetime as D,json,re
from pathlib import Path
import anomaly as A
ROOT=Path(__file__).resolve().parent

def build(metrics,a):
 end=A.date(metrics['as_of']);start=end-D.timedelta(hours=24)
 events=[e for e in a['events'] if start<=A.date(e['at'])<=end]
 gate=[e for e in events if e['source']=='pr-gate' and re.match(r'GATE_(PASS|FAIL|INFRA|CONFLICT|PREPARATION_INFRA) ',e['body'])]
 mix=dict(collections.Counter(e['body'].split()[0] for e in gate))
 reinvokes=collections.Counter();reasons=collections.Counter();repeated=[];previous={}
 for e in events:
  if e['source']!='lane-supervisor':continue
  match=re.match(r'REINVOKE ([a-z0-9-]+) (\S+)',e['body'])
  if not match:continue
  lane,reason=match.groups();reinvokes[lane]+=1;reasons[reason]+=1
  if lane in previous:
   seconds=(A.date(e['at'])-A.date(previous[lane]['at'])).total_seconds()
   if seconds<600:repeated.append(dict(lane=lane,seconds=seconds,reason=reason,progress='unknown; short interval is only a waste candidate'))
  previous[lane]=e
 waits=[]
 for pr in metrics['windows'][0]['merged_prs']:
  # GitHub created->merge is measured elsewhere. Alert routing is only a ready-time proxy.
  n=pr['number'] if isinstance(pr,dict) else pr
  merge=[e for e in events if e['source']=='pr-gate' and e['body'].startswith('MERGE1 ') and n in A.prs(e['body']) and 'merge_rc=0' in e['body']]
  if not merge:continue
  routes=[e for e in a['events'] if e['source']=='pr-gate' and e['body'].startswith('ROUTED ') and n in A.prs(e['body']) and A.date(e['at'])<=A.date(merge[-1]['at'])]
  if routes:waits.append(dict(pr=n,first_observed_route=routes[0]['at'],merged=merge[-1]['at'],route_to_merge_hours=(A.date(merge[-1]['at'])-A.date(routes[0]['at'])).total_seconds()/3600))
 heartbeat=[e for e in events if e['source']=='lane-supervisor' and e['body'].startswith('HEARTBEAT ')]
 counts=dict(collections.Counter(l['state'] for l in a['lanes']))
 return dict(author='Codex',as_of=metrics['as_of'],anomaly_at=a['at'],merged_per_day=metrics['windows'][0]['merged_count'],baseline_merges=9,baseline_gate_runs=34,gate_outcomes=mix,gate_runs=len(gate),pass_fraction=mix.get('GATE_PASS',0)/len(gate) if gate else None,observed_route_to_merge=waits,queue_wait_seconds=None,queue_wait_gap='No authoritative admission/start pair in current alert schema; no inferred ready time.',queued=a['queued'],lane_snapshot=counts,lanes=a['lanes'],supervisor_reinvokes=dict(reinvokes),supervisor_reasons=dict(reasons),quick_reinvoke_candidates=repeated,heartbeat_count=len(heartbeat),heartbeat_last=heartbeat[-1] if heartbeat else None,alert_lines_appended=a['alert_lines_appended'],disks=a['disks'],cache_policy=json.loads((ROOT/'bb2-policy-audit.json').read_text()),anomalies=a['anomalies'],coverage=a['coverage']+['Snapshot RUNNING may include completed roots; no live ownership controller yet.','No full time-integrated idle/blocked/gate-wait state before heartbeat deployment.','Waste-specific token and CPU allocation unknown.'],bb2=metrics['windows'][0]['bb2_tier_hours'],total_job_hours=metrics['windows'][0]['bb2_job_hours'],cpu_point=json.loads((ROOT/'utilisation.json').read_text()),machinery_test=json.loads((ROOT/'machinery/self-test-result.json').read_text()))

def render(v):
 mix=', '.join(k.removeprefix('GATE_')+'='+str(n)for k,n in v['gate_outcomes'].items())
 policy=v['cache_policy'];test=v['machinery_test']
 kinds=dict(collections.Counter(a['kind']for a in v['anomalies']))
 lane=v['lane_validation'];top=list(lane['per_lane'].items())[:3]
 top_text=', '.join(name+'='+format(row['hours'],'.3f')+'jobh'for name,row in top)
 q=lane['overlapping_tags'].get('qualification_script_name',{'hours':0,'jobs':0});af=lane['overlapping_tags'].get('all_features_command',{'hours':0,'jobs':0});lf=lane['overlapping_tags'].get('all_features_log',{'hours':0,'jobs':0});ref=lane['overlapping_tags'].get('reference_script_name',{'hours':0,'jobs':0})
 heartbeat_health='healthy'if v['heartbeat_last'] and (A.date(v['anomaly_at'])-A.date(v['heartbeat_last']['at'])).total_seconds()<=600 else 'stale or unobserved'
 drought=next((a['evidence']for a in v['anomalies']if a['kind']=='MERGE_DROUGHT'),'no active merge-drought alert')
 cap_lines=[f"{name}:{line['line']} {line['text']}" for name,record in policy['sources'].items() for line in record['policy_lines'] if 'SCCACHE_CACHE_SIZE' in line['text']]
 disk_text='; '.join(f"{d['host']}:{d['path']} {d['free_pct']:.2f}% free" for d in v['disks']) or 'unavailable'
 coverage_text='; '.join(v['coverage'])
 provider=v.get('provider_audit',{});quota=provider.get('lanes',[])
 quota_text=(f"Current bounded log audit confirms {len(quota)} lanes with {sum(r['quota_errors_in_bounded_tail']for r in quota)} provider usage-limit errors and {sum(provider.get('reinvokes_since_previous_snapshot',{}).values())} supervisor retries since the previous measurement. Counts are lower bounds; billed tokens and refusal timestamps are unknown. Staged drainer preserves the refused batch and writes a per-lane provider hold; supervisor suppresses further dispatch until explicit recovery. Capacity recovery requires the coordinator; efficiency owns qualified atomic machinery publication. No automatic account/model changes. " if quota else '')
 pressure=[d for d in v['disks'] if d['free_pct']<15]
 pressure_text=('Disk observations below the15% admission floor: '+', '.join(f"{d['host']}:{d['path']} {d['free_pct']:.2f}%"for d in pressure)+'. Existing owners must recover capacity through a bounded SDK-owner census. ')if pressure else 'No observed disk below the15% admission floor. '
 token=v.get('token_audit',{})
 token_text=(f"Session-counter accounting now deduplicates resumed quota footers and excludes run-log copies. Unique historical SDK session inventory{token['unique_session_tokens']:,}; observed interval tokens/merge{token['observed_tokens_per_merge']}, previous corrected baseline{token['previous_observation']}. These are not account/24h spend. Token-ranked owner reviews and qualified rate limits are in token-audit.md/json. "if token else '')
 actions_path=ROOT/f"outcomes-cycle-{v['cycle']}.json"
 actions=json.loads(actions_path.read_text())if actions_path.exists()else {}
 action_text=actions.get('summary','No separately recorded current-cycle action receipt.')
 attempt_path=ROOT/f"machinery/installed-cycle-{v['cycle']}/receipt.json"
 attempt=json.loads(attempt_path.read_text())if attempt_path.exists()else {}
 install_text=(f"Efficiency-authorized publication attempt {attempt['status']}; active Windows steer.sh denied atomic replacement. All published destinations were restored and before hashes verified. Automatic partial-publication rollback passes four controls. No net live change or process interruption; safe atomic publication/restart and heartbeat+dispatch smoke remain required."if attempt else 'Efficiency owns atomic publication/restart and actual heartbeat+dispatch qualification. Candidate remains staged; fixture proof alone does not establish live operation.')
 return f"""# Throughput — Codex

Cycle {v['cycle']}; endpoint {v['as_of']}; anomaly snapshot {v['anomaly_at']}. **{v['merged_per_day']} merges/24h versus baseline9**; {v['gate_runs']} gate outcomes ({mix}), pass fraction {v['pass_fraction']:.1%} versus baseline26.5%. Shifted windows are descriptive, not a controlled policy effect.

{v['alert_lines_appended']} primary ANOMALY lines appended, plus supplemental cache/observability alerts in anomalies-extra-cycle-{v['cycle']}.json; active kinds{kinds}. Supervisor heartbeat count{v['heartbeat_count']}, latest{v['heartbeat_last']}; current health **{heartbeat_health}**. Current disk observations{v['disks']}; coverage{v['coverage']}.

Immediate blocked-progress action: {quota_text}{pressure_text}{drought}. Merge changes are not attributed to our proposals.

Current-cycle actions: {action_text}

Top three cost levers:
1. Lane validation: {lane['lane_validation']['hours']:.6f}jobh ({100*lane['lane_validation']['hours']/v['total_job_hours']:.2f}% of SDK wall), {lane['lane_validation']['jobs']} attempts. Largest lanes: {top_text}. Qualification-name exposure{q['hours']:.6f}h/{q['jobs']} jobs; direct all-features{af['hours']:.6f}h/{af['jobs']} jobs, log-only all-features{lf['hours']:.6f}h/{lf['jobs']} jobs (overlapping observations, not executed-scope proof). Reference-name exposure{ref['hours']:.6f}h/{ref['jobs']} jobs. Only{lane['repeat_extra_hours']:.6f}h exact unchanged-tree/command repetitions, {lane['hours_beyond_two']:.6f}h after two, {lane['cross_tier_tree_groups']} cross-tier same-tree groups. Broad scopes and changed-input qualification are the investigation targets. Owners receive canonical affected-crate, retained gate-proof and private warm-target changes through INDEX; all required platforms/negative controls remain. Conditional10% reduction in qualification-name exposure saves{q['hours']*.1:.3f}jobh/day; realized saving unknown. Details and original log IDs in lane-validation.md/json.
2. Gate progress recovery and cache policy: {drought}. Prioritize canonical gate-speed-20261025T234704Z.md: move the existing exact-base admission before remote dispatch. Actual1495 payload5640s/all0 then missing-baseline parking gives1.566667jobh per equivalent preventable dispatch; not realized savings. Preserve frozen head, exact complete receipts, negative transport controls and publication wakeup; retain successful exact-input payload proof where the existing API supports continuation. Complete same-scope main Clippy/output custody before candidate attribution. Cache spec was relayed at19:02Z; deployment requires current source plus actual-server proof. Latest passive audit{policy['at']} has{len(policy['listening_sockets'])} listening endpoints/{policy['sccache_processes']} processes (not equivalent to servers). Observed cap lines: {cap_lines}. Gate-speed owns the single50G repo repair and actual-server refusal. Configured ceiling reductions are not reclaimed disk or measured CPU savings.
3. Fleet lifecycle: {sum(v['supervisor_reinvokes'].values())} reinvokes/24h, reasons{v['supervisor_reasons']}; {len(v['quick_reinvoke_candidates'])} sub10min repeat candidates, progress unknown. {quota_text}Expanded staged machinery suite{test['status']} ({test['tests']} tests/{test['errors']} errors/{test['seconds']:.3f}s), versus earlier12-test FAIL10.797s. Native drainer removes repeated MSYS forks, checks Busy ownership, preserves batches on launch refusal, keeps retry order and explicit completion. {install_text}

SDK bb2 {v['total_job_hours']:.6f}jobh; tiers{v['bb2']}. CPU point{v['cpu_point']['cpu_busy_percent']:.2f}% busy at{v['cpu_point']['at']}, not integrated saturation. Lane snapshot{v['lane_snapshot']}. Per-lane reinvokes and route-to-merge proxies in throughput.json. Queue wait and integrated idle/gate-wait time remain unknown without authoritative receipt pairs/state history.

Token efficiency: {token_text}Current terminal-quota lanes{len(quota)}. Live roots after coordinator-reported account recovery do not yet prove completed productive output. The shared detector alerts at two last-turn refusals, including a repeated token footer; staged controller remains pending deployment.

Coordinator dispositions19:02Z: heartbeat APPLIED and live verified; cache spec APPLIED as relay to gate-speed after its P0. The new charter authorizes efficiency to publish qualified machinery directly; application still needs actual atomic publication and live smoke evidence. Current native disk observations: {disk_text}. Probe coverage: {coverage_text}. Held PR recovery is assessed from current anomaly records, with no clearance inferred from relay. Earlier controls kept on original proof, fleet ROI unknown. Zero bb2 experiment jobs.
"""
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--cycle',type=int,required=True);args=p.parse_args()
 value=build(json.loads((ROOT/'latest.json').read_text()),json.loads((ROOT/f'anomalies-cycle-{args.cycle}.json').read_text()));value['cycle']=args.cycle
 audit=json.loads((ROOT/'lane-validation.json').read_text());assert audit['as_of']==value['as_of'],'stale lane audit'
 value['lane_validation']={k:v for k,v in audit.items()if k not in ('rows','script_groups')}
 provider_path=ROOT/f'provider-audit-cycle-{args.cycle}.json'
 if provider_path.exists():value['provider_audit']=json.loads(provider_path.read_text())
 token_path=ROOT/'token-audit.json'
 if token_path.exists():
  tokens=json.loads(token_path.read_text())
  if tokens['as_of']==value['as_of']:value['token_audit']=tokens
 A.write(ROOT/'throughput.json',value);(ROOT/'throughput.md').write_text(render(value),encoding='utf-8')
 print(json.dumps({k:value[k]for k in ['cycle','merged_per_day','gate_runs','gate_outcomes','pass_fraction','supervisor_reinvokes','supervisor_reasons','heartbeat_count']}))
