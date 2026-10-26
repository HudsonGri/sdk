"""Codex: render routine cycle findings from existing bounded measurements.

No source collection, builds, remote writes, credentials, or policy application.
The caller reviews INDEX and supplies the disposition and qualitative note.
"""
import argparse
import datetime
import json
from pathlib import Path
import shutil

import measure

ROOT = Path(__file__).resolve().parent
GENERATION = 'sdk-target-reflink-v2-hierarchy'


def read(name):
    return json.loads((ROOT / name).read_text(encoding='utf-8'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle', type=int, required=True)
    parser.add_argument('--live', required=True)
    parser.add_argument('--steer-status', required=True,
                        choices=['PROPOSED', 'APPLIED', 'DECLINED', 'OBSERVED_PARTIAL'])
    parser.add_argument('--note', required=True)
    args = parser.parse_args()
    m, audit, usage = (read(n) for n in
                      ['latest.json', 'cache_audit.json', 'utilisation.json'])
    live = ({'at':m['as_of'], 'status':'unavailable', 'locks':None, 'hashes':{}}
            if args.live == 'unavailable' else read(args.live))
    if audit.get('until') != m['as_of']:
        raise SystemExit('Audit is stale: run cache_audit.py after measure.py completes.')
    # Preserve the previous report by its modification timestamp.
    findings = ROOT / 'findings.md'
    if findings.exists():
        stamp = datetime.datetime.fromtimestamp(findings.stat().st_mtime,
                    datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        archive = ROOT / ('findings-archive-' + stamp + '.md')
        if not archive.exists():
            shutil.copyfile(findings, archive)
    w, since = m['windows'][0], m['windows'][2]
    receipt, compute, waste = w['receipt_coverage'], w['compute'], w['waste']
    slot = receipt['admission_phase_coverage']
    generation = (live.get('target_store_generation')
                  or read('cow-results.json').get('last_observed_generation')
                  or GENERATION)
    generation_label = generation.removeprefix('sdk-target-reflink-')
    v = audit['generations'].get(generation, {})
    states = v.get('states', {})
    locks = live.get('locks')
    old_users = (sum('bb-store-lock' in str(row.get('path', '')) for row in locks)
                 if locks is not None else 'unknown')
    blocked = (sum(bool(row.get('blocker')) for row in locks)
               if locks is not None else 'unknown')
    # Keep evidence comparison explicit in JSON; installation receipts may have
    # a different schema, so do not infer unchanged hashes from their presence.
    previous = read('cow-results.json')
    if live.get('target_store_generation'):
        previous['last_observed_generation'] = generation
    hashes_match = (all(previous.get('deployed', {}).get(path) == value
                        for path, value in live['hashes'].items())
                    if live.get('hashes') else 'unknown')
    sample_age = abs((measure.dt(m['as_of']) - measure.dt(usage['at'])).total_seconds())
    sample_status = ('Current CPU unavailable; last successful' if sample_age > 120
                     else 'CPU')
    lane = w['bb2_tier_hours']['lane_validation']
    cancelled = sum(receipt['execution_termination_hours'].get(k, 0)
                    for k in ['cancelled', 'interrupted'])
    batch = waste['batch_failure']['bb2_job_hours']
    duplicate = waste['duplicate_tree_command_candidates']
    infra = waste['infra_conflict_rebase']
    infra_execution = receipt['execution_termination_hours'].get('infra', 0)
    infra_wrapper = receipt['termination_wrapper_hours'].get('infra', 0)
    report = f'''# Efficiency findings - cycle {args.cycle}

Author: Codex. Endpoint **{m['as_of']}**. Steer disposition after reviewing INDEX and source: **{args.steer_status}**. OBSERVED_PARTIAL denotes observed source or routing progress without complete application qualification; relays are separate from implementation.

{args.note}

Rolling24h: **{w['merged_count']} merges /{w['bb2_jobs']} SDK jobs /{w['bb2_job_hours']:.6f} jobh /{measure.fmt(w['bb2_job_hours_per_merge'])} jobh per merge**. Tiers: lane{lane:.6f}h, PR gate{w['bb2_tier_hours']['gate']:.6f}h, main gate{w['bb2_tier_hours']['main_gate']:.6f}h. Since previous endpoint:{since['bb2_jobs']} jobs/{since['bb2_job_hours']:.6f}h/{since['merged_count']} merges, Claude gross{since['claude_tokens']:,}. All observed SDK merges; roadmap certification and Codex interval token attribution remain unknown.

{sample_status} point **{usage['cpu_busy_percent']:.2f}% busy** at{usage['at']}; I/O wait{usage['cpu_iowait_percent']:.2f}%. Sample age versus endpoint{sample_age:.1f}s. Point sample, not daily saturation or a controlled policy effect. Admission wrapper p50/p95 **{receipt['admission_overhead_median_seconds']:.3f}/{receipt['admission_overhead_p95_seconds']:.3f}s**, includes preparation/probes. SDK slot p50/p95 **{slot['slot_wait_p50_seconds']:.6f}/{slot['slot_wait_p95_seconds']:.6f}s** over{slot['slot_wait_samples']} samples. Partial CPU{compute['attributed_core_hours']:.6f} coreh/{compute['cpu_samples']} jobs; whole-fleet coreh/merge and compile/test split unknown.

## Ranked waste

Rank uses aggregate exposure (observed cost times occurrences). Categories overlap; waste-specific tokens and avoidable shares remain unknown. Saving estimates are conditional scenarios, not realized reductions.

|Rank|Exposure|Structural fix and saving scenario|
|---|---|---|
|1: cold compilation / gate target topology|Lane{lane:.6f}h; {generation_label}: {states.get('hit', 0)} hits/{v.get('requests', 0)} requests; compilation-only share unknown.|Existing gate-speed sibling-target/private-preparation steer.5-10% reusable lane wall at75% reduction:{lane*.0375:.3f}-{lane*.075:.3f} jobh/day scenario; actual gate wall benefit unqualified.|
|2: long failed jobs|{waste['discarded_long_jobs']['failed_jobs_over_600s']} attempts/{waste['discarded_long_jobs']['failed_job_wall_hours']:.6f} wrapperh.|Reconcile original source/control witnesses and retained artifact/proof custody before equivalent broad replay; repair canonical owner causes. A1% avoidable-share scenario:{waste['discarded_long_jobs']['failed_job_wall_hours']*.01:.3f} wrapperh/day; expected negative controls and useful partial proof are retained, actual saving unknown.|
|3: preparation/admission|{receipt['admission_overhead_hours']:.6f} aggregate h.|Existing phase-attribution/classification steer.10% reduction:{receipt['admission_overhead_hours']*.1:.3f} wrapperh/day scenario, CPU unknown.|
|4: cancelled/interrupted|{cancelled:.6f} executionh.|Existing cause/successor/drain steer. Half avoidable:{cancelled/2:.3f} executionh ceiling.|
|5: infrastructure terminations / gate churn|{receipt['terminations'].get('infra', 0)} SDK infra terminals/{infra_wrapper:.6f} wrapperh, including {infra_execution:.6f} measured executionh. Gate INFRA/conflict/rebase separately: {infra['events']} events/{infra['bb2_job_hours']:.6f} matched jobh; overlaps.|Existing incident-preservation/root-cause and guards/recovery steers. Half infra execution exposure:{infra_execution/2:.3f}h conditional ceiling; incident causality and avoidable share unknown.|
|6: failed batches/subsets|{waste['batch_failure']['events']} failures/{batch:.6f}jobh; subsets{waste['batch_descendant_rerun_candidates']['bb2_job_hours']:.6f}h, overlapping.|Owner isolation/bisect plus safe compile reuse. Half batch exposure:{batch/2:.3f}h ceiling; all verification retained.|
|7: duplicate commands|{duplicate['extra_jobs']} extra jobs/{duplicate['extra_wall_hours']:.6f}h.|Strict successful hermetic result reuse only after item1 qualifies. Candidates include failures; not replay permission.|

All remaining waste signals, per-lane tokens, review threads, merge lifetimes, idle upper bound and coverage limits remain in metrics.md. Unknown discarded/avoidable share of failed jobs, causal spot cost, superseded/abandoned cost, idle-with-ready-work and avoidable P1 costs are not zero.

Historical Claude usage gaps remain unallocated:49b8gmtwpq1.output,75bvyyxtafl.output,94btqv5drmz.output,122bxxfs3zlh.output,136blwktudbw.output,175b4mdrgun4.output. Codex interval and waste-specific token allocation remain unknown.

## Verify earlier fixes

Private hierarchical custody **KEPT**, subject to receipt proof. Current hashes match the last qualified efficiency deployment: **{hashes_match}**. Old-global users:{old_users}; currently blocked lease rows:{blocked}. A snapshot cannot establish prior live job completion or all historical waiting. Original shared-parent scheme remains **REVERTED**.

Target sharing **ADJUSTED / incomplete**: {generation_label}: {v.get('requests', 0)} requests/{v.get('unique_keys', 0)} keys; states{states}; cross-job repeated-key groups{v.get('cross_job_repeated_key_groups', 0)}. Publication{v.get('publication_seconds', 0):.3f}s is overhead. Completed process waiting{v.get('local_wait_seconds', 0):.3f}s; overlapping waits are not jobh/coreh. Snapshot cache_audit.json retains every generation and per-job wait summaries. Fleet ROI remains unqualified; historical controlled4.00x fixture is separate evidence.

Digest diagnostics/audit **KEPT**:{v.get('identity_samples', 0)} production samples, aligned endpoint. Export correction, no-incremental sccache and home/preparation guards **KEPT** on previous controls; observational hit rates do not replace them. No input-key relaxation; current action and publication receipts distinguish attempted changes, rollback and actual application. Any observed deployment changes require the qualification described above. Collector **KEPT**:{m['runtime_seconds']:.3f}s, errors{m['errors']}.

## Coordinator decisions

Exact existing steer: steers/gate-speed-20261023T233700Z.md, status{args.steer_status}. Preserve single bb2/current CPUs and full verification. No slot/fleet/model changes justified by a point CPU sample. Item1 remains incomplete: native custody, opaque Cargo/xwin/nextest/doc coverage, source mtime sensitivity and first-empty-target publication. Items2-6 stay ordered. No duplicate steer without new evidence needing owner action.

Cycle cost: **zero bb2 experiment jobs**. Local collection and passive samples are separate from any explicitly recorded canonical queue actions or qualified machinery publication attempts above. No proof waiver or timeout increase. Earlier detailed controls remain in archived findings and evidence files.
'''
    # Aggregate exposure already includes frequency. Re-rank each cycle rather
    # than preserving the original category order as their costs change.
    costs = [lane, waste['discarded_long_jobs']['failed_job_wall_hours'],
             receipt['admission_overhead_hours'], cancelled, infra_wrapper,
             batch, duplicate['extra_wall_hours']]
    lines = report.splitlines(keepends=True)
    positions = [i for i, line in enumerate(lines)
                 if line.startswith(tuple('|' + str(n) + ':' for n in range(1, 8)))]
    assert len(positions) == len(costs)
    ranked = sorted(zip(costs, (lines[i] for i in positions)),
                    key=lambda item: item[0], reverse=True)
    for rank, (position, (_, row)) in enumerate(zip(positions, ranked), 1):
        lines[position] = '|' + str(rank) + ':' + row.split(':', 1)[1]
    report = ''.join(lines)
    throughput_path = ROOT / 'throughput.json'
    if throughput_path.exists():
        throughput = read('throughput.json')
        if throughput.get('cycle') == args.cycle:
            quota=throughput.get('provider_audit',{}).get('lanes',[])
            blocked_priority=(f"Immediate capacity recovery: {len(quota)} provider-quota lanes; staged batch-preserving hold prevents repeated refused dispatch, coordinator restores capacity and applies atomically. " if quota else '')
            focus = ('## Throughput priority and coordinator decisions\n\n'
                     +blocked_priority+
                     f"Priority1: lane validation{throughput['lane_validation']['lane_validation']['hours']:.3f}jobh/"
                     f"{100*throughput['lane_validation']['lane_validation']['hours']/throughput['total_job_hours']:.2f}% of SDK wall; prioritize affected-crate qualification and qualified private warm targets. "
                     'Priority2: move existing exact-base admission before remote payload dispatch (canonical gate-speed-20261025T234704Z.md); #1495 spent5640s before missing-baseline parking. Retain frozen-head, complete receipt and publication-wakeup controls; this is a1.5667jobh ceiling per equivalent preventable dispatch, not realized savings. Restore main output custody and gate-speed single50G policy. '
                     'Priority3: staged native fleet machinery, pending atomic installation and live dispatch proof. '
                     f"Rolling {throughput['merged_per_day']} merges/{throughput['gate_runs']} gate outcomes. "
                     'Keep original negative controls and proof fences. Detailed dispositions, current alerts, '
                     'staged machinery qualification and measurement gaps are in throughput.md/json. '
                     'The overlapping cost-exposure ranking below is separate from blocked-progress priority.\n\n')
            report = report.replace('## Ranked waste\n', focus+'## Ranked waste\n', 1)
    temp = ROOT / 'findings.md.tmp'
    temp.write_text(report, encoding='utf-8')
    temp.replace(findings)
    summary = (f'Codex cycle{args.cycle}: {generation_label} {v.get("requests", 0)} requests, '
               f'{states}, cross-job repeated-key groups{v.get("cross_job_repeated_key_groups", 0)}. '
               f'Gate routing steer {args.steer_status}; old-global users{old_users}, '
               f'currently blocked leases{blocked}. Fleet ROI unqualified; zero bb2 experiment jobs.')
    previous['summary'] = summary
    previous['cycle_' + str(args.cycle)] = {'author':'Codex', 'as_of':m['as_of'],
         'generation':generation, 'cache':v, 'hashes_match':hashes_match, 'steer_status':args.steer_status,
         'note':args.note, 'bb2_experiment_job_seconds':0}
    measure.write_json(ROOT / 'cow-results.json', previous)
    measure.write_json(ROOT / ('cycle-' + str(args.cycle) + '.json'),
         {'author':'Codex', 'as_of':m['as_of'], 'since':since,
          'cache_summary':audit['generations'], 'live':live,
          'utilisation':usage, 'note':args.note})
    measure.write_markdown(m)
    if sample_age > 120 or locks is None:
        with (ROOT / 'metrics.md').open('a', encoding='utf-8') as stream:
            stream.write('\nCurrent-cycle passive bb2 collection unavailable or stale; '
                         'the CPU sample above is historical. Current CPU, helper '
                         'hashes and lease state are unknown; see findings.md.\n')
    print(json.dumps({'as_of':m['as_of'], 'jobh_per_merge':w['bb2_job_hours_per_merge'],
                      'cache':states, 'cross_job_repeat_groups':v.get('cross_job_repeated_key_groups'),
                      'hashes_match':hashes_match, 'blocked_leases':blocked,
                      'steer_status':args.steer_status, 'bb2_jobs_started':0}))


if __name__ == '__main__':
    main()
