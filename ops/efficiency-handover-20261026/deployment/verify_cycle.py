"""Codex: verify one recorded efficiency cycle without collecting or running jobs."""
import argparse
import json
import math
import re

import terminal_audit
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GAPS = ('b8gmtwpq1.output', 'bvyyxtafl.output', 'btqv5drmz.output',
        'bxxfs3zlh.output', 'blwktudbw.output', 'b4mdrgun4.output')


def read(name):
    return json.loads((ROOT / name).read_text(encoding='utf-8'))


def check(records, cycle):
    m, audit, report, cow, inc, long, hist, usage = records
    endpoint = m['as_of']
    assert all(value == endpoint for value in
               (audit['until'], report['as_of'], cow[f'cycle_{cycle}']['as_of'],
                inc['until'], long['until'], hist['as_of'])), 'stale endpoint'
    assert report['utilisation'] == usage, 'CPU receipt differs'
    assert report['since'] == m['windows'][2], 'interval differs'
    entry = cow[f'cycle_{cycle}']
    cache = audit['generations'][entry['generation']]
    assert entry['cache'] == cache, 'cache receipt differs'
    assert sum(cache['states'].values()) == cache['requests'], 'cache counts differ'
    hashes = report['live']['hashes']
    expected = (all(cow.get('deployed', {}).get(k) == v for k, v in hashes.items())
                if hashes else 'unknown')
    assert entry['hashes_match'] == expected, 'deployment qualification differs'
    assert entry['bb2_experiment_job_seconds'] == 0, 'unexpected experiment'
    interval = m['windows'][2]
    receipts = interval['receipt_coverage']
    rows = inc['rows'] + long['rows']
    ids = [row['log_id'] for row in rows]
    assert len(ids) == len(set(ids)), 'duplicate terminal counted'
    assert all(row['termination'] == 'command' and row['rc'] not in (None, 0)
               and row['wall'] > 600 for row in long['rows']), 'invalid long selector'
    if receipts['legacy_rows'] == 0:
        kinds = {k for k in receipts['terminations'] if k not in ('command', None)}
        assert len(inc['rows']) == sum(receipts['terminations'][k] for k in kinds), 'infra count differs'
        assert math.isclose(sum(row['wrapper_hours'] for row in inc['rows']),
                            sum(receipts['termination_wrapper_hours'][k] for k in kinds),
                            abs_tol=1e-9), 'infra hours differ'
        failed = [row for row in rows if row['wall'] > 600 and row['rc'] not in (None, 0)]
        metric = interval['waste']['discarded_long_jobs']
        assert len(failed) == metric['failed_jobs_over_600s'], 'failed long count differs'
        assert math.isclose(sum(row['wrapper_hours'] for row in failed),
                            metric['failed_job_wall_hours'], abs_tol=1e-9), 'failed long hours differ'
    for window in m['windows']:
        if window['merged_count'] == 0:
            assert window['bb2_job_hours_per_merge'] is None, 'undefined ratio became numeric'
    return cache



def check_terminal(m, evidence):
    interval = m['windows'][2]
    assert (evidence['since'], evidence['until']) == (interval['start'], m['as_of']), 'terminal endpoint differs'
    rows = evidence['rows']
    ids = [row['log_id'] for row in rows]
    assert len(rows) == interval['bb2_jobs'], 'terminal count differs'
    assert len(set(ids)) == len(ids), 'duplicate terminal row'
    assert all(isinstance(row['wall'], (int, float)) and math.isfinite(row['wall'])
               and row['wall'] >= 0 for row in rows), 'invalid terminal wall'
    assert math.isclose(sum(row['wall'] for row in rows) / 3600,
                        interval['bb2_job_hours'], abs_tol=1e-9), 'terminal wall differs'
    for log in evidence['logs']:
        assert log['log_id'] in ids, 'unmatched diagnostic log'
        assert len(log['selected']) <= 100 and len(log['tail']) <= 10, 'diagnostic bounds differ'
        assert log['omitted'] == max(0, log['matches'] - len(log['selected'])), 'omission count differs'
        assert all(len(item['text']) <= 1600 for item in log['selected'] + log['tail']), 'diagnostic text exceeds bound'


def check_findings(m, findings):
    assert all(gap in findings for gap in GAPS), 'historical usage gap lost'
    w = m['windows'][0]
    r, v = w['receipt_coverage'], w['waste']
    costs = {
        'cold compilation / gate target topology': w['bb2_tier_hours']['lane_validation'],
        'long failed jobs': v['discarded_long_jobs']['failed_job_wall_hours'],
        'preparation/admission': r['admission_overhead_hours'],
        'cancelled/interrupted': sum(r['execution_termination_hours'].get(k, 0) for k in ('cancelled', 'interrupted')),
        'infrastructure terminations / gate churn': r['termination_wrapper_hours'].get('infra', 0),
        'failed batches/subsets': v['batch_failure']['bb2_job_hours'],
        'duplicate commands': v['duplicate_tree_command_candidates']['extra_wall_hours'],
    }
    rows = [line.split('|')[1] for line in findings.splitlines() if re.match(r'^\|[1-7]:', line)]
    assert [row.split(':', 1)[0] for row in rows] == [str(n) for n in range(1, 8)], 'rank numbers differ'
    assert [row.split(': ', 1)[1] for row in rows] == sorted(costs, key=costs.get, reverse=True), 'exposure ranking differs'
    return [{'category': name, 'hours': costs[name]} for name in sorted(costs, key=costs.get, reverse=True)]


def validate(records, cycle, terminal, findings):
    cache = check(records, cycle)
    check_terminal(records[0], terminal)
    check_findings(records[0], findings)
    return cache


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle', type=int, required=True)
    parser.add_argument('--negative-controls', action='store_true')
    args = parser.parse_args()
    cycle = args.cycle
    records = [read(name) for name in ('latest.json', 'cache_audit.json',
               f'cycle-{cycle}.json', 'cow-results.json', f'incidents-cycle-{cycle}.json',
               f'long-commands-cycle-{cycle}.json')]
    records += [json.loads((ROOT / 'metrics.jsonl').read_text(encoding='utf-8').splitlines()[-1]),
                read('utilisation.json')]
    terminal = read(f'terminal-audit-cycle-{cycle}.json')
    findings = (ROOT / 'findings.md').read_text(encoding='utf-8')
    cache = validate(records, cycle, terminal, findings)
    controls = 0
    if args.negative_controls:
        # In-memory corruptions only: actual verifier must reject each, then pass restoration.
        for obj, key, bad in ((records[1], 'until', 'stale'),
                              (cache, 'requests', cache['requests'] + 1),
                              (terminal, 'until', 'stale'),
                              (terminal, 'rows', terminal['rows'] + [{'log_id': 'negative-control', 'wall': 0}])):
            old = obj[key]
            try:
                obj[key] = bad
                try:
                    validate(records, cycle, terminal, findings)
                except AssertionError:
                    controls += 1
                else:
                    raise RuntimeError('Invalid metadata was admitted')
            finally:
                obj[key] = old
        # Reverse two categories while keeping legacy rank2 intact.
        lines = findings.splitlines()
        positions = [i for i, line in enumerate(lines) if re.match(r'^\|[1-7]:', line)]
        a, b = positions[0], positions[2]
        lines[a] = '|1:' + findings.splitlines()[b].split(':', 1)[1]
        lines[b] = '|3:' + findings.splitlines()[a].split(':', 1)[1]
        try:
            validate(records, cycle, terminal, '\n'.join(lines))
        except AssertionError:
            controls += 1
        else:
            raise RuntimeError('Invalid ranking was admitted')
        validate(records, cycle, terminal, findings)
    summary = terminal_audit.summarize(terminal, ROOT / f'terminal-audit-cycle-{cycle}.json')
    raw_bytes = len(json.dumps(terminal).encode())
    summary_bytes = len(json.dumps(summary).encode())
    proof = {'author': 'Codex', 'cycle': cycle, 'verification': 'PASS',
             'rows': len(terminal['rows']), 'retained_bytes': raw_bytes,
             'summary_bytes': summary_bytes,
             'output_reduction_percent': 100 * (1 - summary_bytes / raw_bytes),
             'runtime_seconds': terminal['runtime_seconds'], 'rank_check': 'PASS',
             'ranked_exposure_hours': check_findings(records[0], findings),
             'qualification': 'bounded output only; no billed-token saving claim'}
    destination = ROOT / f'terminal-audit-check-cycle-{cycle}.json'
    temporary = destination.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(proof, indent=2) + '\n', encoding='utf-8')
    temporary.replace(destination)
    m, _, report, cow, *_ = records
    w = m['windows'][0]
    previous = cow.get(f'cycle_{cycle-1}', {})
    print(json.dumps({'author':'Codex', 'cycle':cycle, 'verification':'PASS',
          'negative_controls_rejected':controls, 'as_of':m['as_of'],
          'jobh_per_merge':w['bb2_job_hours_per_merge'],
          'cpu_percent':report['utilisation']['cpu_busy_percent'],
          'admission_p50_p95_seconds':[w['receipt_coverage']['admission_overhead_median_seconds'],
                                      w['receipt_coverage']['admission_overhead_p95_seconds']],
          'historical_hits_unchanged':cache.get('hit_evidence') == previous.get('cache', {}).get('hit_evidence'),
          'qualified_deployment_hashes_match':cow[f'cycle_{cycle}']['hashes_match'],
          'bb2_jobs_launched':0}))


if __name__ == '__main__':
    main()
