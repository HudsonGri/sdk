"""Codex: verify one recorded efficiency cycle without collecting or running jobs."""
import argparse
import json
import math
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
    cache = check(records, cycle)
    findings = (ROOT / 'findings.md').read_text(encoding='utf-8')
    assert all(gap in findings for gap in GAPS), 'historical usage gap lost'
    assert '|2: long failed jobs|' in findings, 'failed long exposure omitted'
    controls = 0
    if args.negative_controls:
        # In-memory corruptions only: actual verifier must reject each, then pass restoration.
        for obj, key, bad in ((records[1], 'until', 'stale'),
                              (cache, 'requests', cache['requests'] + 1)):
            old = obj[key]
            try:
                obj[key] = bad
                try:
                    check(records, cycle)
                except AssertionError:
                    controls += 1
                else:
                    raise RuntimeError('Invalid metadata was admitted')
            finally:
                obj[key] = old
        check(records, cycle)
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
