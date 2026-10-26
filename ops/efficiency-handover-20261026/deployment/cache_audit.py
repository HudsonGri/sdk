"""Codex: bounded SDK receipt audit, no jobs, credential bodies or mutations."""
import collections
import datetime
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent


def main():
    previous = json.loads((ROOT / 'latest.json').read_text(encoding='utf-8'))
    since = previous['windows'][0]['start']
    until = previous['as_of']
    script = '''import json
for line in open('/home/var/log/buildbox/jobs.jsonl'):
 try: r=json.loads(line)
 except ValueError: continue
 if r.get('repo')!='sdk' and not str(r.get('tkey','')).startswith('sdk/'):continue
 if not (%r <= r.get('at','') <= %r):continue
 for s in (r.get('remote_receipt')or{}).get('target_store')or r.get('target_store')or[]:
  keep={k:s.get(k)for k in ['version','key','status','compile_seconds','compile_rc','clone_seconds','publish_seconds','local_wait_seconds','request_identity']}
  print(json.dumps({'at':r.get('at'),'lane':r.get('lane'),'log_id':r.get('log_id'),'request':keep}))
''' % (since, until)
    result = subprocess.run(['wsl','-d','Ubuntu','-e','python3','-'], input=script,
                            text=True, encoding='utf-8', capture_output=True, check=True)
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    generations = collections.defaultdict(list)
    for row in rows:
        generations[row['request']['version'] or 'unknown'].append(row)
    summaries = {}
    for name, group in generations.items():
        requests = [row['request'] for row in group]
        summaries[name] = {
            'requests': len(group),
            'states': dict(collections.Counter(r['status'] for r in requests)),
            'lanes': dict(collections.Counter(row['lane'] for row in group)),
            'unique_keys': len({r['key'] for r in requests}),
            'publication_seconds': sum(r.get('publish_seconds') or 0 for r in requests),
            'local_wait_samples': sum(r['local_wait_seconds'] is not None for r in requests),
            'local_wait_seconds': sum(r['local_wait_seconds'] or 0 for r in requests),
            'local_wait_max_seconds': max((r['local_wait_seconds'] or 0 for r in requests), default=0),
            'identity_samples': sum(bool(r.get('request_identity')) for r in requests),
            'unique_components': {key:len({r['request_identity'][key] for r in requests
                                           if (r.get('request_identity') or {}).get(key)})
                                  for key in ['source','argv','environment','tools','target','cwd']},
        }
        # Waiters overlap: summed process waiting is not job wall or CPU time.
        jobs = collections.defaultdict(list)
        keys = collections.defaultdict(list)
        for row in group:
            jobs[row['log_id']].append(row)
            keys[row['request']['key']].append(row)
        summaries[name]['wait_jobs_over_one_second'] = [
            {'log_id': log_id, 'lane': job[0]['lane'],
             'process_wait_seconds': sum(r['request']['local_wait_seconds'] or 0 for r in job),
             'maximum_request_wait_seconds': max(r['request']['local_wait_seconds'] or 0 for r in job),
             'requests_over_one_second': sum((r['request']['local_wait_seconds'] or 0) > 1 for r in job)}
            for log_id, job in jobs.items()
            if any((r['request']['local_wait_seconds'] or 0) > 1 for r in job)]
        summaries[name]['repeated_key_groups'] = sum(len(g) > 1 for g in keys.values())
        summaries[name]['cross_job_repeated_key_groups'] = sum(
            len({r['log_id'] for r in g}) > 1 for g in keys.values())
        # A hit without a visible producer is still a hit, but cannot establish
        # cross-job reuse or a before/after saving in this bounded window.
        summaries[name]['hit_evidence'] = [
            {'at': row['at'], 'lane': row['lane'], 'log_id': row['log_id'],
             'key': row['request']['key'],
             'clone_seconds': row['request']['clone_seconds'],
             'compile_seconds': row['request']['compile_seconds'],
             'compile_rc': row['request']['compile_rc'],
             'observed_producer_jobs': sorted({peer['log_id']
                 for peer in keys[row['request']['key']]
                 if peer['request']['status'] == 'miss'
                 and peer['request']['compile_rc'] == 0
                 and peer['request']['publish_seconds'] is not None
                 and peer['at'] <= row['at']}),
             'prior_publication_evidence': [
                 {'at': peer['at'], 'lane': peer['lane'], 'log_id': peer['log_id'],
                  'same_job': peer['log_id'] == row['log_id'],
                  'compile_seconds': peer['request']['compile_seconds'],
                  'clone_seconds': peer['request']['clone_seconds'],
                  'publish_seconds': peer['request']['publish_seconds']}
                 for peer in keys[row['request']['key']]
                 if peer['request']['status'] == 'miss'
                 and peer['request']['compile_rc'] == 0
                 and peer['request']['publish_seconds'] is not None
                 and peer['at'] <= row['at']]}
            for row in group if row['request']['status'] == 'hit']
        # Count requests that match every digest except one component. These
        # overlapping diagnostics identify sensitivity, never permission to
        # relax source, arguments or environment in the actual cache key.
        identities = [r['request']['request_identity'] for r in group
                      if r['request'].get('request_identity')]
        varying = {}
        for component in ['source','argv','environment','tools','target','cwd']:
            peers = collections.defaultdict(list)
            for identity in identities:
                peers[tuple((k,v) for k,v in sorted(identity.items()) if k != component)].append(identity)
            matches = [g for g in peers.values() if len({r.get(component) for r in g}) > 1]
            varying[component] = {'groups':len(matches), 'requests':sum(map(len,matches))}
        summaries[name]['only_component_differs'] = varying
    out = {'author':'Codex', 'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
           'since':since, 'until':until, 'generations':summaries, 'requests':rows}
    temporary = ROOT / 'cache_audit.json.tmp'
    temporary.write_text(json.dumps(out, indent=2)+'\n', encoding='utf-8')
    temporary.replace(ROOT / 'cache_audit.json')
    print(json.dumps(summaries))


if __name__ == '__main__':
    main()
