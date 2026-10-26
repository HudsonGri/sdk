"""Codex: retain bounded SDK terminal metadata without jobs or payload logs."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle', type=int, required=True)
    parser.add_argument('--long-commands', action='store_true',
                        help='Select failed command terminals over600s instead of infrastructure terminals.')
    args = parser.parse_args()
    if args.cycle < 1:
        parser.error('cycle must be positive')
    metrics = json.loads((ROOT / 'latest.json').read_text(encoding='utf-8'))
    start, end = metrics['windows'][2]['start'], metrics['as_of']
    script = '''import json
for line in open('/home/var/log/buildbox/jobs.jsonl'):
 try:r=json.loads(line)
 except ValueError:continue
 if r.get('repo')!='sdk' and not str(r.get('tkey','')).startswith('sdk/'):continue
 if not (%r<=r.get('at','')<=%r):continue
 if %r:
  if r.get('termination')!='command' or r.get('rc') in (None,0) or r.get('wall',0)<=600:continue
 elif r.get('termination') in (None,'command'):continue
 keep={k:r.get(k)for k in ['at','log_id','lane','wall','rc','termination','phase','infra_reason','reason','exec_start','end']}
 # Current client terminals use reason/detail; infra_reason is often absent.
 # Retain bounded diagnostic metadata, never command, environment or payload.
 keep['detail']=r['detail'][:2000] if isinstance(r.get('detail'),str) else None
 keep['remote_receipt']={k:(r.get('remote_receipt')or{}).get(k)for k in ['rc','exit_status','termination','phase','export_error','exec_start','end','home_removed','targets_removed','outer_targets_removed','namespace_custody']}
 print(json.dumps(keep))
''' % (start, end, args.long_commands)
    result = subprocess.run(['wsl', '-d', 'Ubuntu', '-e', 'python3', '-'],
                            input=script, text=True, encoding='utf-8',
                            capture_output=True, check=True)
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    for row in rows:
        row['wrapper_hours'] = row['wall'] / 3600 if row.get('wall') is not None else None
        finish = row['remote_receipt'].get('end') or row.get('end')
        begin = row.get('exec_start')
        row['execution_hours'] = None
        if begin and finish:
            dt = lambda value: datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            seconds = (dt(finish) - dt(begin)).total_seconds()
            if seconds >= 0:
                row['execution_hours'] = seconds / 3600
    evidence = {'author': 'Codex', 'since': start, 'until': end, 'rows': rows,
                'note': 'Metadata only; client status is not remote proof admission or avoidable CPU cost.'}
    prefix = 'long-commands' if args.long_commands else 'incidents'
    destination = ROOT / (prefix + '-cycle-' + str(args.cycle) + '.json')
    temporary = destination.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(evidence, indent=2) + '\n', encoding='utf-8')
    temporary.replace(destination)
    print(json.dumps(evidence))


if __name__ == '__main__':
    main()
