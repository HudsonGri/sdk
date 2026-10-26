"""Codex: bounded SDK diagnostics; no jobs or command/environment metadata reads."""
import argparse
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent
REMOTE = r'''
import json,re
from collections import deque
from pathlib import Path
start,end=WINDOW
rows=[]
for line in open('/home/var/log/buildbox/jobs.jsonl'):
 try:r=json.loads(line)
 except ValueError:continue
 if r.get('repo')!='sdk' and not str(r.get('tkey','')).startswith('sdk/'):continue
 if not start<=r.get('at','')<=end:continue
 rows.append({k:r.get(k) for k in ('at','log_id','lane','wall','rc','termination')})
pattern=re.compile(r'^BUILD_SLOT_TREE=|^error(?:\[|:)|^(?:AssertionError|RuntimeError|ValueError|IndentationError|Error):|^test result:|^test .+FAILED$|^GATE_(?:DONE|WALL|MAIN_QUALIFICATION)|^AUTHORITY84|^Caused by:|^  (?:could not execute process|No such file)|^[A-Z_]+_EXPORT\b')
logs=[]
for row in rows:
 if row['rc']==0 and (row['wall'] or 0)<=600:continue
 ident=row['log_id'] or ''
 if not re.fullmatch(r'[A-Za-z0-9_-]+',ident):continue
 path=Path('/home/var/log/buildbox')/(ident+'.log')
 if not path.is_file():continue
 selected=[];total=0;count=0;tail=deque(maxlen=10)
 def retain(value,number):
  # Keep source identity and failures even when a large positive matrix fills
  # the bounded selection. Equal-priority rows retain their original order.
  rank=2
  if value.startswith('BUILD_SLOT_TREE='):rank=0
  else:
   try:record=json.loads(value)
   except ValueError:
    if 'FAILED' in value or 'Error' in value or value.startswith('error'):rank=1
   else:
    if isinstance(record,dict) and (
      any(record.get(k) not in (None,0) for k in ('exit','rc','returncode'))
      or record.get('accepted') is False or record.get('nonvacuous') is False
      or record.get('status') in ('FAIL','FAILED','TIMEOUT','ERROR')):rank=1
  item={'line':number,'text':value[:1600],'clipped':len(value)>1600,'_rank':rank}
  if len(selected)<100:selected.append(item)
  else:
   worst=max(range(len(selected)),key=lambda i:(selected[i]['_rank'],i))
   if rank<selected[worst]['_rank']:selected[worst]=item
 for count,line in enumerate(path.open(errors='replace'),1):
  line=line.rstrip('\n');value=None
  tail.append({'line':count,'text':line[:1600],'clipped':len(line)>1600})
  # Actual qualification runners emit case receipts as a JSON array.
  # Project fields only: never retain their executable/command/environment bodies.
  if line.startswith('[{'):
   try:cases=json.loads(line)
   except ValueError:cases=None
   if isinstance(cases,list):
    for record in cases:
     if not isinstance(record,dict) or 'accepted' not in record:continue
     keep={k:record[k] for k in ('target','selected','seconds','sha256','passed','failed','ignored','returncode','accepted') if k in record}
     value=json.dumps(keep);total+=1
     retain(value,count)
    continue
  if pattern.search(line):value=line
  elif any(field in line for field in ('"exit"','"rc"','"returncode"','"accepted"','"nonvacuous"','"status"')):
   at=line.find('{')
   try:record=json.loads(line[at:]) if at>=0 else None
   except ValueError:record=None
   if isinstance(record,dict):
    keep={k:record[k] for k in ('label','name','stage','phase','mode','variant','selection','status','exit','rc','returncode','accepted','seconds','seconds_with_compile','nonvacuous','archive_sha256') if k in record}
    if isinstance(record.get('error'),str):keep['error']=record['error'][:600]
    if keep:value=json.dumps(keep)
  if value is None:continue
  if value.startswith('error: Command failed:'):
   value='error: Command failed [command omitted; consult original SDK log]'
  total+=1
  retain(value,count)
 selected.sort(key=lambda item:item['line'])
 for item in selected:item.pop('_rank')
 logs.append({'log_id':ident,'lines':count,'matches':total,'omitted':max(0,total-len(selected)),'selected':selected,'tail':list(tail)})
print(json.dumps({'rows':rows,'logs':logs}))
'''


def summarize(evidence, destination):
    summaries = []
    by_id = {row['log_id']: row for row in evidence['rows']}
    for log in evidence['logs']:
        witnesses = list(dict.fromkeys(x['text'] for x in log['selected']
                         if not x['text'].startswith('BUILD_SLOT_TREE=')))
        def priority(value):
            try:
                record = json.loads(value)
            except ValueError:
                return 0 if ('FAILED' in value or 'Error' in value or value.startswith('error')) else 2
            return 0 if (record.get('exit') not in (None, 0)
                         or record.get('rc') not in (None, 0)
                         or record.get('returncode') not in (None, 0)
                         or record.get('accepted') is False
                         or record.get('status') in ('FAIL', 'FAILED', 'TIMEOUT', 'ERROR')) else 1
        witnesses.sort(key=priority)
        cases = []
        for value in witnesses:
            try:
                record = json.loads(value)
            except ValueError:
                continue
            if isinstance(record, dict) and 'accepted' in record:
                cases.append(record)
        summaries.append({'log_id': log['log_id'], 'rc': by_id[log['log_id']]['rc'],
                          'case_records': len(cases),
                          'case_rejected': sum(x['accepted'] is False for x in cases),
                          'matches': log['matches'], 'omitted': log['omitted'],
                          'witnesses': [value[:400] for value in witnesses[:4]],
                          'partial': not witnesses or len(witnesses) > 4 or log['omitted'] > 0
                                     or any(x['clipped'] for x in log['selected'])
                                     or any(len(value) > 400 for value in witnesses[:4]),
                          'tail_if_no_witness': log['tail'][-2:] if not witnesses else []})
    counts = {}
    for row in evidence['rows']:
        counts[row['lane']] = counts.get(row['lane'], 0) + 1
    return {'file': destination.name, 'rows': len(evidence['rows']), 'per_lane': counts,
            'logs': summaries, 'runtime_seconds': evidence['runtime_seconds']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle', type=int, required=True)
    args = parser.parse_args()
    if args.cycle < 1:
        parser.error('cycle must be positive')
    started = time.monotonic()
    m = json.loads((ROOT / 'latest.json').read_text(encoding='utf-8'))
    start, end = m['windows'][2]['start'], m['as_of']
    result = subprocess.run(['wsl', '-d', 'Ubuntu', '-e', 'python3', '-'],
                            input='WINDOW=' + repr((start, end)) + '\n' + REMOTE,
                            text=True, encoding='utf-8', capture_output=True, check=True)
    evidence = {'author': 'Codex', 'since': start, 'until': end,
                **json.loads(result.stdout)}
    evidence['runtime_seconds'] = time.monotonic() - started
    destination = ROOT / f'terminal-audit-cycle-{args.cycle}.json'
    temporary = destination.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(evidence, indent=2) + '\n', encoding='utf-8')
    temporary.replace(destination)
    # Retain bounded evidence locally; prioritize failures in concise output.
    print(json.dumps(summarize(evidence, destination)))


if __name__ == '__main__':
    main()
