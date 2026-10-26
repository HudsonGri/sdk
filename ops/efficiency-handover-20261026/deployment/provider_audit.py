"""Codex: bounded usage-refusal metadata from SDK lane logs, never auth stores."""
import argparse
import collections
import datetime as D
import json
from pathlib import Path
import re
import anomaly as A
from machinery.fleet_usage import scan,terminal_quota

QUOTA=re.compile(r'^ERROR:.*(?:hit your usage limit|usage limit (?:reached|exceeded)|insufficient (?:credits|quota))',re.I|re.M)
COMPLETED=re.compile(r'^tokens used\s*:?',re.M)

def refusal_count(tail):
    return len(list(QUOTA.finditer(tail)))if terminal_quota(tail)else 0

def main(cycle):
    data=A.read(A.ROOT/f'anomalies-cycle-{cycle}.json');now=A.date(data['at']);rows=[]
    # Reuse the same pinned observation as anomaly.py, rather than reclassifying restarted lanes.
    rows=data.get('usage_limit',{}).get('lanes',[])
    kinds={r['lane']for r in rows};end=A.date(data['at'])
    start=A.date(A.read(A.ROOT/'latest.json')['windows'][-1]['start'])
    reinvokes=collections.Counter()
    for event in data['events']:
        match=re.match(r'REINVOKE ([a-z0-9-]+) (\S+)',event['body'])
        if event['source']=='lane-supervisor' and match and match[1]in kinds and start<=A.date(event['at'])<=end:
            reinvokes[match[1]]+=1
    receipt=dict(author='Codex',cycle=cycle,at=data['at'],lanes=rows,reinvokes_since_previous_snapshot=dict(reinvokes),
                 limits='Bounded error counts are lower bounds; no billed tokens, exact refusal timestamps or automatic provider reset inferred.',bb2_jobs_started=0)
    A.write(A.ROOT/f'provider-audit-cycle-{cycle}.json',receipt)
    return receipt

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--cycle',type=int,required=True)
    print(json.dumps(main(parser.parse_args().cycle)))
