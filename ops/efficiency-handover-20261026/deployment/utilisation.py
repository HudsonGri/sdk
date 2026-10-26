"""Codex: one bounded /proc CPU sample; no build or provider operation."""
import datetime
import json
from pathlib import Path
import subprocess


def sample():
    code = '''import json,os,time
from pathlib import Path
def cpu():return [int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
a=cpu();start=time.monotonic();time.sleep(1);b=cpu();elapsed=time.monotonic()-start
d=[y-x for x,y in zip(a,b)];total=sum(d)
m=dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
print(json.dumps({'sample_seconds':elapsed,'cpu_busy_percent':100*(total-d[3]-d[4])/total,
 'cpu_iowait_percent':100*d[4]/total,'logical_cpus':os.cpu_count(),'load':os.getloadavg(),
 'mem_available_kib':int(m['MemAvailable'].split()[0]),
 'pressure':{k:Path('/proc/pressure/'+k).read_text().strip() for k in ['cpu','memory','io']}}))
'''
    r = subprocess.run(['wsl','-d','Ubuntu','-e','ssh','-o','BatchMode=yes','bb2','python3 -'],
                       input=code, text=True, capture_output=True, check=True, timeout=20)
    out = json.loads(r.stdout)
    out['at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return out


if __name__ == '__main__':
    out = sample()
    Path(__file__).with_name('utilisation.json').write_text(json.dumps(out, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(out))
