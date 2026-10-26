"""Codex: one fake-drainer trace, bounded; never real Codex or live lane."""
from pathlib import Path
import json,os,subprocess,time
root=Path('D:/lane-scratch/efficiency/drainer-diagnosis-224');root.mkdir(exist_ok=True)
for name in ['bin','inbox','lane-state']:(root/name).mkdir(exist_ok=True)
(root/'fake.log').write_text('session id: fake\n')
(root/'lane-running.ps1').write_text('exit 1\n')
(root/'bin/codex').write_text('#!/bin/bash\nwhile [ "$#" -gt 0 ]; do if [ "$1" = -o ]; then out=$2; shift 2; else shift; fi; done\nprintf "LANE_STATUS: DONE\\n" > "$out"\necho fake-codex-final\n',newline='\n')
(root/'inbox/fake.md').write_text('## Steer fake\ncontinue\n')
(root/'steer.sh').write_bytes(Path('Q:/lanes/efficiency/machinery/steer.sh').read_bytes())
env={**os.environ,'FLEET_ROOT':root.as_posix(),'FLEET_ALERTS':(root/'alerts.log').as_posix(),'FLEET_WORKSPACE':root.as_posix(),'FLEET_CODEX':(root/'bin/codex').as_posix()}
start=time.monotonic()
with (root/'trace.log').open('wb')as output:
 proc=subprocess.Popen(['C:/Program Files/Git/usr/bin/bash.exe','--noprofile','--norc','-x',(root/'steer.sh').as_posix(),'fake'],env=env,stdin=subprocess.DEVNULL,stdout=output,stderr=output)
 try:rc=proc.wait(timeout=3)
 except subprocess.TimeoutExpired:
  subprocess.run(['taskkill','/PID',str(proc.pid),'/T','/F'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=2);rc=proc.wait(timeout=2)
value=dict(author='Codex',cycle=224,rc=rc,seconds=time.monotonic()-start,trace=(root/'trace.log').read_text(errors='replace')[-9000:],bb2_jobs=0)
Path('Q:/lanes/efficiency/machinery/drainer-diagnosis-cycle-224.json').write_text(json.dumps(value,indent=2)+'\n')
print(json.dumps(value))
