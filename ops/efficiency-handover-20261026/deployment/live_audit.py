"""Codex: one passive bb2 helper-hash/lease snapshot, no jobs or writes there."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent
REMOTE = '''import ast,hashlib,json,subprocess
from pathlib import Path
files=['/home/var/bin/bb_target_store.py','/home/var/bin/bb_cargo_cache.py']
locks=json.loads(subprocess.check_output(['lslocks','--json','-o','PID,BLOCKER,PATH'],text=True)).get('locks',[])
keep=[r for r in locks if any(x in str(r.get('path','')) for x in ['bb-store-lock','cargo-target-leases','sdk-target-store-v1/locks'])]
sources={f:Path(f).read_bytes() for f in files}
generation=next((node.value.value for node in ast.parse(sources[files[0]]).body
 if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='VERSION' for t in node.targets)
 and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str)),None)
print(json.dumps({'hashes':{f:hashlib.sha256(body).hexdigest() for f,body in sources.items()},
 'target_store_generation':generation,'locks':keep}))
'''


def main():
    result = subprocess.run(['wsl','-d','Ubuntu','-e','ssh','-o','BatchMode=yes',
                             'bb2','python3 -'], input=REMOTE, text=True,
                            encoding='utf-8', capture_output=True, check=True,
                            timeout=30)
    value = json.loads(result.stdout)
    now = datetime.datetime.now(datetime.timezone.utc)
    value['at'] = now.isoformat()
    path = ROOT / ('live-' + now.strftime('%Y%m%dT%H%M%SZ') + '.json')
    gate = Path('Q:/lanes/pr-gate-checks.sh').read_text(encoding='utf-8')
    value['gate_target_lines'] = [line for line in gate.splitlines()
                                  if line.startswith(('target_root=', 'gate_target_root=',
                                                      'test_target=', 'clippy_target='))]
    value['gate_sha256'] = hashlib.sha256(gate.encode()).hexdigest()
    path.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'snapshot':path.name, 'at':value['at'], 'hashes':value['hashes'],
                     'old_global_users':sum('bb-store-lock' in str(r.get('path',''))
                                            for r in value['locks']),
                     'blocked':[r for r in value['locks'] if r.get('blocker')],
                     'gate_target_lines':value['gate_target_lines']}))


if __name__ == '__main__':
    main()
