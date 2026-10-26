"""Codex: atomic, predecessor-fenced installs; no running file edited in place."""
import hashlib
from pathlib import Path
import subprocess

ROOT = Path('Q:/lanes/efficiency')


def remote(code):
    r = subprocess.run(['wsl','-d','Ubuntu','-e','ssh','-o','BatchMode=yes','bb2','python3 -'],
                       input=code, capture_output=True, text=True)
    print(r.stdout, end=''); print(r.stderr, end='')
    r.check_returncode()


def main():
    names = ['bb_target_store.py','bb_cargo_cache.py','bb-job.cow-candidate.py','buildbox.cow-candidate.mjs']
    for name in names:
        subprocess.run(['wsl','-d','Ubuntu','-e','scp','-q','-o','BatchMode=yes',
                        '/mnt/q/lanes/efficiency/'+name,'bb2:/mnt/bb/efficiency/'+name],check=True)
    remote('''import ast,subprocess
from pathlib import Path
for name in ['bb_target_store.py','bb_cargo_cache.py','bb-job.cow-candidate.py']:
 ast.parse((Path('/mnt/bb/efficiency')/name).read_text())
subprocess.run(['/home/var/tools/node24/node-v24.3.0-linux-x64/bin/node','--check','/mnt/bb/efficiency/buildbox.cow-candidate.mjs'],check=True)
print('Candidate syntax passed on bb2')
''')
    old_job = hashlib.sha256((ROOT/'bb-job.before-cow.py').read_bytes()).hexdigest()
    remote('''from pathlib import Path
import hashlib,os
def install(source,destination,predecessor=None):
 dst=Path(destination);dst.parent.mkdir(parents=True,exist_ok=True)
 if predecessor is not None:
  assert hashlib.sha256(dst.read_bytes()).hexdigest()==predecessor, 'predecessor changed: '+str(dst)
 elif dst.exists():
  assert dst.read_bytes()==source,'new helper already exists with different content'
 tmp=dst.with_name(dst.name+'.efficiency-install-'+str(os.getpid()))
 with tmp.open('xb') as f:f.write(source);f.flush();os.fsync(f.fileno())
 tmp.chmod(0o755);os.replace(tmp,dst)
 print(str(dst),hashlib.sha256(dst.read_bytes()).hexdigest())
root=Path('/mnt/bb/efficiency')
for name in ['bb_target_store.py','bb_cargo_cache.py']:
 install((root/name).read_bytes(),'/home/var/bin/'+name)
install(b'#!/bin/sh\\nexec python3 /home/var/bin/bb_cargo_cache.py "$@"\\n','/home/var/bin/bb-cargo/cargo')
install((root/'bb-job.cow-candidate.py').read_bytes(),'/home/var/bin/bb-job.py',%r)
''' % old_job)
    old = hashlib.sha256((ROOT/'buildbox.before-cow.mjs').read_bytes()).hexdigest()
    # Same atomic install in WSL; preserve the latest concurrent owner change.
    code = '''from pathlib import Path
import hashlib,os
p=Path('/home/var/bin/buildbox.mjs')
assert hashlib.sha256(p.read_bytes()).hexdigest()==%r, 'WSL predecessor changed'
source=Path('/mnt/q/lanes/efficiency/buildbox.cow-candidate.mjs').read_bytes()
tmp=p.with_name(p.name+'.efficiency-cow-'+str(os.getpid()))
with tmp.open('xb') as f:f.write(source);f.flush();os.fsync(f.fileno())
tmp.chmod(p.stat().st_mode);os.replace(tmp,p)
print(str(p),hashlib.sha256(p.read_bytes()).hexdigest())
''' % old
    r = subprocess.run(['wsl','-d','Ubuntu','-e','python3','-'],input=code,text=True,capture_output=True)
    print(r.stdout,end=''); print(r.stderr,end=''); r.check_returncode()


if __name__ == '__main__':
    main()
