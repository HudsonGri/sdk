import hashlib,os,pathlib,shutil
p=pathlib.Path('/home/var/bin/bb-job.py'); q=p.with_name('bb-job.efficiency-candidate.py')
assert hashlib.sha256(p.read_bytes()).hexdigest()=='05e85a5f4062c74b26a1b7aa1d9cbb48c2791ae3f543f9d8cb5f0b548e4cc991','live source changed; stop'
compile(q.read_text(),str(q),'exec')
shutil.copy2(p,p.with_name('bb-job.before-efficiency-20261023.py'))
q.chmod(p.stat().st_mode); os.replace(q,p)
print(hashlib.sha256(p.read_bytes()).hexdigest())
