from pathlib import Path
import hashlib,shutil,os
p=Path('/home/var/bin/build-slot');old=p.read_text()
a='  unset CARGO_INCREMENTAL';b='  export CARGO_INCREMENTAL=0  # ephemeral targets cannot reuse incremental output'
assert old.count(a)==1
new=old.replace(a,b).replace('CARGO_PROFILE_DEV_INCREMENTAL=true CARGO_PROFILE_TEST_INCREMENTAL=true','CARGO_PROFILE_DEV_INCREMENTAL=false CARGO_PROFILE_TEST_INCREMENTAL=false')
shutil.copy2(p,p.with_name('build-slot.before-efficiency-20261023'))
q=p.with_name('build-slot.efficiency.tmp');q.write_text(new);q.chmod(p.stat().st_mode);os.replace(q,p)
print('native build-slot',hashlib.sha256(p.read_bytes()).hexdigest())
