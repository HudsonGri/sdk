"""Codex: bounded passive bb2 cache policy/socket/disk evidence; never launches a server."""
import datetime,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
REMOTE=r'''
import collections,hashlib,json,os
from pathlib import Path
sources={}
for name in ['bb-job.py','sccache-rustc']:
 p=Path('/home/var/bin')/name;b=p.read_bytes();lines=b.decode().splitlines()
 sources[name]={'sha256':hashlib.sha256(b).hexdigest(),'policy_lines':[{'line':i+1,'text':line.strip()}for i,line in enumerate(lines)if any(s in line for s in ['SCCACHE_CACHE_SIZE','SCCACHE_SERVER_UDS','--start-server','server_identity','def stop_server','def cost_receipt'])]}
sockets={}
for line in Path('/proc/net/unix').read_text().splitlines()[1:]:
 parts=line.split()
 if len(parts)>=8 and parts[3]=='00010000' and 'sccache' in parts[7]:sockets[parts[6]]={'path':parts[7],'owners':[]}
processes=0;states=collections.Counter();parents=collections.Counter();rss_kib=0;threads=0
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  if p.joinpath('comm').read_text().strip()!='sccache':continue
  processes+=1
  status={line.split(':',1)[0]:line.split(':',1)[1].strip()for line in p.joinpath('status').read_text().splitlines()if ':'in line}
  states[status.get('State','unknown').split()[0]]+=1;parents[status.get('PPid','unknown')]+=1
  rss_kib+=int(status.get('VmRSS','0').split()[0]);threads+=int(status.get('Threads','0'))
  for fd in p.joinpath('fd').iterdir():
   try:target=os.readlink(fd)
   except OSError:continue
   if target.startswith('socket:[') and target[8:-1] in sockets:sockets[target[8:-1]]['owners'].append(int(p.name))
 except OSError:continue
disks={}
for path in ['/','/mnt/bb']:
 st=os.statvfs(path);disks[path]={'free_percent':100*st.f_bavail/st.f_blocks,'free_bytes':st.f_bavail*st.f_frsize}
print(json.dumps({'sources':sources,'sccache_processes':processes,'process_states':dict(states),'process_parent_counts':dict(parents),'process_rss_kib':rss_kib,'process_threads':threads,'listening_sockets':list(sockets.values()),'disks':disks,'note':'Processes are not equivalent to servers; listener owners may share descriptors. RSS is a point aggregate, not exclusive memory. No recursive du or cache server command.'}))
'''
if __name__=='__main__':
 result=subprocess.run(['wsl','-d','Ubuntu','-e','ssh','-o','BatchMode=yes','bb2','python3 -'],input=REMOTE,text=True,capture_output=True,timeout=25,check=True)
 value=json.loads(result.stdout);value.update(author='Codex',at=datetime.datetime.now(datetime.timezone.utc).isoformat(),bb2_jobs_started=0)
 (ROOT/'bb2-policy-audit.json').write_text(json.dumps(value,indent=2)+'\n')
 print(json.dumps({'at':value['at'],'processes':value['sccache_processes'],'states':value['process_states'],'rss_kib':value['process_rss_kib'],'threads':value['process_threads'],'listeners':len(value['listening_sockets']),'disks':value['disks'],'policy':value['sources']}))
