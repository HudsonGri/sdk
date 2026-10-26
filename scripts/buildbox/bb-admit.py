#!/usr/bin/env python3
"""bb2 admission: event-driven Unix socket, per-tenant FIFO, bounded tier/share buckets.

No ticket polling or process-directory rescans. SCM_RIGHTS retains the caller's
stdio. pidfds hold a slot until the command actually exits, including when its
client is killed. A daemon restart adopts journaled live leases before admitting.
"""
import array
from collections import defaultdict, deque
import fcntl
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import struct
import subprocess
import sys
import time
import uuid
from admission_policy import cargo_query, canonical_argv, resolve_wrapper, capture_limits, LIMIT_ENV, cache_authority, disk_pressure, disk_free, disk_low
from admission_policy import process_identity as identity, atomic_json as atomic, CPU_LEASE_SHAPE, require_tenant
import cpu_leases
import input_closure

ROOT = Path(os.environ.get('BB_ADMIT_ROOT', '/mnt/bb/admit'))
SOCKET = ROOT/'admit.sock'
SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def validate_root():
    if ROOT!=Path('/mnt/bb/admit') and cpu_leases.CGROUP_ROOT==Path('/sys/fs/cgroup/bbjobs.slice/buildbox-cpu'):
        raise cpu_leases.LeaseError('admission-root','the host has one admission authority at /mnt/bb/admit')


def live_pidfd(pid,birth):
    if not birth or identity(pid)!=birth:return None
    try:fd=os.pidfd_open(pid)
    except ProcessLookupError:return None
    if identity(pid)!=birth:
        os.close(fd);return None
    return fd


class Scheduler:
    def __init__(self, limits=None, caps=None):
        self.limits = limits or {'compile':40, 'other':48, 'light':256}
        self.caps = caps or {'graphcoder':16, 'cloud':3}
        self.queues = defaultdict(lambda:defaultdict(deque))
        self.running = defaultdict(int); self.shares = defaultdict(int)
        self.critical = 0; self.proof = 0
        self.turn = defaultdict(lambda:-1); self.clock = 0
        self.free = set(range(CPU_LEASE_SHAPE[0]))

    def add(self, ticket):
        if ticket['tier']==0 and self.critical >= 4: ticket['tier']=2
        self.queues[(ticket['cls'],ticket['tier'])][ticket['tenant']].append(ticket)

    def take(self, cls=None):
        if not self.free: return None
        # Four tiers and the configured tenant set are independent of ticket
        # count. Inspect only each tenant's head; never scan waiting tickets.
        for tier in range(4):
            if tier==0 and self.critical>=4: continue
            eligible=[]
            for category in ([cls] if cls is not None else self.limits):
                if self.running[category] >= self.limits[category]: continue
                for tenant, q in self.queues[(category,tier)].items():
                    if not q: continue
                    share=sum(self.shares[(kind,tenant)] for kind in self.limits)
                    if tier!=0 and share>=self.caps.get(tenant,CPU_LEASE_SHAPE[0]): continue
                    if tier==3 and (self.proof or any(any(self.queues[(kind,t)].values()) for kind in self.limits for t in range(3))): continue
                    eligible.append((share,self.turn[(category,tier,tenant)],q[0]['seq'],tenant,category))
            if eligible:
                _,_,_,tenant,category=min(eligible)
                tenants=self.queues[(category,tier)];t=tenants[tenant].popleft()
                self.turn[(category,tier,tenant)]=self.clock; self.clock+=1
                if not tenants[tenant]: del tenants[tenant]
                self.running[category]+=1; self.shares[(category,tenant)]+=1
                self.critical+=tier==0; self.proof+=tier==3
                t['cpu_index']=min(self.free);self.free.remove(t['cpu_index'])
                return t
        return None

    def release(self,t):
        self.running[t['cls']]-=1; self.shares[(t['cls'],t['tenant'])]-=1
        self.critical-=t['tier']==0; self.proof-=t['tier']==3
        if t['cpu_index'] in self.free:raise RuntimeError('kind=CPU_LEASE step=double-release recovery='+str(ROOT/'daemon.log'))
        self.free.add(t['cpu_index'])


def serve():
    validate_root()
    ROOT.mkdir(parents=True,exist_ok=True)
    lock=open(ROOT/'daemon.lock','a')
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: return
    limits={'compile':40,'other':48,'light':256}; caps={'graphcoder':16,'cloud':3}
    conf=ROOT/'admit.conf'
    if conf.exists():
        for token in conf.read_text().split():
            key,sep,value=token.partition('=')
            if sep and value.isdigit():
                if key.startswith('tenant.'): caps[key[7:]]=int(value)
                elif key in limits: limits[key]=int(value)
    sched=Scheduler(limits,caps); sel=selectors.DefaultSelector(); jobs={}; clients={}; sequence=0
    SOCKET.unlink(missing_ok=True)
    server=socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET); server.bind(str(SOCKET)); SOCKET.chmod(0o600); server.listen(256)
    sel.register(server,selectors.EVENT_READ,'accept')
    # Reconcile durable tickets with root-owned kernel custody once, before
    # admission. An orphan between spawn and journal publication still owns
    # its slot. A PID exit is not permission to overlap live descendants.
    restored=set()
    def restore(t,journal):
        fd=live_pidfd(t['pid'],t['identity'])
        if fd is None and t.get('id') and cpu_leases.record_path(t['id']).exists():
            child=subprocess.Popen(['sudo','-n','/usr/bin/python3','-B',str(Path(cpu_leases.__file__).resolve()),'observe',t['id']],stdout=subprocess.DEVNULL)
            t.update(child=child,pid=child.pid,identity=identity(child.pid),recovered=True)
            fd=os.pidfd_open(child.pid)
        if fd is None:journal.unlink(missing_ok=True);return
        if t.get('cpu_index') not in sched.free:
            os.close(fd)
            raise cpu_leases.LeaseError('recovery-generation','drain legacy admission before CPU lease cut-over')
        sched.free.remove(t['cpu_index'])
        t.update(conn=None,journal=journal,pidfd=fd)
        sched.running[t['cls']]+=1;sched.shares[(t['cls'],t['tenant'])]+=1
        sched.critical+=t['tier']==0;sched.proof+=t['tier']==3
        jobs[fd]=t;sel.register(fd,selectors.EVENT_READ,'done');restored.add(t['id'])
    for journal in ROOT.glob('lease-*.json'):
        t=json.loads(journal.read_text())
        restore(t,journal)
    for file in cpu_leases.CONTROL_ROOT.glob('*.json'):
        token=file.stem
        if token in restored:continue
        record,_=cpu_leases.verified_record(token,directory_required=False)
        manager=record['manager']
        t={'id':token,'cpu_index':record['index'],'pid':manager['pid'],'identity':manager['identity'],
           'tier':record['tier'],'tenant':record['tenant'],'cls':record['cls']}
        restore(t,ROOT/('lease-'+token+'.json'))
    atomic(ROOT/'daemon.json',{'pid':os.getpid(),'identity':identity(os.getpid()),'pid_namespace':os.readlink('/proc/self/ns/pid'),'limits':limits,'caps':caps,'source_sha256':SOURCE_SHA256})
    ready=os.environ.get('BB_ADMIT_READY_FD')
    if ready:
        os.write(int(ready),b'ready'); os.close(int(ready))

    def close_client(c):
        t=clients.pop(c,None)
        try: sel.unregister(c)
        except KeyError: pass
        c.close()
        if t:
            t['conn']=None; t['cancelled']=True
            if t.get('pid') and identity(t['pid'])==t['identity']:
                # Release only on the pidfd exit event, after SIGKILL has taken
                # effect. This also covers an uncatchable kill of the client.
                try: os.killpg(t['pid'],signal.SIGKILL)
                except ProcessLookupError: pass
            else:
                for fd in t.get('fds',[]): os.close(fd)
                t['fds']=[]

    def launch(t):
        if t.get('cancelled'):
            sched.release(t); return
        env=t['env']; env['BB_ADMITTED']='1'
        width=CPU_LEASE_SHAPE[1]
        requested=env.get('CARGO_BUILD_JOBS','')
        env['CARGO_BUILD_JOBS']=str(min(width,int(requested)) if requested.isdigit() and int(requested)>0 else width)
        env['BUILD_SLOT_ADMISSION_START']=t['admission_start']
        env['BB_ADMIT_WAIT_SECONDS']=str(round(time.time()-t['at'],6))
        env['BB_ADMIT_SOURCE_SHA256']=SOURCE_SHA256
        env['BB_ADMIT_TIER']=str(t['tier']); env['BB_ADMIT_TENANT']=t['tenant']
        rendezvous=None;ready=None
        try:
            if ROOT==Path('/mnt/bb/admit'):
                readings=disk_free()
                pressure=disk_pressure(readings)
                if pressure:os.write(t['fds'][2],pressure.encode())
                if disk_low(readings):
                    os.write(t['fds'][2],('BB_DISK_FREE '+json.dumps(readings)+'\nBB_DISK_LOW INFRA: / and /mnt/bb require at least 10% free\n').encode())
                    alert={'at':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'event':'BB_DISK_LOW','repo':env.get('BUILD_SLOT_REPO'),'lane':env.get('BUILD_SLOT_LABEL'),'pr':env.get('BUILD_SLOT_PR'),'receipt_meta':env.get('BUILD_SLOT_RECEIPT_META'),'disk_free':readings}
                    log=Path('/mnt/bb/log/alerts.jsonl');log.parent.mkdir(parents=True,exist_ok=True)
                    with log.open('a') as stream:
                        fcntl.flock(stream,fcntl.LOCK_EX);stream.write(json.dumps(alert)+'\n');stream.flush()
                    meta=env.get('BUILD_SLOT_RECEIPT_META')
                    if meta:atomic(meta+'.runtime',{'disk_free':readings,'cache':'not-started','admission':{'tier':t['tier'],'tenant':t['tenant'],'wait_seconds':env['BB_ADMIT_WAIT_SECONDS']}})
                    t['conn'].send(json.dumps({'rc':224}).encode());sched.release(t);close_client(t['conn']);return
            command=t['argv']
            actual=canonical_argv(command, env)
            proof=input_closure.admit(actual,env)
            os.write(t['fds'][2],('BB_INPUTS_ADMITTED '+json.dumps(proof)+'\n').encode())
            wrapper=resolve_wrapper(env, compiler=bool(actual and Path(actual[0]).name=='cargo'), cwd=t['cwd'])
            if wrapper: os.write(t['fds'][2],('BB_WRAPPER_ADMITTED '+json.dumps(wrapper)+'\n').encode())
            if env.get('BB_CACHE_DISABLED')!='1' and wrapper and Path(wrapper['path'])==Path(__file__).with_name('sccache-rustc').resolve():
                owner=cache_authority().admitted(env)
                os.write(t['fds'][2],('BB_CACHE_ADMITTED '+json.dumps(owner)+'\n').encode())
            if env.get('BB_ADMIT_WRAPPED_COMMAND')!='1':
                command=[sys.executable,str(Path(__file__).with_name('bb-job.py')),*command]
            command=cpu_leases.launch(t['cpu_index'],t['id'],t['tenant'],command)
            rendezvous='bb-admit-ready-'+t['id']
            ready=socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET);ready.bind('\0'+rendezvous);ready.listen(1)
            env['BB_CPU_ADMISSION_SOCKET']='@'+rendezvous
            child=subprocess.Popen(command,cwd=t['cwd'],env=env,stdin=t['fds'][0],stdout=t['fds'][1],stderr=t['fds'][2],start_new_session=True)
            t.update(pid=child.pid,identity=identity(child.pid),child=child)
            t['journal']=ROOT/('lease-'+t['id']+'.json')
            atomic(t['journal'],{k:t[k] for k in ['id','pid','identity','tier','tenant','cls','cpu_index']})
            fd=os.pidfd_open(child.pid); t['pidfd']=fd; jobs[fd]=t
            import select
            notified,_,_=select.select([ready,fd],[],[])
            if ready not in notified:raise cpu_leases.LeaseError('constructor-exit','kernel constructor ended before handoff')
            with ready.accept()[0] as connection:
                peer_pid,peer_uid,_=struct.unpack('3i',connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
                if peer_uid!=0 or connection.recv(64)!=t['id'].encode():raise cpu_leases.LeaseError('constructor-peer','untrusted ready peer')
                record,_=cpu_leases.verified_record(t['id'])
                if record['manager']!={'pid':peer_pid,'identity':identity(peer_pid)}:
                    raise cpu_leases.LeaseError('constructor-birth','ready peer differs from root custody')
                connection.send(b'EXEC')
            sel.register(fd,selectors.EVENT_READ,'done')
            meta=env.get('BUILD_SLOT_RECEIPT_META')
            if meta: atomic(meta+'.job',{'pid':child.pid,'identity':t['identity'],'pid_namespace':os.readlink('/proc/self/ns/pid')})
        except Exception as e:
            child=t.get('child')
            if child and child.poll() is None:
                try:os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                child.wait(timeout=10)
            if cpu_leases.record_path(t['id']).exists():
                subprocess.run(['sudo','-n','/usr/bin/python3','-B',str(Path(cpu_leases.__file__).resolve()),
                                'cancel',t['id']],check=True,stdout=subprocess.DEVNULL)
            if t.get('pidfd') is not None:
                fd=t['pidfd'];jobs.pop(fd,None)
                try:sel.unregister(fd)
                except KeyError:pass
                os.close(fd)
            if t.get('journal'):t['journal'].unlink(missing_ok=True)
            t.pop('pid',None)
            try: t['conn'].send(json.dumps({'rc':224,'error':str(e)}).encode())
            except OSError: pass
            sched.release(t); close_client(t['conn'])
        finally:
            if ready:ready.close()
            for fd in t['fds']: os.close(fd)
            t['fds']=[]

    while True:
        for key,_ in sel.select():
            if key.data=='accept':
                c,_=server.accept(); c.setblocking(False); clients[c]=None; sel.register(c,selectors.EVENT_READ,'client')
            elif key.data=='client':
                c=key.fileobj
                try: data,anc,_,_=c.recvmsg(4*1024*1024,socket.CMSG_SPACE(3*array.array('i').itemsize))
                except BlockingIOError: continue
                except (ConnectionResetError,BrokenPipeError):close_client(c);continue
                if not data: close_client(c); continue
                if clients[c] is not None: continue
                fds=array.array('i')
                for level,typ,raw in anc:
                    if level==socket.SOL_SOCKET and typ==socket.SCM_RIGHTS: fds.frombytes(raw[:len(raw)//fds.itemsize*fds.itemsize])
                try:
                    t=json.loads(data);require_tenant(t.get('tenant'),t.get('argv',[]))
                except (ValueError,RuntimeError,TypeError) as error:
                    for fd in fds:os.close(fd)
                    c.send(json.dumps({'rc':224,'error':str(error)}).encode());close_client(c);continue
                t.update(conn=c,fds=list(fds),id=uuid.uuid4().hex,seq=sequence,at=time.time()); sequence+=1
                if len(fds)!=3 or t['cls'] not in limits or t['tier'] not in range(4) or t['tenant'] not in caps:
                    for fd in fds: os.close(fd)
                    c.send(json.dumps({'rc':224,'error':'invalid admission class/tier/tenant; configure tenant.<name> cap'}).encode())
                    close_client(c); continue
                clients[c]=t; sched.add(t)
                os.write(t['fds'][2],('BB_ADMIT_TICKET '+json.dumps({'id':t['id'],'tier':t['tier'],'tenant':t['tenant']})+'\n').encode())
            elif key.data=='done':
                fd=key.fd; t=jobs.pop(fd); sel.unregister(fd); os.close(fd)
                rc=t['child'].wait() if 'child' in t else 137
                if t.get('recovered'):rc=137  # Original command outcome is unavailable.
                if rc<0: rc=128-rc
                # A crashed manager does not release live descendants. The
                # privileged adapter retires this exact root-custodied token.
                if cpu_leases.record_path(t['id']).exists():
                    cleanup=subprocess.run(['sudo','-n','/usr/bin/python3','-B',str(Path(cpu_leases.__file__).resolve()),'cancel',t['id']],capture_output=True,text=True)
                    if cleanup.returncode:
                        raise cpu_leases.LeaseError('retirement',cleanup.stderr)
                    if t.get('env',{}).get('BUILD_SLOT_RECEIPT_META'):
                        terminal=Path(t['env']['BUILD_SLOT_RECEIPT_META']+'.cpu-lease.terminal')
                        if not terminal.exists():atomic(terminal,json.loads(cleanup.stdout))
                t['journal'].unlink(missing_ok=True); sched.release(t)
                c=t.get('conn')
                if c:
                    try: c.send(json.dumps({'rc':rc}).encode())
                    except OSError: pass
                    # It is complete: disconnect must not signal a reused PID.
                    t.pop('pid',None); close_client(c)
        while True:
            t=sched.take()
            if t is None: break
            launch(t)


def client(args):
    validate_root()
    # A daemon may have been started by a caller with different limits. The
    # ticket carries this caller's environment through that process boundary.
    if os.environ.get('BB_CALLER_RLIMITS_READY') != '1':
        os.environ[LIMIT_ENV] = json.dumps(capture_limits())
        os.environ['BB_CALLER_RLIMITS_READY'] = '1'
    tier,tenant,sep,*argv=args
    if sep!='--' or not argv: raise ValueError('bb-admit <tier> <tenant> -- command ...')
    require_tenant(tenant,argv)
    # Before daemon startup, queueing, cache startup or any CPU lease.
    proof=input_closure.admit(canonical_argv(argv,os.environ))
    print('BB_INPUTS_ADMITTED '+json.dumps(proof),file=sys.stderr,flush=True)
    owner=cpu_leases.current(required=False)
    if owner is not None:
        if tenant != owner['tenant']:
            raise cpu_leases.LeaseError('nested-tenant', 'nested command cannot change its lease accounting tenant')
        os.environ['BB_ADMITTED']='1'
        os.environ['BB_ADMIT_TENANT']=owner['tenant']
        if os.environ.get('BB_ADMIT_WRAPPED_COMMAND')=='1':os.execvpe(argv[0],argv,os.environ)
        os.execv(sys.executable,[sys.executable,str(Path(__file__).with_name('bb-job.py')),*argv])
    tier={'crit':0,'prio':0,'merge':1,'qual':1,'lane':2,'proof':3}.get(tier,int(tier) if tier.isdigit() else 2)
    if cargo_query(canonical_argv(argv, os.environ)):
        # Read-only Cargo queries retain receipts and hermetic execution but
        # take no build ticket, even when every tenant/compile slot is occupied.
        os.environ['BB_ADMITTED']='1'
        os.environ['BB_ADMIT_WAIT_SECONDS']='0'
        if os.environ.get('BB_ADMIT_WRAPPED_COMMAND')=='1': os.execvpe(argv[0],argv,os.environ)
        os.execv(sys.executable,[sys.executable,str(Path(__file__).with_name('bb-job.py')),*argv])
    ROOT.mkdir(parents=True,exist_ok=True)
    # A rolling upgrade takes EXCLUSIVE here: existing admitted/queued work
    # drains uninterrupted while new ticket handshakes block in flock.
    with open(ROOT/'upgrade.lock','a') as upgrade:
        fcntl.flock(upgrade,fcntl.LOCK_SH)
        with open(ROOT/'startup.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            c=socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            try: c.connect(str(SOCKET))
            except (FileNotFoundError,ConnectionRefusedError):
                log=open(ROOT/'daemon.log','ab')
                # Startup notification uses a pipe/flock, not a ticket poll loop.
                ready_r,ready_w=os.pipe()
                process=subprocess.Popen([sys.executable,__file__,'serve'],env={**os.environ,'BB_ADMIT_READY_FD':str(ready_w)},pass_fds=[ready_w],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
                os.close(ready_w)
                import select
                # One-time daemon startup wait. Tickets thereafter wait solely on
                # their socket, and kernel pidfd events release running slots.
                if not select.select([ready_r],[],[],5)[0] or os.read(ready_r,32)!=b'ready':
                    raise RuntimeError('bb-admit daemon did not become ready; see '+str(ROOT/'daemon.log'))
                os.close(ready_r)
                c.connect(str(SOCKET))
        stamp=os.environ.get('BUILD_SLOT_ADMISSION_START') or time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
        env=dict(os.environ)
        cls=env.get('BB_ADMIT_CLASS','compile')
        fds=array.array('i',[0,1,2])
        c.sendmsg([json.dumps({'tier':tier,'tenant':tenant,'cls':cls,'argv':argv,'cwd':os.getcwd(),'env':env,'admission_start':stamp}).encode()],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,fds)])
    for sig in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP]:
        signal.signal(sig,lambda n,f: (_ for _ in ()).throw(SystemExit(128+n)))
    data=c.recv(65536)
    if not data: print('bb-admit: daemon disconnected; INFRA',file=sys.stderr); return 224
    result=json.loads(data)
    if result.get('error'): print('bb-admit: '+result['error'],file=sys.stderr)
    return result['rc']


if __name__=='__main__':
    try:
        if sys.argv[1:] == ['serve']: serve()
        else: raise SystemExit(client(sys.argv[1:]))
    except (OSError,RuntimeError,ValueError,KeyError) as error:
        print(str(error) if 'recovery=' in str(error) else str(cpu_leases.LeaseError('admission',error)),file=sys.stderr)
        raise SystemExit(224)
