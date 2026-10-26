"""Codex: run on bb2; actual XFS reflinks and competing processes."""
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import stat
import tempfile
import time
import subprocess
import unittest
import sys
import select
from unittest.mock import patch

import bb_target_store as store
import bb_cargo_cache as cargo_cache


def flight(root, target, queue, delay=0):
    time.sleep(delay)
    session = store.Session(root, 'same', target).open()
    status = session.receipt['status']
    if status == 'miss':
        (Path(target) / 'artifact').write_bytes(b'completed')
        time.sleep(0.1)
        session.publish()
    queue.put(status)
    session.close()


def orphaned_builder(lock_path, connection, inherit):
    with open(lock_path, 'a') as lease:
        store.fcntl.flock(lease, store.fcntl.LOCK_EX)
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(0.3)'],
                               pass_fds=(lease.fileno(),) if inherit else ())
        connection.send(child.pid)
        os._exit(0)


def try_local_lease(path, connection):
    with Path(path).open('a') as lease:
        try:
            store.fcntl.flock(lease, store.fcntl.LOCK_EX | store.fcntl.LOCK_NB)
            connection.send(True)
        except BlockingIOError:
            connection.send(False)


class TargetStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/mnt/bb/efficiency')
        self.base = Path(self.tmp.name)
        self.root = self.base / 'store'
        self.target = self.base / 'target'; self.target.mkdir()

    def tearDown(self):
        store.permissions(self.base, True)
        self.tmp.cleanup()

    def session(self, key='one', target=None):
        return store.Session(self.root, key, target or self.target).open()

    def competing_lease(self, held_path, candidate_path):
        with held_path.open('a') as lease:
            store.fcntl.flock(lease, store.fcntl.LOCK_EX)
            parent, child = mp.Pipe()
            process = mp.Process(target=try_local_lease, args=(candidate_path, child))
            process.start()
            try:
                self.assertTrue(parent.poll(2), 'competing lease did not report')
                acquired = parent.recv()
                process.join(2)
                self.assertEqual(0, process.exitcode)
                return acquired
            finally:
                if process.is_alive():
                    process.terminate(); process.join(2)
                parent.close(); child.close()

    def test_distinct_jobs_same_virtual_target_do_not_serialize(self):
        target = self.base / 'work/target'; target.mkdir(parents=True)
        a = cargo_cache.local_target_lock(self.base / 'job-a', target)
        b = cargo_cache.local_target_lock(self.base / 'job-b', target)
        self.assertTrue(self.competing_lease(a, b))
        self.assertNotEqual(a.stat().st_ino, b.stat().st_ino)
        # Old lexical parent is shared even when target is a private bind.
        old_a = target.parent / ('.' + target.name + '.bb-store-lock')
        old_b = target.parent / ('.' + target.name + '.bb-store-lock')
        with self.assertRaises(AssertionError):
            self.assertTrue(self.competing_lease(old_a, old_b))

    def test_same_job_target_lease_excludes_aliases_not_distinct_targets(self):
        job = self.base / 'job'
        target = self.base / 'work/target'; target.mkdir(parents=True)
        alias = self.base / 'alias'; alias.symlink_to(target, target_is_directory=True)
        a = cargo_cache.local_target_lock(job, target)
        self.assertFalse(self.competing_lease(a, cargo_cache.local_target_lock(job, alias)))
        b = cargo_cache.local_target_lock(job, target / 'separate')
        self.assertTrue(self.competing_lease(a, b))
        # An over-specific per-invocation lock loses same-target exclusion.
        bad = cargo_cache.local_target_lock(self.base / 'wrong-invocation-owner', target)
        with self.assertRaises(AssertionError):
            self.assertFalse(self.competing_lease(a, bad))

    def test_target_hierarchy_excludes_parent_writers_but_allows_siblings(self):
        root = self.base / 'virtual-target'; root.mkdir()
        job = self.base / 'job'
        a, b = root / 'a', root / 'b'
        a.mkdir(); b.mkdir()
        # Use a forked reader to inspect every active lease non-blockingly;
        # otherwise a test timeout could accidentally qualify exclusion.
        with cargo_cache.local_target_leases(job, a, root):
            self.assertTrue(self.competing_lease_check(job, b, root))
            self.assertFalse(self.competing_lease_check(job, a, root))
            self.assertFalse(self.competing_lease_check(job, root, root))
        with cargo_cache.local_target_leases(job, root, root):
            self.assertFalse(self.competing_lease_check(job, a, root))
        # Old leaf-only ownership admits a child during its parent's snapshot.
        with self.assertRaises(AssertionError):
            self.assertFalse(self.competing_lease(
                cargo_cache.local_target_lock(job, root),
                cargo_cache.local_target_lock(job, a)))

    def competing_lease_check(self, job, target, root):
        # Probe exactly the production acquisitions, replacing only blocking
        # flock with its nonblocking form so the oracle cannot hang.
        def probe(connection):
            actual = store.fcntl.flock
            def nonblocking(fd, mode):
                return actual(fd, mode | store.fcntl.LOCK_NB)
            with patch.object(cargo_cache.fcntl, 'flock', side_effect=nonblocking):
                try:
                    with cargo_cache.local_target_leases(job, target, root):
                        connection.send(True)
                except BlockingIOError:
                    connection.send(False)
        parent, child = mp.Pipe()
        process = mp.Process(target=probe, args=(child,)); process.start()
        try:
            self.assertTrue(parent.poll(2)); result = parent.recv()
            process.join(2); self.assertEqual(0, process.exitcode)
            return result
        finally:
            if process.is_alive(): process.terminate(); process.join(2)
            parent.close(); child.close()

    def test_reflink_private_writes_and_deletes(self):
        s = self.session()
        (self.target / 'binary').write_bytes(b'base artifact')
        s.publish(); s.close()
        base = self.root / 'entries/one/target/binary'
        before = hashlib.sha256(base.read_bytes()).hexdigest()
        clone = self.base / 'clone'; clone.mkdir()
        t = self.session(target=clone)
        self.assertEqual('hit', t.receipt['status'])
        self.assertNotEqual(base.stat().st_ino, (clone / 'binary').stat().st_ino)
        (clone / 'binary').write_bytes(b'changed')
        (clone / 'binary').unlink()
        self.assertEqual(before, hashlib.sha256(base.read_bytes()).hexdigest())
        self.assertFalse(base.stat().st_mode & 0o222)
        t.close()

    def test_shared_inode_negative_control_is_detected(self):
        # The old shared/hardlink shape must violate the same isolation oracle.
        a = self.target / 'a'; a.write_bytes(b'base')
        b = self.target / 'b'; os.link(a, b)
        b.write_bytes(b'leak')
        with self.assertRaises(AssertionError):
            self.assertEqual(b'base', a.read_bytes())

    def test_builder_lease_survives_proxy_death_and_negative_control(self):
        for inherit in [True,False]:
            path=self.base/('orphan-'+str(inherit))
            parent,child=mp.Pipe()
            process=mp.Process(target=orphaned_builder,args=(path,child,inherit));process.start()
            pid=parent.recv();process.join(1);self.assertEqual(0,process.exitcode)
            with path.open('a') as lease:
                if inherit:
                    with self.assertRaises(BlockingIOError):
                        store.fcntl.flock(lease,store.fcntl.LOCK_EX|store.fcntl.LOCK_NB)
                else:
                    store.fcntl.flock(lease,store.fcntl.LOCK_EX|store.fcntl.LOCK_NB)
            fd=os.pidfd_open(pid)
            try:self.assertTrue(select.select([fd],[],[],1)[0])
            finally:os.close(fd)

    def test_identical_requests_single_flight(self):
        a = self.base / 'a'; a.mkdir()
        b = self.base / 'b'; b.mkdir()
        q = mp.Queue()
        processes = [mp.Process(target=flight, args=(self.root, a, q)),
                     mp.Process(target=flight, args=(self.root, b, q, 0.02))]
        for p in processes: p.start()
        for p in processes:
            p.join(3); self.assertEqual(0, p.exitcode)
        self.assertEqual(['hit', 'miss'], sorted([q.get(timeout=1), q.get(timeout=1)]))
        self.assertEqual(b'completed', (b / 'artifact').read_bytes())

    def test_failed_builder_does_not_publish(self):
        s = self.session(); (self.target / 'partial').write_text('incomplete')
        s.close()  # The job exits nonzero and therefore never calls publish.
        clone = self.base / 'clone'; clone.mkdir()
        t = self.session(target=clone)
        self.assertEqual('miss', t.receipt['status'])
        self.assertEqual([], list(clone.iterdir())); t.close()

    def test_incomplete_staging_is_not_an_entry(self):
        s = self.session(); s.close()
        partial = self.root / 'staging/one-crashed'; partial.mkdir()
        (partial / 'complete.json').write_text('{}')
        s = self.session(); self.assertEqual('miss', s.receipt['status']); s.close()

    def test_symlink_escape_never_published(self):
        s = self.session()
        (self.target / 'escape').symlink_to('../external')
        with self.assertRaises(ValueError): s.publish()
        self.assertFalse(s.entry.exists()); s.close()

    def test_fifo_never_published(self):
        s = self.session(); os.mkfifo(self.target / 'fifo')
        with self.assertRaises(ValueError): s.publish()
        self.assertFalse(s.entry.exists()); s.close()

    def test_launcher_scaffold_is_never_copied_or_chmodded(self):
        scaffold = self.target / '.host-src'; scaffold.mkdir()
        (scaffold / 'source').write_text('not an artifact')
        (self.target / 'binary').write_text('artifact')
        s = self.session(); s.publish(); s.close()
        self.assertFalse((s.entry / 'target/.host-src').exists())
        out = self.base / 'out'; out.mkdir()
        private = out / '.host-src'; private.mkdir(); private.chmod(0o500)
        t = self.session(target=out)
        self.assertEqual(0o500, stat.S_IMODE(private.stat().st_mode))
        self.assertTrue((out / 'binary').stat().st_mode & stat.S_IWUSR)
        t.close()

    def test_eviction_respects_lease_and_budget(self):
        s = self.session(); (self.target / 'x').write_bytes(b'1234'); s.publish(budget=4)
        other = self.base / 'other'; other.mkdir()
        t = self.session('two', other); (other / 'y').write_bytes(b'1234')
        t.publish(budget=4)
        self.assertEqual('busy-budget', t.receipt['publish'])
        self.assertTrue(s.entry.exists())
        s.close(); t.publish(budget=4)
        self.assertFalse(s.entry.exists()); self.assertTrue(t.entry.exists()); t.close()

    def test_key_requires_full_provenance_and_changes_on_inputs(self):
        meta = {'repo':'sdk', 'source':{'content_sha256':'source', 'commit':'a'},
                'argv':['cargo','test','-p','one']}
        env = {'RUSTFLAGS':'-C debuginfo=0', 'BB_TARGET_REQUEST_SHA256':'payload',
               'HOME':'/job1/home', 'CARGO_BUILD_JOBS':'16'}
        with patch.object(store, 'tool_identity', return_value=['tools']):
            key = store.request_key(meta, env, Path('/job1'))
            components = {}
            self.assertEqual(key, store.request_key(meta, env, Path('/job1'), components))
            self.assertEqual({'version','source','argv','cwd','environment','tools','target'}, set(components))
            self.assertTrue(all(len(value) == 64 for value in components.values()))
            self.assertNotIn('debuginfo', json.dumps(components))
            with patch.object(store, 'VERSION', 'different-custody-generation'):
                self.assertNotEqual(key, store.request_key(meta, env, Path('/job1')))
            self.assertEqual(key, store.request_key(meta, {**env, 'HOME':'/job2/home',
                                      'CARGO_BUILD_JOBS':'8'}, Path('/job2')))
            for name in ['RUSTFLAGS']:
                self.assertNotEqual(key, store.request_key(meta, {**env,name:'different'}, Path('/job1')))
            changed = {**meta, 'source':{'content_sha256':'changed', 'commit':'a'}}
            self.assertNotEqual(key, store.request_key(changed, env, Path('/job1')))
            changed = {**meta, 'argv':['cargo','test','-p','two']}
            self.assertNotEqual(key, store.request_key(changed, env, Path('/job1')))
            self.assertIsNone(store.request_key(meta, {}, Path('/job1')))
            self.assertIsNone(store.request_key({**meta,'repo':'unknown'}, env, Path('/job1')))

    def test_ordinary_directory_cannot_claim_private_mount(self):
        self.assertFalse(store.private_target({'job_prefix':str(self.base/'job')}, self.target))

    def test_test_execution_is_after_publication_and_never_replayed(self):
        meta = self.base / 'meta.json'
        meta.write_text(json.dumps({'repo':'sdk','source':{'content_sha256':'tree'}}))
        env = {'BUILD_SLOT_RECEIPT_META':str(meta), 'BB_JOB_HOME':str(self.base/'home')}
        real_run = subprocess.run
        def compile_command(args, **kw):
            if args[0] == 'cp':
                return real_run(args, **kw)
            self.assertIn('--no-run', args)
            self.assertGreaterEqual(len(kw['pass_fds']),2)
            (self.target / 'test-binary').write_text('compiled')
            return type('Result', (), {'returncode':0})()
        executions = []
        def execute_command(args, **kw):
            self.assertTrue((self.root/'entries/one/complete.json').exists())
            self.assertNotIn('--no-run', args)
            executions.append(args)
            return 17  # A cached build must not turn failing tests into PASS.
        local_leases = cargo_cache.local_target_leases
        with patch.dict(os.environ, env, clear=True), \
             patch.object(store, 'ROOT', self.root), \
             patch.object(store, 'private_target', return_value=True), \
             patch.object(store, 'request_key', return_value='one'), \
             patch.object(cargo_cache, 'target_path', return_value=self.target), \
             patch.object(cargo_cache, 'local_target_leases', side_effect=lambda job, target: local_leases(job, target, self.target)), \
             patch.object(cargo_cache.subprocess, 'run', side_effect=compile_command), \
             patch.object(cargo_cache.subprocess, 'call', side_effect=execute_command):
            self.assertEqual(17, cargo_cache.run(['test','--locked','--','--test-threads=2']))
            store.remove_tree(self.target); self.target.mkdir()
            self.assertEqual(17, cargo_cache.run(['test','--locked','--','--test-threads=2']))
        self.assertEqual(2, len(executions))
        rows = [json.loads(x) for x in Path(str(meta)+'.target-store.jsonl').read_text().splitlines()]
        self.assertEqual(['miss','hit'], [x['status'] for x in rows])

    def test_cargo_compile_selection(self):
        self.assertEqual((['test','--locked','--no-run'],True),
                         cargo_cache.compilation(['test','--locked','--','--ignored']))
        for verb in ['build','check','clippy']:
            self.assertEqual(([verb,'--locked'],False), cargo_cache.compilation([verb,'--locked']))
        self.assertEqual((None,False), cargo_cache.compilation(['metadata','--locked']))
        self.assertEqual((None,False), cargo_cache.compilation(['test','--doc']))
        self.assertEqual((['test','--no-run'],False), cargo_cache.compilation(['test','--no-run']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
