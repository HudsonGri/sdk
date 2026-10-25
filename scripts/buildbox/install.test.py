import ast
import concurrent.futures
import importlib.util
import inspect
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('installer',HERE/'bb-install.py')
installer=importlib.util.module_from_spec(spec);spec.loader.exec_module(installer)


def isolated_python(*args):
    return [sys.executable,'-I','-B',*map(str,args)]


def without_qualification(suite):
    """Remove one actual qualification command, independent of source formatting."""
    tree=ast.parse(inspect.getsource(installer.qualify))
    commands=[node for node in ast.walk(tree) if isinstance(node,ast.Assign)
              and any(isinstance(target,ast.Name) and target.id=='commands' for target in node.targets)]
    if len(commands)!=1 or not isinstance(commands[0].value,ast.List):
        raise AssertionError('qualification command list must have one owner')
    entries=commands[0].value.elts
    removed=[entry for entry in entries if isinstance(entry,ast.List)
             and any(isinstance(value,ast.Constant) and value.value==suite for value in entry.elts)]
    if len(removed)!=1:
        raise AssertionError('negative control must remove exactly one qualification')
    commands[0].value.elts=[entry for entry in entries if entry is not removed[0]]
    namespace=dict(installer.__dict__)
    exec(compile(ast.fix_missing_locations(tree),'<missing-qualification-control>','exec'),namespace)
    return namespace['qualify']


class InstallTests(unittest.TestCase):
    def test_privileged_import_keeps_source_immutable_without_inherited_environment(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            source=Path(temporary)/'module.py';source.write_text('VALUE=1\n')
            driver='import importlib.util,sys;s=importlib.util.spec_from_file_location("fixture",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);assert m.VALUE==1'
            env={k:v for k,v in os.environ.items() if k!='PYTHONDONTWRITEBYTECODE'}
            cache=source.parent/'__pycache__'
            try:
                subprocess.run(['sudo','-n',*isolated_python('-c',driver,source)],env=env,check=True)
                self.assertFalse(cache.exists())
                # The former sudo entry lost the env flag and wrote a root
                # directory into the upload, preventing unprivileged pruning.
                old=isolated_python('-c',driver,source);old.remove('-B')
                subprocess.run(['sudo','-n',*old],env=env,check=True)
                self.assertTrue(cache.exists());self.assertEqual(cache.stat().st_uid,0)
            finally:
                if cache.exists():
                    subprocess.run(['sudo','-n',*isolated_python('-c','import shutil,sys;shutil.rmtree(sys.argv[1])',cache)],check=True)

    def test_published_worker_imports_only_the_declared_tool_generation(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary)
            for name in installer.TOOLS:shutil.copyfile(HERE/name,root/name)
            command=[sys.executable,'-I','-B','-c',
                     'import runpy,sys;sys.path.insert(0,sys.argv[1]);'
                     'runpy.run_path(sys.argv[1]+"/bb-job.py",run_name="installed_worker")',str(root)]
            good=subprocess.run(command,cwd=root,capture_output=True)
            self.assertEqual(good.returncode,0,good.stderr.decode())
            # The former publication list omitted a runtime import even though
            # qualification had copied every source file into its stage.
            (root/'job_custody.py').unlink()
            bad=subprocess.run(command,cwd=root,capture_output=True)
            self.assertNotEqual(bad.returncode,0)
            self.assertIn(b"No module named 'job_custody'",bad.stderr)

    def test_fifty_concurrent_exec_jobs_see_complete_generations(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            def content(value):return f'#!/bin/sh\nprintf "{value}\\n"\n'.encode()
            installer.publish(tool,content('old'))
            barrier=threading.Barrier(51)
            def execute(_):
                barrier.wait()
                for _ in range(8):
                    run=subprocess.run([str(tool)],capture_output=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertIn(run.stdout,[b'old\n',b'new\n'])
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as workers:
                futures=[workers.submit(execute,i) for i in range(50)]
                # Publication is a separate process, as in production. A fork
                # from a writer thread can inherit another thread's open write
                # fd before CLOEXEC and manufacture ETXTBSY unrelated to rename.
                writer=subprocess.Popen(isolated_python('-c',
                    'import importlib.util,sys;from pathlib import Path;'
                    's=importlib.util.spec_from_file_location("i",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
                    '[m.publish(Path(sys.argv[2]),bytes.fromhex(sys.argv[3+n%2])) for n in range(80)]',
                    str(HERE/'bb-install.py'),str(tool),content('old').hex(),content('new').hex()))
                barrier.wait()
                for future in futures:future.result()
                self.assertEqual(writer.wait(),0)
            # Deterministic old in-place publication window: exec sees an empty
            # file between truncate and write, even though the path exists.
            tool.chmod(0o755)
            with tool.open('wb'):
                with self.assertRaises(OSError):subprocess.run([str(tool)],check=True)

    def test_command_custody_failure_is_a_publication_barrier(self):
        def run(command, **kwargs):
            if 'command_custody.test.py' in command:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0)
        with patch.object(installer.subprocess, 'run', side_effect=run):
            with self.assertRaises(subprocess.CalledProcessError):
                installer.qualify(Path('/unused-frozen-candidate'), 'var')
            # Negative control executes the former qualification policy: the
            # same failed command-custody suite cannot block publication.
            without_qualification('command_custody.test.py')(Path('/unused-frozen-candidate'), 'var')

    def test_failed_qualification_never_publishes(self):
        self.assertIn('sccache-rustc',installer.TOOLS)
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool';installer.publish(tool,b'original')
            with patch.object(installer.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['test'])):
                with self.assertRaises(subprocess.CalledProcessError):installer.qualify(Path(temporary),'var')
            self.assertEqual(tool.read_bytes(),b'original')
            self.assertEqual(list(Path(temporary).iterdir()),[tool])

    def test_cache_and_retirement_controls_are_mandatory_before_publication(self):
        for suite in ['prune.test.py','cargo_cache.test.py','receipt-outcome.test.mjs','prune-admission.test.mjs','reaping.test.py']:
            with self.subTest(suite=suite):
                calls=[]
                def run(command,**kwargs):
                    calls.append(command)
                    if command[-1]==suite:raise subprocess.CalledProcessError(1,command)
                with patch.object(installer.subprocess,'run',side_effect=run):
                    with self.assertRaises(subprocess.CalledProcessError):installer.qualify(Path('/fixture'),'var')
                self.assertEqual(calls[-1][-1],suite)
                # The former omission admitted an otherwise failing suite.
                with patch.object(installer.subprocess,'run',side_effect=run):without_qualification(suite)(Path('/fixture'),'var')

    def test_selected_publication_preserves_peers_and_fences_stale_predecessors(self):
        driver='''import importlib.util,sys,tempfile,shutil,json,os
from pathlib import Path
source=Path(sys.argv[1]);s=importlib.util.spec_from_file_location('installer',source);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
stage=Path(tempfile.mkdtemp(prefix='bb-install-control-',dir='/mnt/bb/tmp'))
live=stage/'live';live.mkdir()
try:
 for name in m.TOOLS:
  m.publish(stage/name,(source.parent/name).read_bytes(),0o444)
  m.publish(live/name,('old-'+name).encode())
 hashes={name:m.file_hash(stage/name) for name in m.TOOLS}
 previous={name:m.file_hash(live/name) for name in m.TOOLS}
 m.publish(stage/'.qualified.json',json.dumps(hashes).encode(),0o400)
 m.publish(stage/'.predecessors.json',json.dumps(previous).encode(),0o400)
 m.BIN=live;m.secure_bin=lambda:live
 peer=live/'bb-job.py';m.publish(peer,b'newer peer runtime')
 for selection in [[],['unknown'],['bb-install.py','bb-install.py']]:
  try:m.publish_candidate(stage,selection)
  except ValueError:pass
  else:raise AssertionError('invalid selection admitted')
 m.publish_candidate(stage,['bb_target_store.py','bb-install.py'])
 assert peer.read_bytes()==b'newer peer runtime'
 manifest=json.loads((live/'installed-tools.json').read_text())
 assert manifest['sha256']['bb-job.py']==m.file_hash(peer)
 assert m.file_hash(live/'bb_target_store.py')==hashes['bb_target_store.py']
 m.publish(live/'bb_target_store.py',b'newer selected tool')
 before={name:m.file_hash(live/name) for name in m.TOOLS}
 try:m.publish_candidate(stage,['bb_target_store.py'])
 except ValueError as error:assert 'predecessor changed' in str(error)
 else:raise AssertionError('stale candidate published')
 assert before=={name:m.file_hash(live/name) for name in m.TOOLS}
 # Actual unfenced former shape overwrites the newer selected tool.
 old={'__file__':str(source),'__name__':'old_installer'}
 code=source.read_text().replace('if any(file_hash(BIN/name) != previous.get(name) for name in selected):','if False:')
 exec(compile(code,'unfenced-installer','exec'),old)
 old['BIN']=live;old['secure_bin']=lambda:live
 old['publish_candidate'](stage,['bb_target_store.py'])
 assert before['bb_target_store.py']!=m.file_hash(live/'bb_target_store.py')
finally:shutil.rmtree(stage)
'''
        subprocess.run(['sudo','-n',*isolated_python('-c',driver,HERE/'bb-install.py')],check=True)

    def test_proxy_storage_failure_blocks_tool_qualification(self):
        def rejected(command, **kwargs):
            if 'cargo_cache.test.py' in command:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0)
        with patch.object(installer.subprocess, 'run', side_effect=rejected):
            with self.assertRaises(subprocess.CalledProcessError):
                installer.qualify(HERE, 'var')
            # The old publication boundary accepts this failing proxy suite.
            without_qualification('cargo_cache.test.py')(HERE, 'var')

    def test_privileged_publication_refuses_direct_lane_edits(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            script='from pathlib import Path;import sys,os;p=Path(sys.argv[1]);p.write_bytes(b"qualified");os.chown(p,0,0);p.chmod(0o555)'
            subprocess.run(['sudo','-n',*isolated_python('-c',script,str(tool))],check=True)
            with self.assertRaises(PermissionError):tool.write_bytes(b'partial')
            self.assertEqual(tool.read_bytes(),b'qualified')
            # The former var-owned mode admits exactly the same write.
            unprotected=Path(temporary)/'unprotected';unprotected.write_bytes(b'old');unprotected.write_bytes(b'partial')
            self.assertEqual(unprotected.read_bytes(),b'partial')

    def test_writable_home_cannot_replace_pinned_tool_directory(self):
        import uuid
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);alias=root/'bin';alias.mkdir();(alias/'tool').write_text('original')
            store=Path('/opt')/('gate-speed-install-fixture-'+uuid.uuid4().hex)
            driver='''import importlib.util,sys,os,subprocess
from pathlib import Path
s=importlib.util.spec_from_file_location('installer',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
alias=Path(sys.argv[2]);store=Path(sys.argv[3]);os.chown(alias,0,0);m.secure_bin(alias,store,False)
m.secure_bin(alias,store,False) # repeated publication preserves the pinned entry
assert os.statvfs(alias).f_flag & os.ST_RDONLY
m.publish(store/'tool',b'updated');assert (alias/'tool').read_bytes()==b'updated'
probe="from pathlib import Path;import sys,errno;p=Path(sys.argv[1]);\\ntry:p.rename(p.with_name('replaced'))\\nexcept OSError as e:assert e.errno==errno.EBUSY\\nelse:raise AssertionError('tool entry replaced')"
subprocess.run(['runuser','-u','var','--','python3','-B','-c',probe,str(alias)],check=True)
assert (alias/'tool').read_text()=='updated'
'''
            try:
                subprocess.run(['sudo','-n','unshare','--mount','--propagation','private',*isolated_python('-c',driver,str(HERE/'bb-install.py'),str(alias),str(store))],check=True)
                # The old unmounted root-owned entry is still movable from a
                # writable parent: ownership of files alone is insufficient.
                alias.rename(root/'old-replaced');self.assertTrue((root/'old-replaced/tool').exists())
            finally:
                subprocess.run(['sudo','-n',*isolated_python('-c','import shutil,sys;shutil.rmtree(sys.argv[1],ignore_errors=True)',str(store))],check=True)
                subprocess.run(['sudo','-n',*isolated_python('-c','import os,sys;from pathlib import Path;[(os.chown(p,int(sys.argv[2]),int(sys.argv[3]))) for p in Path(sys.argv[1]).iterdir() if p.is_dir()]',str(root),str(os.getuid()),str(os.getgid()))],check=True)


if __name__=='__main__':unittest.main()
