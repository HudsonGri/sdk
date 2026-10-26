"""Real Git/Cargo metadata controls; no compiler and no mocked tool output."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('affected',HERE/'gate-affected.py')
affected=importlib.util.module_from_spec(spec);spec.loader.exec_module(affected)


class AffectedTests(unittest.TestCase):
    def test_literal_module_edges_and_actual_old_sibling_negative(self):
        with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as temporary:
            root=Path(temporary)
            def write(path,text):
                file=root/path;file.parent.mkdir(parents=True,exist_ok=True);file.write_text(text)
            def git(*args):
                return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.PIPE).decode().strip()
            def commit():
                git('add','-A');git('-c','user.name=fixture','-c','user.email=fixture@example.invalid','-c','commit.gpgsign=false','commit','-qm','fixture')
                return git('rev-parse','HEAD')
            git('init','-q')
            write('Cargo.toml','[workspace]\nresolver="2"\nmembers=["core","consumer","fuzz","unrelated"]\n')
            for name in ['core','consumer','fuzz','unrelated']:
                write(name+'/Cargo.toml','[package]\nname="fixture-'+name+'"\nversion="0.0.0"\nedition="2024"\n'+
                      ('[dependencies]\nfixture-core={path="../core"}\n' if name=='consumer' else ''))
                write(name+'/src/lib.rs','')
            # This source relationship reproduces actual SDK harness-core
            # build.rs:4 #[path="src/native_environment.rs"]. Cargo supplies
            # all package/target records from these actual manifests.
            write('core/build.rs','#[path="src/native_environment.rs"]\npub mod native_environment;\nfn main() {}\n')
            write('core/src/native_environment.rs','pub fn variable() {}\n')
            write('core/src/content_verifier.rs','pub fn verify() {}\n')
            write('core/src/lib.rs','mod content_verifier;\nmod nested;\n')
            write('core/src/nested.rs','pub mod child;\n')
            write('core/src/nested/child.rs','pub fn child() {}\n')
            write('fuzz/src/lib.rs','#[path="../../core/src/nested.rs"]\nmod shared;\n')
            write('unrelated/src/lib.rs','// #[path="../../core/src/native_environment.rs"] mod absent;\nconst TEXT:&str=r#"mod content_verifier;"#;\n')
            subprocess.run(['cargo','generate-lockfile','--offline'],cwd=root,check=True,capture_output=True)
            base=commit()
            old={ '__file__':str(HERE/'fixtures/gate-affected-before.py.txt') }
            exec(compile((HERE/'fixtures/gate-affected-before.py.txt').read_text(),'actual-installed-selector','exec'),old)
            for file,mode,packages in [
                ('core/src/content_verifier.rs','affected',['fixture-consumer','fixture-core']),
                ('core/src/native_environment.rs','full',['fixture-consumer','fixture-core','fixture-fuzz','fixture-unrelated']),
                ('core/src/nested/child.rs','affected',['fixture-consumer','fixture-core','fixture-fuzz']),
                ('Cargo.lock','full',['fixture-consumer','fixture-core','fixture-fuzz','fixture-unrelated']),
            ]:
                with self.subTest(file=file):
                    git('reset','--hard',base)
                    target=root/file
                    target.write_text((target.read_text() if target.exists() else '')+('\n# changed\n' if file=='Cargo.lock' else '\n// changed\n'))
                    commit()
                    result=affected.plan(root,base)
                    self.assertEqual(result['mode'],mode,result)
                    self.assertEqual(result['tests'],packages,result)
                    if file.endswith('content_verifier.rs'):
                        self.assertEqual(old['plan'](root,base)['mode'],'full')

    def test_inline_modules_raw_strings_comments_and_manifest_includes(self):
        source=Path('/fixture/src/lib.rs')
        text='''/* mod bogus; /* #[path="also-bogus.rs"] */ */
mod outer { #[path = r#"real.rs"#] pub mod child; }
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared.rs"));
include_str!("data.txt");
const S:&str="mod misleading;";
'''
        edges=affected.rust_references(source,text,{source})
        self.assertEqual(edges,[(Path('/fixture/src/outer/real.rs'),'source'),
                                ('/shared.rs','manifest-source'),(Path('/fixture/src/data.txt'),'data')])


if __name__=='__main__':unittest.main(verbosity=2)
