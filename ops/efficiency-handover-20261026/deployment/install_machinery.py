"""Codex: hash-fenced atomic publication of the qualified SDK fleet bundle.

No lane processes, jobs or inboxes are terminated. Activation is a separate,
recorded supervisor-only restart. --self-test uses fixtures under D: only.
"""
import argparse,hashlib,json,os,tempfile,unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parent
LIVE=Path('C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx')
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None

def publish(source,destination,expected_source,expected_before):
    if sha(source)!=expected_source:raise ValueError('unqualified source')
    if sha(destination)!=expected_before:raise ValueError('destination changed; inspect concurrent owner')
    temporary=destination.with_name(destination.name+'.efficiency-new')
    try:
        with temporary.open('xb')as stream:
            stream.write(source.read_bytes());stream.flush();os.fsync(stream.fileno())
        if sha(temporary)!=expected_source or sha(destination)!=expected_before:
            raise ValueError('publication inputs changed')
        os.replace(temporary,destination)
    finally:
        temporary.unlink(missing_ok=True)

def restore(receipt,archive,live):
    for name,installed in reversed(list(receipt['installed'].items())):
        destination=live/name
        if sha(destination)!=installed:raise ValueError('concurrent owner update; preserve '+name)
        original=receipt['before'][name]
        if original is None:destination.unlink()
        else:publish(archive/name,destination,original,installed)
    receipt['rollback_verified']={name:sha(live/name)==expected for name,expected in receipt['before'].items()}
    if not all(receipt['rollback_verified'].values()):raise ValueError('rollback incomplete')

class AtomicControls(unittest.TestCase):
    def test_replace_keeps_prior_open_reader(self):
        with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as temp:
            root=Path(temp);old=root/'live';new=root/'new';old.write_bytes(b'old');new.write_bytes(b'qualified')
            # Old in-place publication destroys the prior generation's oracle.
            old.write_bytes(new.read_bytes());self.assertNotEqual(old.read_bytes(),b'old')
            old.write_bytes(b'old');before=sha(old)
            # Windows readers must share delete permission. Test identity using
            # a retained hard link: atomic replacement leaves that generation.
            retained=root/'retained';os.link(old,retained)
            publish(new,old,sha(new),before)
            self.assertEqual(retained.read_bytes(),b'old');self.assertEqual(old.read_bytes(),b'qualified')
    def test_stale_destination_refused(self):
        with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as temp:
            root=Path(temp);old=root/'live';new=root/'new';old.write_bytes(b'owner update');new.write_bytes(b'qualified')
            with self.assertRaises(ValueError):publish(new,old,sha(new),None)
            self.assertEqual(old.read_bytes(),b'owner update')
    def test_unqualified_source_refused(self):
        with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as temp:
            root=Path(temp);old=root/'live';new=root/'new';old.write_bytes(b'old');new.write_bytes(b'changed')
            with self.assertRaises(ValueError):publish(new,old,'wrong',sha(old))
            self.assertEqual(old.read_bytes(),b'old')
    def test_partial_bundle_failure_restores_only_our_generation(self):
        with tempfile.TemporaryDirectory(dir='D:/lane-scratch/efficiency')as temp:
            root=Path(temp);live=root/'live';archive=root/'archive';live.mkdir();archive.mkdir()
            (live/'helper').write_bytes(b'new');(archive/'helper').write_bytes(b'old')
            (live/'added').write_bytes(b'new helper')
            receipt=dict(before={'helper':sha(archive/'helper'),'added':None},installed={name:sha(live/name)for name in ['helper','added']})
            self.assertNotEqual(sha(live/'helper'),receipt['before']['helper']) # old partial install fails oracle
            restore(receipt,archive,live)
            self.assertEqual((live/'helper').read_bytes(),b'old');self.assertFalse((live/'added').exists())
            (live/'helper').write_bytes(b'other owner')
            with self.assertRaises(ValueError):restore(dict(before={'helper':sha(archive/'helper')},installed={'helper':'old generation'}),archive,live)
            self.assertEqual((live/'helper').read_bytes(),b'other owner')

def install(cycle):
    staged=ROOT/'machinery';manifest=json.loads((staged/'manifest.json').read_text())
    proof=json.loads((staged/'self-test-result.json').read_text())
    assert proof['status']=='PASS'and proof['tests']>=20 and proof['seconds']<10
    assert sha(staged/'self-test.py')==manifest['self_test_sha256']
    before={name:sha(LIVE/name)for name in manifest['install_order']}
    assert all(sha(staged/name)==manifest['staged'][name]for name in before)
    archive=ROOT/f'machinery/installed-cycle-{cycle}';archive.mkdir(exist_ok=False)
    for name in before:
        if before[name]is not None:(archive/name).write_bytes((LIVE/name).read_bytes())
    receipt=dict(author='Codex',cycle=cycle,before=before,installed={},self_test=proof,
                 status='PUBLISHING',activation='pending supervisor restart and heartbeat/dispatch proof')
    target=archive/'receipt.json'
    try:
        for name in manifest['install_order']:
            publish(staged/name,LIVE/name,manifest['staged'][name],before[name])
            receipt['installed'][name]=sha(LIVE/name)
        receipt['status']='PUBLISHED'
    except BaseException:
        restore(receipt,archive,LIVE)
        receipt['status']='ROLLED_BACK_PUBLICATION_REFUSED'
        raise
    finally:target.write_text(json.dumps(receipt,indent=2))
    return receipt

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--self-test',action='store_true');parser.add_argument('--apply',action='store_true');parser.add_argument('--cycle',type=int)
    args=parser.parse_args()
    if args.self_test:
        result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(AtomicControls));raise SystemExit(not result.wasSuccessful())
    if not args.apply or not args.cycle:parser.error('--apply --cycle required')
    print(json.dumps(install(args.cycle)))
