"""Codex: retain original fix evidence without rerunning unchanged experiments."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CX = Path('C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def verify(cycle, machinery_test_rerun=False):
    controls = {
        'rules_reader': sha(ROOT/'read_rules.py') == '16cf62d06ddad69f508b612152bdddfb3affc961630d949efa5ddcc227ea0ad6',
        'verifier': sha(ROOT/'verify_cycle.py') == 'd12fd914d7d33f381b0dacc5502ef83b829eaab7354c00b12544ca3a63901ccb',
    }
    old = (ROOT/'terminal_audit-cycle-194-before.py').read_text()
    corrected = old.replace("('target','selected','seconds'", "('kind','target','selected','seconds'").replace("('label','name','stage'", "('kind','label','name','stage'")
    controls['terminal_metadata_correction'] = (ROOT/'terminal_audit.py').read_text() == corrected
    custody = Path('Q:/lanes/e2e-gate-qualification-20261025/clippy-custody')
    controls['gate_custody_archive'] = sha(custody/'clippy-custody-proof.tar.gz') == 'ad691d0da12652caf04c89f96c5bc9d75cce6d22e0710f1e11fc8f6932fdb82d'
    controls['gate_custody_patch'] = sha(custody/'gate-output-custody.patch') == '57e2e0e1ab7111a475228ed46b5a1e3f5a8af06ea22f8a57ae5a7df1a8d7d436'
    manifest = json.loads((ROOT/'machinery/manifest.json').read_text())
    staged = {name: sha(ROOT/'machinery'/name) == expected for name, expected in manifest['staged'].items()}
    staged['self_test'] = sha(ROOT/'machinery/self-test.py') == manifest['self_test_sha256']
    live = {name: (CX/name).is_file() and sha(CX/name) == expected for name, expected in manifest['staged'].items()}
    test = json.loads((ROOT/'machinery/self-test-result.json').read_text())
    retained = test['status'] == 'PASS' and test['seconds'] < 10
    passed = all(controls.values()) and all(staged.values()) and retained
    receipt = dict(author='Codex',cycle=cycle,checks=controls,status='PASS' if passed else 'FAIL',
                   staged_hash_checks=staged,live_matches_staged=live,retained_machinery_test=test,
                   machinery_test_rerun=machinery_test_rerun,bb2_jobs_started=0,
                   limits=('Machinery fixture qualification rerun this cycle; ' if machinery_test_rerun else 'Unchanged fixture qualification retained; ')+
                          'source hashes do not prove live dispatch or fleet ROI. Deployment requires atomic publication/restart and actual smoke. Efficiency is authorized directly. No bb2 experiments.')
    target = ROOT/f'prior-fix-verification-cycle-{cycle}.json'
    temporary = target.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    temporary.replace(target)
    return receipt

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle',type=int,required=True)
    parser.add_argument('--machinery-test-rerun',action='store_true')
    args = parser.parse_args()
    result = verify(args.cycle,args.machinery_test_rerun)
    print(json.dumps(result))
    raise SystemExit(0 if result['status']=='PASS' else 1)
