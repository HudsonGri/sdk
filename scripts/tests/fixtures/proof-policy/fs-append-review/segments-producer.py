from pathlib import Path
import ast, hashlib, json, os, re, subprocess, sys, time
from collections import Counter

records = Path('.bb-fs-segments-records')
records.mkdir()
mode = os.environ['FS_SEGMENTS_MODE']
assert mode in ('positive', 'negative', 'clippy')
files = ['rust/crates/filesystem/src/workspace/content.rs',
         'rust/crates/filesystem/src/tests/workspace/mod.rs',
         'rust/crates/filesystem/src/tests/workspace/append_segments.rs']
sha = lambda data: hashlib.sha256(data).hexdigest()
def refuse(kind, value, tb):
    print(f'REFUSED step=segments: {value}; recovery: pwsh -NoProfile -File .bb-fs-segments-dispatch.ps1 -RunId {time.time_ns()}', flush=True)
    sys.__excepthook__(kind, value, tb)
sys.excepthook = refuse
subprocess.run(['rustfmt', '--edition', '2024', '--config', 'skip_children=true', *files], check=True)
names = subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'], text=True).splitlines()
manifest = {n: sha(Path(n).read_bytes()) for n in sorted(set(names)) if Path(n).is_file()}
(records/'source.json').write_text(json.dumps(manifest, indent=2))
(records/'files.json').write_text(json.dumps(files, indent=2))
shared = ast.parse(Path('.bb-fs-witness-stage.py').read_text())
exec(compile(ast.Module(body=[n for n in shared.body if isinstance(n,ast.FunctionDef) and n.name in ('build','run')],type_ignores=[]),'<shared-runtime-functions>','exec'))
(records/'compiled-source.json').write_text(json.dumps(manifest, indent=2))
if mode == 'positive':
    executable = build('positive')
    suites = [('append-segments','workspace::tests::append_segments::')]
    namespace = {}
    exec(compile(ast.parse(Path('.bb-fs-custodian-test-plan.py').read_text()),'<shared-test-plan>','exec'), namespace)
    namespace['admit_test_plan'](executable, suites, records, run)
    assert all(run(executable,label,name) for label,name in suites), 'positive runtime checks failed; inspect named suite output'
    (records/'completed.json').write_text(json.dumps(dict(mode=mode,suites=suites,source_manifest_sha256=sha((records/'source.json').read_bytes())),indent=2))
    sys.exit(0)
if mode == 'negative':
    positive = dict(manifest)
    name = files[0]
    original = Path(name).read_bytes()
    before = b'let start = if append {'
    assert original.count(before) == 1
    mutant = original.replace(before, b'let start = if false {', 1)
    # This is the existing replacement behavior applied to append: an EOF of zero.
    # Keep the condition independent so a lost prefix cannot hide behind replacement.
    Path(name).write_bytes(mutant)
    try:
        compiled = dict(manifest); compiled[name] = sha(mutant)
        (records/'compiled-source.json').write_text(json.dumps(compiled, indent=2))
        (records/'mutations.json').write_text(json.dumps([dict(path=name,before=before.decode(),after='let start = if false {')], indent=2))
        (records/'mutant.rs').write_bytes(mutant)
        executable = build('negative')
        assert run(executable, 'negative-eof', 'append_segments_preserve_prefix_and_share_one_publication', 101)
        body = (records/'negative-eof.out').read_text()
        assert 'assertion' in body and 'offset: 0' in body, 'negative control failed for an unexpected reason'
    finally:
        Path(name).write_bytes(original)
        (records/'negative-compiled-source.json').write_text((records/'compiled-source.json').read_text())
        (records/'compiled-source.json').write_text(json.dumps(positive, indent=2))
    (records/'completed.json').write_text(json.dumps(dict(mode=mode,suites=[['negative-eof','append_segments_preserve_prefix_and_share_one_publication']],source_manifest_sha256=sha((records/'source.json').read_bytes())),indent=2))
    sys.exit(0)
for n, digest in manifest.items():
    assert sha(Path(n).read_bytes()) == digest, ('source changed during qualification', n)
command = ['cargo','clippy','--locked','--keep-going','-p','acyclic-fs','-p','acyclic-fs-fuzz','--all-targets','--all-features','--message-format=short','--','--cap-lints','warn']
started = time.monotonic()
with (records/'clippy.out').open('w') as output:
    result = subprocess.run(command,stdout=output,stderr=subprocess.STDOUT)
(records/'clippy.json').write_text(json.dumps(dict(command=command,exit=result.returncode,seconds=time.monotonic()-started), indent=2))
assert result.returncode == 0, 'clippy compilation failed; inspect clippy.out'
baseline = json.loads(Path('.bb-fs-table-ratchet.json').read_text())
findings = Counter(re.sub(r':\d+:\d+: (warning|error): ', ': ', line) for line in (records/'clippy.out').read_text().splitlines() if re.match(r'^[^ ]+\.rs:\d+:\d+: (warning|error): ', line))
comparison = dict(main=baseline['main'],candidate_findings=sum(findings.values()),new_findings=dict(findings-Counter(baseline['findings'])))
(records/'clippy-comparison.json').write_text(json.dumps(comparison,indent=2)); print(json.dumps(comparison),flush=True)
assert not comparison['new_findings'], 'new clippy findings; inspect clippy-comparison.json'
(records/'completed.json').write_text(json.dumps(dict(mode=mode,suites=[],source_manifest_sha256=sha((records/'source.json').read_bytes())),indent=2))
