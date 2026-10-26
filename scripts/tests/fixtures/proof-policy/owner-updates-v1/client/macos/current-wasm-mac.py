from pathlib import Path
import hashlib, json, os, platform, subprocess, sys, time

assert platform.system() == 'Darwin'
root = Path(sys.argv[1]).resolve()
assert root.parent == Path('/Users/var') and root.name.startswith('client-perf-node-cef1e4151943.')
files = {
    'typescript/packages/harness/test/node-swarm.mjs': '4a161e50d15f057b097327f3906c756e8c315162eb6c7c0523ca3648d535fc19',
    'typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm': '61f893d420abd5b3a27cec5249a827c1cff42e75e575c40e46045a0eeb6d85f4',
}
for name, digest in files.items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest() == digest, name
environment = {name: value for name, value in os.environ.items()
               if not any(part in name.upper() for part in ['API_KEY', 'AUTH_TOKEN', 'ACCESS_TOKEN', 'SECRET'])}
environment.update(HOME=str(root/'home'), CODEX_HOME=str(root/'home/codex'),
                   XDG_DATA_HOME=str(root/'home/data'), XDG_CONFIG_HOME=str(root/'home/config'),
                   XDG_CACHE_HOME=str(root/'home/cache'), XDG_STATE_HOME=str(root/'home/state'))
command = ['/opt/homebrew/bin/node', str(root/'typescript/packages/harness/test/node-swarm.mjs')]
started = time.monotonic()
result = subprocess.run(command, cwd=root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
seconds = time.monotonic()-started
with (root/'node.log').open('x') as output:
    output.write(result.stdout)
receipt = {'platform': platform.platform(), 'command': command, 'rc': result.returncode,
           'seconds': seconds, 'artifacts': files, 'output': result.stdout}
with (root/'receipt.json').open('x') as output:
    json.dump(receipt, output, indent=2)
print(json.dumps(receipt), flush=True)
assert result.returncode == 0 and seconds < 10, receipt
node = json.loads(result.stdout)
assert node['cases'] == 1 and node['seconds'] < 10, node
print('CLIENT_CURRENT_MAC_NODE_WASM_ADMITTED', flush=True)
