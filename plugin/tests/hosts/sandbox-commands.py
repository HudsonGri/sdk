#!/usr/bin/env python3
"""Native-host command contract, using only fixture profiles and a local model.

Run on the host OS with prebuilt binaries; never starts a model or reads a login.
Codex uses its own sandbox CLI. Claude uses its real Bash tool and sandbox with
a scripted HTTP response. Pi uses its real RPC Bash tool; Pi has no native
sandbox, so an externally supplied Pi sandbox must be qualified separately.
"""
import argparse
import http.server
import json
import os
from pathlib import Path
import selectors
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time


def checked(argv, *, cwd, env, payload=None):
    result = subprocess.run(argv, cwd=cwd, env=env, input=payload, text=True,
                            capture_output=True, timeout=8)
    assert result.returncode == 0, (argv, result.stdout[-2000:], result.stderr[-2000:])
    return result.stdout


def inside(config):
    """Every operation here runs inside the host's native command boundary."""
    cfg = json.loads(Path(config).read_text())
    workspace, view = Path(cfg['workspace']), Path(cfg['view'])
    binary, parent, child = cfg['binary'], cfg['parent'], cfg['child']
    assert Path.cwd() == workspace, 'native tool lost the parent workspace cwd'
    assert view.is_relative_to(workspace / '.volumes/agents')
    assert (view / 'parent.txt').read_text() == 'parent'
    (view / 'child.txt').write_text('sandbox-child')
    blocked = False
    try:
        Path(cfg['outside']).write_text('must be refused by the host')
    except OSError:
        blocked = True
    assert blocked == (cfg['host'] != 'pi'), 'host sandbox boundary changed'

    def cli(args, cwd=workspace, payload=None):
        env = dict(os.environ)
        env.pop('ACYCLIC_TASK', None)
        return checked([binary, *args], cwd=cwd, env=env, payload=payload)

    # Both explicit identities and cwd discovery go through the same client.
    cli(['agents'], cwd=view)
    hook = json.loads(cli(['__hook', cfg['host'], 'Stop'], cwd=view,
                          payload=json.dumps({'cwd': str(view), 'task': child})))
    assert hook.get('continue') is not False, hook
    cli(['--task', child, 'capture'], cwd=view)
    cli(['--task', parent, 'fs', 'merge', 'agents/sandbox-child'])
    assert (workspace / 'child.txt').read_text() == 'sandbox-child'
    cli(['--task', parent, 'agents'])
    cli(['--task', parent, 'log'])
    cli(['--task', parent, 'discard', 'agents/sandbox-child'])
    assert (workspace / 'child.txt').read_text() == 'sandbox-child'
    doctor = json.loads(cli(['doctor', cfg['host']]))
    assert doctor['additional_sandbox_roots'] == []

    # A legacy external data directory cannot masquerade as an admitted mailbox.
    negative = Path(cfg['negative'])
    for args in [['agents'], ['log'], ['discard', 'agents/sandbox-child'],
                 ['fs', 'merge', 'agents/sandbox-child'], ['capture']]:
        result = subprocess.run([binary, '--task', parent, *args], cwd=negative,
                                text=True, capture_output=True, timeout=2)
        assert result.returncode != 0, ('external data endpoint admitted', args)
    rejected = json.loads(cli(['__hook', cfg['host'], 'Stop'], cwd=negative,
                              payload=json.dumps({'cwd': str(negative), 'task': parent})))
    assert rejected.get('continue') is False, rejected
    external = Path(cfg['external_data'])
    assert not list((external / 'requests').iterdir()), 'negative published a request outside'
    assert not list((external / 'results').iterdir()), 'negative published a result outside'
    result = dict(host=cfg['host'], commands=True, captured=True, merged=True,
                  discarded=True, hook_command=True, external_data_rejected=True,
                  outside_write_blocked=blocked, model_calls=0)
    (workspace / 'sandbox-result.json').write_text(json.dumps(result))


def scripted_model(command):
    class Handler(http.server.BaseHTTPRequestHandler):
        """One real Bash tool call; no network model and no inherited authentication."""

        def log_message(self, *_args):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if self.path.endswith('/count_tokens'):
                body = b'{"input_tokens":1}'
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            assert '/messages' in self.path, self.path
            done = any(block.get('type') == 'tool_result' and block.get('tool_use_id') == 'sandbox-probe'
                       for message in request.get('messages', [])
                       for block in (message.get('content') if isinstance(message.get('content'), list) else []))
            block = {'type': 'text', 'text': ''} if done else {
                'type': 'tool_use', 'id': 'sandbox-probe', 'name': 'Bash', 'input': {}}
            delta = {'type': 'text_delta', 'text': 'done'} if done else {
                'type': 'input_json_delta', 'partial_json': json.dumps({'command': command,
                                                                      'description': 'Sandbox command contract'})}
            events = [
                ('message_start', {'message': {'id': 'fixture-message', 'type': 'message', 'role': 'assistant',
                                              'model': 'claude-haiku-5-5', 'content': [], 'stop_reason': None,
                                              'usage': {'input_tokens': 1, 'output_tokens': 0}}}),
                ('content_block_start', {'index': 0, 'content_block': block}),
                ('content_block_delta', {'index': 0, 'delta': delta}),
                ('content_block_stop', {'index': 0}),
                ('message_delta', {'delta': {'stop_reason': 'end_turn' if done else 'tool_use', 'stop_sequence': None},
                                   'usage': {'output_tokens': 1}}),
                ('message_stop', {}),
            ]
            body = ''.join('event: ' + name + '\ndata: ' + json.dumps({'type': name, **fields}) + '\n\n'
                           for name, fields in events).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    return Handler


def pi_command(binary, command, *, cwd, env, log):
    native = subprocess.Popen([binary, '--mode', 'rpc', '--no-session', '--no-skills',
                               '--no-prompt-templates', '--no-themes'], cwd=cwd, env=env,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                              text=True, bufsize=1)
    try:
        native.stdin.write(json.dumps({'id': 'sandbox', 'type': 'bash', 'command': command}) + '\n')
        native.stdin.flush()
        with selectors.DefaultSelector() as ready:
            ready.register(native.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 8
            while True:
                remaining = deadline - time.monotonic()
                assert remaining > 0 and ready.select(remaining), 'Pi command did not complete'
                line = native.stdout.readline()
                assert line, 'Pi exited without command result'
                message = json.loads(line)
                if message.get('id') == 'sandbox':
                    assert message.get('success'), message
                    assert message['data']['exitCode'] == 0, message
                    break
        native.stdin.close()
        assert native.wait(timeout=2) == 0
    finally:
        if native.poll() is None:
            native.kill()
            native.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inside')
    parser.add_argument('--host', choices=['codex', 'claude-code', 'pi'])
    parser.add_argument('--binary')
    parser.add_argument('--graphcoder')
    parser.add_argument('--native')
    parser.add_argument('--package')
    parser.add_argument('--scratch')
    args = parser.parse_args()
    if args.inside:
        inside(args.inside)
        return
    assert all([args.host, args.binary, args.graphcoder, args.native, args.package, args.scratch])
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='plugin-sandbox-', dir=args.scratch) as fixture:
        root = Path(fixture)
        workspace, state = root / 'workspace', root / 'trusted-state'
        workspace.mkdir()
        state.mkdir()
        (workspace / 'parent.txt').write_text('parent')
        # Whitelist environment: never inspect/copy the user's login or keys.
        env = {'PATH': str(Path(args.native).parent) + ':' + os.environ['PATH'], 'LANG': 'C.UTF-8'}
        for name in ['HOME', 'USERPROFILE', 'CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'XDG_STATE_HOME',
                     'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'PLUGIN_DATA', 'CLAUDE_PLUGIN_DATA', 'PI_CODING_AGENT_DIR']:
            path = root / name.lower()
            path.mkdir()
            env[name] = str(path)
        with (root / 'owner.log').open('w+') as owner_log, (root / 'native.log').open('w+') as native_log:
            owner_env = dict(env)
            for name in ['HOME', 'USERPROFILE', 'XDG_STATE_HOME', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME']:
                path = root / 'owner-profile' / name.lower()
                path.mkdir(parents=True)
                owner_env[name] = str(path)
            owner = subprocess.Popen([args.graphcoder, '--host-workspace', '--host-state-directory', str(state),
                                      '--workspace', str(workspace)], cwd=workspace, env=owner_env,
                                     stdout=owner_log, stderr=owner_log)
            try:
                descriptor = workspace / '.acyclic-sdk/host-files/owner.json'
                deadline = time.monotonic() + 2
                while not descriptor.exists():
                    assert owner.poll() is None, 'workspace owner exited'
                    assert time.monotonic() < deadline, 'workspace owner not ready'
                    time.sleep(0.01)

                def action(value):
                    return json.loads(checked([args.binary, '__workspace'], cwd=workspace, env=env,
                                              payload=json.dumps(value)))

                parent = action({'kind': 'open', 'host': args.host, 'session': 'sandbox-commands'})
                child = action({'kind': 'fork', 'parent': parent['task'], 'reference': 'sandbox-child'})
                view = Path(child['cwd'])
                env['ACYCLIC_TASK'] = parent['task']
                negative = workspace / '.acyclic-sdk/sandbox-negative'
                negative.mkdir()
                external = root / 'legacy-data'
                external.mkdir()
                (external / 'requests').mkdir()
                (external / 'results').mkdir()
                # Descriptor must name the negative workspace; only its location is wrong.
                endpoint = json.loads(descriptor.read_text())
                endpoint['workspace'] = str(negative)
                (external / 'owner.json').write_text(json.dumps(endpoint))
                (negative / '.acyclic-sdk').mkdir()
                (negative / '.acyclic-sdk/host-files').symlink_to(external, target_is_directory=True)
                # Count only request/result projections, not the descriptor itself.
                config = workspace / 'sandbox-config.json'
                config.write_text(json.dumps(dict(host=args.host, workspace=str(workspace), view=str(view),
                    binary=args.binary, parent=parent['task'], child=child['task'], negative=str(negative),
                    external_data=str(external), outside=str(state / 'forbidden'))))
                probe = workspace / 'sandbox-commands.py'
                shutil.copyfile(__file__, probe)
                command = shlex.join([sys.executable, str(probe), '--inside', str(config)])
                if args.host == 'codex':
                    checked([args.native, 'sandbox', '-c', 'sandbox_mode="workspace-write"', '-c',
                             'approval_policy="never"', '-c', 'sandbox_workspace_write.writable_roots=' +
                             json.dumps([str(workspace)]), '--', sys.executable, str(probe), '--inside', str(config)],
                            cwd=workspace, env=env)
                elif args.host == 'claude-code':
                    policy = root / 'policy.json'
                    policy.write_text(json.dumps({'sandbox': {'enabled': True, 'failIfUnavailable': True,
                        'autoAllowBashIfSandboxed': True, 'allowUnsandboxedCommands': False}}))
                    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), scripted_model(command))
                    worker = threading.Thread(target=server.serve_forever, daemon=True)
                    worker.start()
                    env.update(ANTHROPIC_BASE_URL=f'http://127.0.0.1:{server.server_port}',
                               ANTHROPIC_API_KEY='local-fixture-only', CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC='1',
                               DISABLE_AUTOUPDATER='1')
                    try:
                        result = subprocess.run([args.native, '-p', 'Run the sandbox contract.', '--model',
                            'claude-haiku-5-5', '--settings', str(policy), '--permission-mode', 'default',
                            '--add-dir', str(workspace), '--plugin-dir', str(Path(args.package) / 'claude'),
                            '--allowedTools', 'Bash', '--output-format', 'stream-json', '--verbose',
                            '--include-hook-events'],
                            cwd=workspace, env=env, text=True, stdout=native_log, stderr=native_log, timeout=8)
                        assert result.returncode == 0, (root / 'native.log').read_text()[-5000:]
                    finally:
                        server.shutdown()
                        server.server_close()
                        worker.join()
                    native_log.flush()
                    events = [json.loads(line) for line in (root / 'native.log').read_text().splitlines()
                              if line.startswith('{')]
                    hooks = {event.get('hook_event') for event in events
                             if event.get('subtype') == 'hook_response' and event.get('outcome') == 'success'}
                    assert {'SessionStart', 'Stop'} <= hooks, ('native capture hooks absent', hooks)
                    for event in events:
                        if event.get('subtype') == 'hook_response':
                            outcome = json.loads(event['stdout'])
                            assert outcome.get('continue') is not False, outcome
                else:
                    checked([args.binary, 'install', 'pi', '--plugin-root', args.package, '--settings',
                             str(Path(env['PI_CODING_AGENT_DIR']) / 'extensions/acyclic.ts')], cwd=workspace, env=env)
                    pi_command(args.native, command, cwd=workspace, env=env, log=native_log)
                    session = action({'kind': 'hook', 'host': 'pi', 'event': 'NativeSession',
                                      'input': {'task': parent['task']}})
                    assert session['id'], 'native Pi extension did not attach its session'
                result_path = workspace / 'sandbox-result.json'
                assert result_path.exists(), (root / 'native.log').read_text()[-5000:]
                result = json.loads(result_path.read_text())
                for name in ['XDG_STATE_HOME', 'XDG_DATA_HOME', 'PLUGIN_DATA', 'CLAUDE_PLUGIN_DATA']:
                    assert not list(Path(env[name]).iterdir()), f'client wrote external {name}'
                result['seconds'] = time.monotonic() - started
                assert result['seconds'] < 10, result
                print(json.dumps(result), flush=True)
            finally:
                owner.terminate()
                owner.wait(timeout=2)


if __name__ == '__main__':
    main()
