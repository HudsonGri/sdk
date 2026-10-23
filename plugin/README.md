# Native host workspace coordination

Claude Code, Codex and Pi run their own sessions, providers, tools, permissions
and agent loops. Acyclic coordinates isolated workspaces, capture and merges
through an already-running GraphCoder harness.

Start GraphCoder outside the host sandbox, from the workspace:

```sh
graphcoder --host-workspace --host-state-directory /path/to/explicit/state
acyclic install codex
acyclic host codex -- --sandbox workspace-write --ask-for-approval never
```

Codex requires **0.164.0 or later**; install and doctor reject older or unknown
versions. Pi targets **1.1.0**. Use `--plugin-root PATH` for package assets and
`--settings PATH` for a Claude settings file or Pi extension file.

The plugin's `spawn` tool starts a separate native host process in the admitted
lazy view under `<workspace>/.volumes/agents/`: `codex exec`, `claude -p`, or
`pi --mode json -p`. Native instruction/config discovery and tools run there.
Native built-in subagents keep their host behavior and **are not harness forks**.

The root launcher forwards the same explicit native policy arguments to managed
children. Spawn cannot override them. Codex requires explicit `--sandbox` and
`--ask-for-approval`; Claude requires explicit `--settings` and
`--permission-mode`. For Codex and Claude, the launcher records its already
writable workspace with native `--add-dir` on both parent and child. Changing
the child's cwd therefore retains the parent's workspace boundary, including
the coordination mailbox; it admits no path outside the parent's workspace.
Launch Claude with its native plugin directory enabled:

```sh
acyclic host claude-code -- --settings /path/to/policy.json --permission-mode default --plugin-dir /path/to/plugin/claude
acyclic host pi --
```

Direct native sessions continue to work normally. Managed spawn requires the
launcher context; host metadata does not provide the effective security policy,
so the plugin does not guess it. Native configuration directories remain owned
by the native host. The plugin never reads credentials or native transcripts.
Pi's native CLI receives its saved native session file when resuming.

The coordination tools are `spawn`, `send`, `wait`, `cancel`, `archive` and
`resume`. Each takes a parent-local `reference`; spawn/send/resume also take a
`prompt`. They use the same child request, inbox, control, status, delivered
and result files as GraphCoder. Wait completes after the accepted inputs settle.
Cancel stops and reaps the native child process tree before capturing partial
writes. Resume re-launches the saved native session in the recreated view.
Archive freezes the shared child reference permanently.

Capture happens at native turn/session boundaries and child process exit.
Failed or cancelled native tools leave their filesystem state capturable;
there is no per-tool pending-write ledger or pre/post hook pairing.

```sh
acyclic agents
acyclic fs log
acyclic --task TASK_ID fs merge agents/CHILD_REF
```

All sandboxed coordination and custody writes stay within the existing workspace:

| Path | Use |
| --- | --- |
| `.acyclic-sdk/host-files/owner.json` | Discover the running GraphCoder owner |
| `.acyclic-sdk/host-files/requests/` | Publish coordination requests |
| `.acyclic-sdk/host-files/results/` | Read and retire disposable results |
| `.acyclic-sdk/host-processes/` | Native process custody and exclusive ownership |
| `.volumes/agents/` | Admitted native child working views |

Install declares workspace access without adding sandbox roots. Commands never
start a service or write state-directory identities or lifecycle locks. Keep the
host sandbox and permissions enabled. GraphCoder must already be running.

The separate [identity package](identity-mcp.md) retains its command surface.
The plugin e2e lane owns sandbox-ON cross-OS real-host acceptance, recorded in
`Q:/lanes/plugin-e2e/`. Source checks and no-model probes alone do not establish
native model-session acceptance.
