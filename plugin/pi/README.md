# Pi native SDK shim

`index.ts` targets `@earendil-works/pi-coding-agent` **1.1.0**. It registers
workspace coordination tools and forwards lifecycle observations to the
already-running GraphCoder workspace owner. Pi's native `createAgentSession`,
`SessionManager` and `DefaultResourceLoader` retain agent execution, context,
models, tools and extension loading.

Install with `acyclic install pi`. Start GraphCoder outside the host sandbox
as described in [the plugin README](../README.md). Child sessions receive
their shared harness view under the checkout's `.volumes/agents/` as cwd.
`spawn_agent`, `wait_agent`, `cancel_agent` and `reopen_agent` manage native
SDK handles; `acyclic fs merge agents/<ref>` performs the shared merge.

The shim stores no separate durable task index, execution journal, provider
or filesystem tool implementation. The shared harness owns resource
allocation, task files, capture and history. Cancellation first aborts the
native Pi session, waits for it, then requests shared task cancellation.

Reopen currently requires a retained native SDK session handle. Recovering
that handle after a process restart and the full recursive host acceptance
matrix remain qualification work. Installation alone is not proof of parity.
