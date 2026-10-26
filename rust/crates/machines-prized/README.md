# acyclic-machines-prized

`MachinesProvider` over [Prized Sandboxes](https://prized.dev/docs/sandboxes): Firecracker
microVMs that boot from a template, pause with their memory, and fork into many live copies
in one call. It is one replaceable provider behind the `acyclic-machines`
contract: nothing Prized-specific crosses into the harness.

| Trait method            | Prized call (`https://api.prized.dev/api/v1`)                                  |
|-------------------------|--------------------------------------------------------------------------------|
| `qualify_image`         | digest → registered template; `GET /sandbox-templates` must list it           |
| `create`                | `POST /sandboxes?wait=running` with an `Idempotency-Key`                       |
| `fork_machine`          | `POST /sandboxes/{id}/fork` with `{count}`: one call makes every child         |
| `checkpoint`            | fork the machine once, then pause the copy                                     |
| `fork`                  | fork the checkpoint's paused copy with `{count}`; the copy stays paused        |
| `suspend` / `wake`      | `POST /sandboxes/{id}/pause` / `resume` (memory and processes are kept)        |
| `destroy_*`             | `DELETE /sandboxes/{id}`, then read until `404`                                |
| `exec`                  | `POST {toolboxBase}/process/execute`                                           |
| `events`                | poll `GET /sandboxes/{id}` and diff state                                      |
| `list_machines`         | `GET /sandboxes?label=acyclic.managed=true` (and the tenant's label)           |
| `usage`                 | allocation estimate, marked provisional                                        |
| `recover`               | registry, then `GET /sandboxes?label=acyclic.key=…` and Prized's idempotency   |

**Live fork is one call.** A Prized fork copies the running sandbox's memory, processes, and
disk into `count` children (up to 256) at the same instant, so `fork_machine` sends one
request whatever the count, with no fan-out tree and no child forked from another child.
Children come back in fork order and are `running` when the call returns. Copies start on the
parent's host, which holds at most 31 copies of a 1 vCPU sandbox; a fork that does not fit
makes nothing and fails with `ProviderError::Rejected` (`409 fork_capacity`), as does a fork
on a host being replaced (`409 fork_unsupported_host`). Children are independent once
running, so a source can be destroyed before its children.

**Checkpoints are paused copies.** `checkpoint` forks the machine into one copy and pauses it
at once: the copy keeps memory, processes and disk, bills storage only, and outlives its
source. The pause lands as the copy finishes booting, about a second past the fork, so the
checkpoint matches the source exactly only if its workload holds still that long. `fork`
forks the paused copy where it stands: Prized copies a paused sandbox at its pause point and
leaves it paused, so every fork of one checkpoint starts from the same state. A copy resumed
outside the provider no longer holds the checkpoint, and `fork` refuses it (`Conflict`).

**Limits the provider states rather than hides:**

- Prized deletes a sandbox when its run time ends: 24 hours by default, up to 7 days on a
  paid workspace. The clock stops while a sandbox is paused, and on a paid workspace each
  resume starts it again. `ExpirationPolicy::Never` means the workspace's default run time.
  A `MaxAge` of up to 7 days is sent as `maxRuntimeMin`, and every `wake` sets the end back
  to the machine's creation plus its age (`POST /sandboxes/{id}/timeout`). A machine within
  a minute of its age while suspended is not woken; it stays paused until destroyed. A
  resume made outside the provider (a request to the machine's endpoint that does not send
  `Prized-Auto-Resume: false`) gets Prized's full run time until the next `wake`, which pins
  it again. A longer `MaxAge`, `Idle`, and `AtUnixMs` are `Unsupported`.
- The free tier allows 60 minutes of running in all: a `MaxAge` over an hour is refused
  (`Rejected`), a resume carries on the time left, a fork's children keep their parent's
  deletion time, and a paused sandbox is deleted 30 days after its last use.
- `autoPauseMin` is fixed at creation, in whole minutes (the 15-second default pauses after
  one idle minute); `set_suspension_policy` is `Unsupported`.
- Prized has one network policy: outbound internet, nothing inbound, no traffic between
  sandboxes. Register the digest that stands for it with
  `PrizedConfig::register_network_policy`; any other digest is refused.
- `exec` returns output when the command ends, stderr merged into stdout, in 64 KiB frames.
  It never wakes a suspended machine (`Prized-Auto-Resume: false`). An answer lost on the
  way back, `502 sandbox_unreachable`, or `410 sandbox_deleted` (the sandbox is being
  deleted, perhaps under the running command) is `CommandIndeterminate`. `read_output` is
  `Unsupported`.
- Budgets are per workspace in Prized, so per-machine budgets must be zero.

A `MachineId` is the sandbox id packed into a version-8 UUID that spells `prized`:
`sbx_0123456789abcdef` is `7072697a-6564-8012-8345-006789abcdef`. The contract is stored in
`acyclic.*` labels, and every read ignores sandboxes of another tenant (`acyclic.tenant`).
The API key goes only to the API host and `term.prized.dev` (`PrizedConfig::edge_hosts`).
`PrizedConfig::labels` (env `PRIZED_LABELS`, `key=value,...`) adds up to 8 labels of your
own to every sandbox, for your filters and cleanup.

Live checks, ignored by default and gated on `PRIZED_API_KEY` (a key with Sandboxes: Write):

```sh
# Full Machines conformance suite (1 vCPU sandboxes, deleted afterwards):
PRIZED_API_KEY=... cargo test -p acyclic-machines-prized --test live -- --ignored conformance
# One-call fork of a running machine into 8 children, timed:
PRIZED_API_KEY=... cargo test -p acyclic-machines-prized --test live -- --ignored fan_out --nocapture
# A MaxAge machine keeps its end across a suspend and wake (one sandbox; a paid workspace):
PRIZED_API_KEY=... cargo test -p acyclic-machines-prized --test live -- --ignored max_age --nocapture
```
