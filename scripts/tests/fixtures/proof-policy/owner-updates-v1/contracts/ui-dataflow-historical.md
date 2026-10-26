# UI data-flow audit (2026-10-25), on main b7333953cb

The verdict: no client follows only the agents in view, and the wire lets a server push more than the client asked for.

1. **Swarm subscription: UNBOUNDED.** Nothing chooses streams by what's in view or focused.
   - `typescript/packages/harness/src/client.ts:268-290` sends a cursor for every known authority, and the `subscribe()` path has no cap (`:160` caps only projections, at 64).
   - `typescript/packages/swarm/src/index.ts:58` `watch(id)` opens one event stream for the whole swarm, and `detail().agents` returns every agent (`protocol.ts:91`).
2. **Wire: UNBOUNDED.**
   - `proto/harness/v2/harness.proto:182-185` (ResumeRequest) and `:63-68` (ObserveRequest) have no page, byte or credit fields.
   - `wire-transport.ts:437-443` AsyncQueue buffers without limit, and `:75` and `:358` apply no backpressure. `maximumMessageBytes` defaults to MAX_SAFE_INTEGER (`:64`, `:114`).
   - `client.ts:349-365` `#accept` takes deliveries for authorities the client never requested.
   - `limits-contract.ts:10-46` has effectively infinite constants.
3. **Pagination: PARTIAL.**
   - `index.ts:335-355` `conversation()` loads the whole history.
   - The page maximum is 2^32−1 (`conversation-page-contract.ts:4`).
   - The child page size has no cap (`agent-harness.ts:691`; `runtime/children.rs:34-36`).
   - In Swarm, list, detail, artifactFiles and trace are unpaged (`swarm/src/index.ts:50-55`), capped only by a 64 MB limit.
4. **PageWindow bug:** `pagination.ts:104-110` evicts items without moving that side's cursor, so the next load leaves a hole. `items` also copies the whole window on every read (`:20`).
5. **Not one core:**
   - GraphCoder uses its own terminal.rs wire loop and doesn't use `client::Client`.
   - The web path uses HarnessClient and wire-transport.
   - Swarm cloud uses a separate SwarmClient.
   - ClientViews (Rust/WASM) handles predictions only and doesn't decide what to load.
