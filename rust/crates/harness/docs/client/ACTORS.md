# Actor observations in TypeScript

`AgentClient` is the WASM export of the shared Rust client. Supply the runtime's
existing authenticated `WasmStream`, an agent path, exact conversation/status
cursors and its registered credential slot. Creating this client does not start
an agent runtime or another transport.

`next()` returns the generated Rust `AgentEvent` union. Narrow `kind` before
reading a conversation or status. `agentRecord(agent, index)` returns that same
union or `null` for a missing retained record. `agentStatus(agent)` returns the
canonical `StatusRecord` or `null` before delivery; a selected agent's absence is
never represented by an invented idle phase. An unselected agent is a typed
`not_requested` refusal.

`Phase`, `TurnInput`, `ConversationEntry`, `Sender` and terminal `AgentOutcome`
come from their production Rust serde definitions. A waiting approval includes
the actual request id in `phase.waiting_approval.id`. These observations confer
no authority to decide it. The owning host authenticates the human/parent and
uses its canonical decision and inbox publication path.

Conversation sequence, message inbox sequence, status cursor and alarm deadline
are `bigint`. `AgentFileRef` retains the ordinary generated file-reference shape
with a `bigint` descriptor byte length, matching this exact-integer serializer.
The older admitted-file projection's Number descriptor must not be assumed here.

Call `setView` with one focused agent followed by visible agents and matching
`BigUint64Array` cursor arrays. At most eight agents are admitted. Overlapping
agents keep their folds and cursors; deselection releases their records/follows.
Read one record or bounded window metadata for the visible frame. Output bodies
remain immutable file references and are fetched through the host's authorized
reader only when needed.

Input staging, approval request reads and decisions belong to the runtime host.
The browser host migration supplies those bindings; this observation adapter
does not implement another outbox, submission transport or approval policy.
