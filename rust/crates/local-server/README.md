# SDK localhost Actors host

`acyclic-local-server` composes the SDK's canonical Streams unary and session
services, filesystem service and Actors service on one numeric loopback HTTP
endpoint. It is single-node and has no quorum or Cloud dependency. The public
fixture session authority needs no login, keychain, environment key or secret.
It is intended for localhost development and conformance, with local callers
treated as the host administrator.

Start the stock harness package with one command:

```sh
cargo run --locked -p acyclic-local-server --features native-harness --bin sdk-local -- \
  --workspace ./project --catalog ./models.json --model fixture/tiny --listen 127.0.0.1:7000
```

The catalog explicitly binds the model transport. Optional-auth loopback model
fixtures need no credentials. The host never discovers native credentials.
Its default account is `local` and namespace is `harness`; both can be selected
with explicit flags. The command prints the endpoint after deployment is ready.
Ctrl-C fences Actors admission and drains package-owned requests before
retiring native providers. State is held in a private temporary directory.
An embedded server's shutdown owner is installed before its handle is returned.
Dropping that handle or cancelling its shutdown waiter requests the same drain;
the drain retains effect transports and package owners until retirement.
Admitted RPCs own their work independently of a connected response observer.
Shutdown closes the catalog, wakes guest waiters, and drains every request
lease before recovering workflow effects and retiring the shared transports.

Connect the shipping client:

```sh
graphcoder --endpoint http://127.0.0.1:7000 -p 'hello'
```

`--input-format jsonl --operation UUID` uses the same request files, query fold,
completion decoder and exit policy as embedded GraphCoder. The deployed host
owns execution after the client disconnects. The client does not load a model,
workspace, credentials or another harness implementation. Interactive TUI
attachment is not yet exposed by this command.

Embedding applications register compiled Rust Actors packages and Workers by
digest. Canonical deployment selects an immutable artifact and a full account /
namespace binding; actor IDs are scoped within that binding. Native workspace
bindings belong to exactly one namespace. Worker deployment, invocation and
deletion use the same public SDK wire API. This host does not interpret JS or
download code. Package subscriptions and resources are fixed by the registered
artifact. Dynamic subscription edits, deleteAll, resource rebinding and code
migration return explicit unsupported errors rather than discarding journals
or reporting a nonexistent update. Deployment limits are validated metadata;
the compiled package's provider and admission policy enforce execution bounds.

Run the unchanged portable scenario corpus through socket deployment:

```sh
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- \
  --substrate localhost --report localhost.json
```

The same pinned scenario package runs on both substrates. Filesystem references
are published/read through its wire service; Actors invoke the checkpoint/outbox
contract over the canonical API. SDK providers remain the sole storage engines.
After effects drain, one shared gRPC retirement boundary terminates open follows
and sessions with Unavailable trailers, so shutdown never depends on idle clients
disconnecting.
