# Shared auth and protected secrets

`acyclic-auth` owns the capability contract, signature verification and authority
intersection. Enable `client` for the native HTTP secrets adapter. The browser
adapter and CLI call the same `secrets::Client`; transports do not implement
permission matching, CAS handling, response interpretation or retries.

Secrets management uses only these POST routes:

- `/v1/identity/secrets/put`: `{scope,name,kind,value,expectedGeneration}`
- `/v1/identity/secrets/list`: `{scope}`
- `/v1/identity/secrets/delete`: `{scope,name,expectedGeneration}`

Put and delete return `{generation,name,version,kind}`. Delete's kind is
`deleted`. List returns `{generation,variables}`: plain rows have
`{kind:"plain",name,version,value}`, protected rows have
`{kind:"protected",name,version}`. A protected value in any reply is rejected.
There is no get, reveal or export route. Strict requests reject account selectors;
the authenticated issuer chain supplies the owner.

Grant the independent `secrets` category with an exact `operation`, exact `scope`
and optional `name`. An absent/null name covers all names in that scope. Listing
a scope requires a scope grant without a name. Existing resource, execution,
billing and socket permissions authorize no secrets operation. Ancestor
restrictions, attenuation and owner policy still intersect in the existing
verified authority API.

Native callers construct `secrets::Client` with `HttpTransport::new(origin)` and
an existing `credential::CredentialProvider`. Credentials resolve on every call.
Only HTTPS origins and loopback HTTP origins are admitted. Redirects are
refused. A generation conflict requires a new list and an explicitly chosen new
mutation; the client never retries an overwrite automatically.

`@acyclic-labs/auth` exports `SecretsClient` over this Rust implementation. Its
request/reply API uses generated protobuf shapes; generation/version fields are
`bigint`. The HTTP JSON contract is generated from the same Rust declarations.
Native semantic and wire Debug output and every client error are redacted.
Plain list values are deliberate results, so applications must not log results
indiscriminately.

The portable `secrets::conformance::verify` accepts two clients and a fresh,
disposable scope. The first client needs all three secrets permissions; the
second must have product permissions and no secrets permissions. It checks CAS,
plain/protected visibility, independent denial of every operation and deletion.
`conformance/secrets-v1.json` publishes the normative case inventory and vectors;
these are contract examples, not captured external-provider fixtures.
