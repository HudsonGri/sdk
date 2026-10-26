# Provider exchange recordings

The native provider transport records exchanges only when `ACYCLIC_PROVIDER_RECORD`
names a directory. This applies to every native provider adapter and host. Embedders
can configure the same mechanism with `ReqwestTransport::with_recording(directory)`.
Recording does not choose or redirect endpoints; Codex login remains restricted to
its built-in endpoint. OAuth refresh requests are excluded.

For example, in PowerShell:

```powershell
$env:ACYCLIC_PROVIDER_RECORD = 'D:/lane-scratch/providers/captures'
graphcoder --model anthropic/claude-haiku-5-5 -p 'Reply with a short greeting.'
Remove-Item Env:ACYCLIC_PROVIDER_RECORD
```

Each exclusive `exchange-*` directory contains `request.body`, `response.body` and
`provenance.json`. Bodies are exact bytes. The response is appended as chunks arrive,
including original SSE framing. Provenance records the method, URL path, status,
content type, timestamp, adapter, catalog provider, selected model, executable SHA256,
compiled source commit, both body SHA256 digests and transport chunk lengths.
Headers contribute names only: no Authorization, cookie, ChatGPT-Account-Id, API key
or other header value is persisted. URL authority, user information, query and
fragment are excluded. Request and response body content is intentionally preserved.
Archive builders set `ACYCLIC_PROVIDER_SOURCE_COMMIT` to their exact source commit;
recording refuses a binary without source provenance.

The `end` field is absent while receiving. A settled exchange records `eof`,
`cancelled` or `interrupted`. A successful model decoder can cancel its HTTP body
after a protocol terminal, before network EOF. That recording contains the exact
consumed prefix, and remains replayable without waiting for additional network data.
Cancellation and interruption do not imply a successful model response: the normal
model decoder validates the recorded prefix on replay. No unreceived bytes are added.

Replay uses the existing shared `harness-mock-model` server. A script step selects
an admitted recording instead of generated text, tools or JSON:

```json
{"cases":{"capture":{"steps":[{"recording":"captures/exchange-example"}]}}}
```

Paths are relative to the script. Run the server with:

```sh
harness-mock-model --script replay.json --record requests.jsonl
```

Its ready event supplies the port; configure the test provider base URL as
`http://127.0.0.1:PORT/capture/v1`. The loader verifies settled provenance, body
hashes and chunk lengths before serving. Raw captures bypass template substitution
and SSE generation. Edited bodies, active captures, and steps combining raw and
generated responses are refused. An unadmitted recording reference cannot fall
back to an empty generated response.

Derived Responses replay uses `acyclic_harness_providers::decode_recording(adapter,
body, maximum, dispatch).await` before any selector substitution. It calls the same
SSE and Responses decoder as live dispatch, with strict semantic coverage enabled.
It returns the complete batch only after validating the full consumed recording.
Plain function calls and at most one plain assistant text message are supported;
terminal text is `MessageCompleted`, without duplicated incremental `Content`.
Namespaces, custom calls, reasoning, annotations/logprobs, unknown item semantics,
incomplete output, inconsistent terminal calls and trailing events are refused.
Unsupported adapters are refused. This conservative API does not certify arbitrary
provider semantics. Keep the verified raw bodies and provenance immutable; label
substituted replay as derived and encode it only through the shared mock wire path.
Raw replay does not use this projection policy and remains byte exact.
