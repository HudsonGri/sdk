import assert from "node:assert/strict";
import test from "node:test";
import { SwarmClient } from "@acyclic-labs/swarm";

const routes = [
  ["json", client => client.list()],
  ["events", async client => {
    const iterator = client.watch("fixture");
    try { return await iterator.next(); } finally { await iterator.return(); }
  }],
  ["tree", async client => new Uint8Array(await (await client.artifactTree("fixture")).arrayBuffer())],
];

for (const [name, invoke] of routes) {
  test(`shared HTTP policy owns ${name} credentials and redirects`, async () => {
    let calls = 0;
    const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: async (_url, init) => {
      calls++;
      assert.equal(init.credentials, "omit");
      assert.equal(init.redirect, "error");
      assert.ok(init.signal instanceof AbortSignal);
      return new Response(name === "events" ? 'data: {}\n\n' : "{}", { headers: { "content-type": name === "events" ? "text/event-stream" : "application/json" } });
    } });
    const result = await invoke(client);
    assert.ok(result !== undefined);
    assert.equal(calls, 1);
  });
}

for (const name of ["json", "events", "tree"]) {
  test(`shared HTTP cancels a late ${name} body after abort`, async () => {
    let enter;
    const entered = new Promise(resolve => { enter = resolve; });
    let finish;
    let cancelled = 0;
    const controller = new AbortController();
    const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: () => {
      enter();
      return new Promise(resolve => { finish = resolve; });
    } });
    const options = { signal: controller.signal };
    const iterator = name === "events" ? client.watch("fixture", options) : undefined;
    const pending = name === "json" ? client.list(options) : iterator ? iterator.next() : client.artifactTree("fixture", options);
    const rejected = assert.rejects(pending);
    await Promise.race([entered, pending]);
    controller.abort();
    finish(new Response(new ReadableStream({
      start(stream) { stream.enqueue(new TextEncoder().encode(name === "events" ? 'data: {}\n\n' : "{}")); },
      cancel() { cancelled++; },
    }), { headers: { "content-type": name === "events" ? "text/event-stream" : "application/json" } }));
    await rejected;
    assert.equal(cancelled, 1);
    await iterator?.return();
  });
}

test("shared HTTP cancels a pending JSON read independently of fetch", async () => {
  let enter;
  const entered = new Promise(resolve => { enter = resolve; });
  let cancelled = 0;
  const controller = new AbortController();
  const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: async () => new Response(new ReadableStream({
    pull() { enter(); }, cancel() { cancelled++; },
  }, { highWaterMark: 0 })) });
  const pending = client.list({ signal: controller.signal });
  const rejected = assert.rejects(pending);
  await Promise.race([entered, pending]);
  controller.abort();
  await rejected;
  assert.equal(cancelled, 1);
});

test("shared HTTP keeps absent bodies distinct from empty subscription bodies", async () => {
  const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: async () =>
    new Response(null, { headers: { "content-type": "text/event-stream" } }) });
  await assert.rejects(client.watch("fixture").next(), /has no body/);
  const response = await client.artifactTree("fixture");
  assert.equal(response.body, null);
});

test("shared HTTP cancels an artifact read with the original abort reason", async () => {
  let enter;
  const entered = new Promise(resolve => { enter = resolve; });
  let cancelled = 0;
  const controller = new AbortController();
  const reason = new Error("fixture cancellation");
  const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: async () =>
    new Response(new ReadableStream({ pull() { enter(); }, cancel() { cancelled++; } }, { highWaterMark: 0 })) });
  const response = await client.artifactTree("fixture", { signal: controller.signal });
  const pending = response.arrayBuffer();
  const rejected = assert.rejects(pending, error => error === reason);
  await Promise.race([entered, pending]);
  controller.abort(reason);
  await rejected;
  assert.equal(cancelled, 1);
});

test("shared HTTP retires a subscription with an invalid content type", async () => {
  let cancelled = 0;
  const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: async () => new Response(new ReadableStream({
    cancel() { cancelled++; },
  }), { headers: { "content-type": "application/json" } }) });
  await assert.rejects(client.watch("fixture").next(), TypeError);
  assert.equal(cancelled, 1);
});

test("shared HTTP uploads immutable Blobs without reading or copying them", async () => {
  const blob = new Blob([new Uint8Array(1024 * 1024)]);
  Object.defineProperty(blob, "arrayBuffer", { value() { throw new Error("Blob materialization"); } });
  let calls = 0;
  const client = new SwarmClient({ endpoint: "https://fixture", token: "fixture", fetch: async (_url, init) => {
    calls++;
    assert.equal(init.body, blob);
    assert.equal(init.headers.get("idempotency-key"), "fixture");
    assert.equal(init.credentials, "omit");
    return new Response('{}');
  } });
  await client.upload(blob, { idempotencyKey: "fixture" });
  assert.equal(calls, 1);
});
