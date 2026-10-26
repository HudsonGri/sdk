import { beforeAll, expect, test } from "bun:test";
import fc from "fast-check";
import initialize from "../generated/wasm/acyclic_harness_wasm.js";
import "fake-indexeddb/auto";
import { FileClient, ClientQuery, TaskVolume, WasmStream, initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

test("nested reference owner credentials are rejected before persistence", async () => {
  const f = await setup();
  const file = { volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "files", class: "project", owner: { kind: "project", id: "project" } }, path: "prompt.txt", version: "v1", descriptor: { sha256: Array(32).fill(1), byte_length: 1, media_type: "text/plain" }, display_name: "prompt" };
  const command = JSON.parse(new TextDecoder().decode(encode.encode("pinned")));
  const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  await f.client.enqueue(request("pinned.json"), bytes({ ...command, payload: { content: file } }));
  const before = await f.outbox.head();
  for (const container of ["content", "artifacts", "references"]) {
    for (const key of ["token", "Authorization", "credential", "bearer", "unexpected"]) {
      const reference = structuredClone(file);
      Object.assign(reference.volume.owner, { [key]: "secret" });
      const payload = { [container]: container === "content" ? reference : [reference] };
      await expect(f.client.enqueue(request("owner-token.json"), bytes({ ...command, payload }))).rejects.toMatchObject({ kind: "Harness" });
      expect(f.client.pending()).toBe(1);
      expect(await f.outbox.head()).toEqual(before);
    }
  }
  expect(await f.client.flush()).toBe(1);
  f.free();
});
const encode = { encode: (operationId: string) => new TextEncoder().encode(JSON.stringify({ operationId, authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} })) };
const limits = { commands: 8n, bytes: 4096n, durability: "Accepted" } as const;
const queryLimits = { files: 32n, segments: 64n, bytes: 8192n };
const request = (id: string) => ({ Task: { task: "task", file: { Request: id } } }) as const;

async function setup(durable = false) {
  const name = `file-client-${crypto.randomUUID()}`;
  const stream = durable ? await WasmStream.openBrowser(name, 64, 65536n) : new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const policy = { ...limits, durability: durable ? "Durable" : "Accepted" } as const;
  const client = await FileClient.open(outbox, target, policy);
  return { name, stream, outbox, target, client, policy,
    free() { client.free(); outbox.free(); target.free(); stream.free(); } };
}

test("the exported client is the Rust file client and restores enqueue order", async () => {
  const f = await setup();
  await f.client.enqueue(request("z.json"), encode.encode("first"));
  await f.client.enqueue(request("a.json"), encode.encode("second"));
  expect(f.client.pending()).toBe(2);
  const query = await ClientQuery.open(f.target, "Tasks", null, queryLimits);
  const stop = Promise.withResolvers<void>();
  const seen: string[] = [];
  const follow = query.follow(delivery => { if (delivery.file) seen.push(JSON.parse(new TextDecoder().decode(delivery.file.bytes)).operationId); }, () => {
    if (seen.length === 2) stop.resolve();
  }, stop.promise);
  f.client.free();
  const restored = await FileClient.open(f.outbox, f.target, f.policy);
  expect(restored.pending()).toBe(2);
  expect(await restored.flush()).toBe(2);
  await follow;
  expect(seen).toEqual(["first", "second"]);
  expect(await restored.flush()).toBe(0);
  query.free(); restored.free(); f.outbox.free(); f.target.free(); f.stream.free();
});

test("duplicate requests keep identity and conflicting metadata cannot replace them", async () => {
  const f = await setup();
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  expect(f.client.pending()).toBe(1);
  await expect(f.client.enqueue(request("one.json"), encode.encode("different"))).rejects.toBeDefined();
  expect(f.client.pending()).toBe(1);
  expect(await f.client.flush()).toBe(1);
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  expect(await f.client.flush()).toBe(0);
  f.free();
});

for (const [commands, kind] of [[1n, "CommandQuota"], [2n, "ByteQuota"]] as const) {
test(`${kind} rejects before storing another file and allows acknowledged retries`, async () => {
  const f = await setup();
  f.client.free();
  const bytes = BigInt(encode.encode("one").byteLength);
  const limit = kind === "CommandQuota" ? commands : bytes;
  const client = await FileClient.open(f.outbox, f.target, { ...f.policy, commands, bytes });
  await client.enqueue(request("one.json"), encode.encode("one"));
  const before = await f.outbox.head();
  await expect(client.enqueue(request("two.json"), encode.encode("two"))).rejects.toEqual({ kind, detail: { limit } });
  await expect(client.enqueue("Goal", encode.encode("goal"))).rejects.toMatchObject({ kind: "Harness" });
  expect(await f.outbox.head()).toEqual(before);
  expect(client.pending()).toBe(1);
  expect(await client.flush()).toBe(1);
  await client.enqueue(request("two.json"), encode.encode("two"));
  await client.enqueue(request("one.json"), encode.encode("one"));
  expect(client.pending()).toBe(1);
  client.free();
  const reopened = await FileClient.open(f.outbox, f.target, { ...f.policy, commands, bytes });
  await reopened.enqueue(request("one.json"), encode.encode("one"));
  expect(reopened.pending()).toBe(1);
  expect(await reopened.flush()).toBe(1);
  reopened.free(); f.outbox.free(); f.target.free(); f.stream.free();
});
}

test("a stale client cannot move the persisted cursor backwards", async () => {
  const f = await setup();
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  await f.client.enqueue(request("two.json"), encode.encode("two"));
  const stale = await FileClient.open(f.outbox, f.target, f.policy);
  expect(await f.client.flush()).toBe(2);
  const cursor = f.client.cursor();
  await expect(stale.flush()).rejects.toBeDefined();
  stale.free();
  const reopened = await FileClient.open(f.outbox, f.target, f.policy);
  expect(reopened.cursor()).toEqual(cursor);
  expect(reopened.pending()).toBe(0);
  reopened.free(); f.free();
});

test("a terminal rejection retires its entry and allows the next request after reopen", async () => {
  const f = await setup();
  await f.target.write(await f.target.head(), request("rejected.json"), encode.encode("different"), null);
  await f.client.enqueue(request("rejected.json"), encode.encode("rejected"));
  await f.client.enqueue(request("valid.json"), encode.encode("valid"));
  await expect(f.client.flush()).rejects.toMatchObject({ kind: "Rejected", detail: { request: request("rejected.json") } });
  expect(f.client.pending()).toBe(1);
  f.client.free();
  const reopened = await FileClient.open(f.outbox, f.target, f.policy);
  expect(await reopened.flush()).toBe(1);
  reopened.free();
  const restored = await FileClient.open(f.outbox, f.target, f.policy);
  expect(restored.pending()).toBe(0);
  restored.free(); f.outbox.free(); f.target.free(); f.stream.free();
});

for (const mutation of [
  { kind: "interaction.resolve.approval" }, { offlineSafe: false }, { bearer: "credential" },
  { payload: { content: "inline prompt" } }, { payload: { attachments: new Uint8Array([1, 2]) } },
  { payload: { metadata: { authorization: 1 } } }, { payload: { metadata: { hint: "text" } } },
  { authority: { kind: "task", id: "../escape" } },
  { payload: { references: [{ path: "unpinned" }] } },
]) {
  test(`unsafe request is rejected before any file publication: ${JSON.stringify(mutation)}`, async () => {
    const f = await setup();
    const before = await f.outbox.head();
    const value = JSON.parse(new TextDecoder().decode(encode.encode("unsafe")));
    await expect(f.client.enqueue(request("unsafe.json"), new TextEncoder().encode(JSON.stringify({ ...value, ...mutation })))).rejects.toMatchObject({ kind: "Harness" });
    expect(await f.outbox.head()).toEqual(before);
    expect(f.client.pending()).toBe(0);
    f.free();
  });
}

test("persistent provider files recover pending intent after all handles close", async () => {
  const f = await setup(true);
  await f.client.enqueue(request("offline.json"), encode.encode("queued"));
  const { name, policy } = f;
  f.free();
  const stream = await WasmStream.openBrowser(name, 64, 65536n);
  const outbox = await TaskVolume.open(stream, "client/files");
  const target = await TaskVolume.open(stream, "agent/files");
  const client = await FileClient.open(outbox, target, policy);
  expect(client.pending()).toBe(1);
  const readerStream = await WasmStream.openBrowser(name, 64, 65536n);
  const readerVolume = await TaskVolume.open(readerStream, "agent/files");
  const query = await ClientQuery.open(readerVolume, "Tasks", null, queryLimits);
  const stop = Promise.withResolvers<void>();
  const followed = query.follow(() => {}, () => stop.resolve(), stop.promise);
  expect(await client.flush()).toBe(1);
  await followed;
  const views = query.views();
  expect(JSON.parse(new TextDecoder().decode(views.segments(request("offline.json"))![0])).operationId).toBe("queued");
  views.free(); query.free(); readerVolume.free(); readerStream.free();
  client.free(); outbox.free(); target.free(); stream.free();
});

// FileClient-specific admission/publication oracles remain with its recovery suite.
test("a registered idle wait leaves the only command slot available for flush", async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const payload = new TextEncoder().encode(JSON.stringify({ operationId: "one", authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} }));
  const client = await FileClient.open(outbox, target, { commands: 1n, bytes: BigInt(payload.length), durability: "Accepted" });
  try {
    await client.enqueue({ Task: { task: "task", file: { Request: "one.json" } } }, payload);
    let settled = false;
    const idle = client.whenIdle().then(() => { settled = true; });
    // spawn_local polls the wait on the next microtask. This queued barrier lets
    // registration run before flush without a timer or polling for readiness.
    await new Promise<void>(resolve => queueMicrotask(resolve));
    expect(settled).toBe(false);
    expect(client.pending()).toBe(1);
    expect(await client.flush()).toBe(1);
    await idle;
    expect(settled).toBe(true);
    expect(client.pending()).toBe(0);
  } finally {
    client.free(); outbox.free(); target.free(); stream.free();
  }
});

for (const quota of ["CommandQuota", "ByteQuota"] as const) {
test(`waiting ${quota} is reserved before futures retain caller buffers`, async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const payload = new TextEncoder().encode(JSON.stringify({ operationId: "one", authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} }));
  const limits = { commands: quota === "CommandQuota" ? 1n : 4n, bytes: BigInt(payload.length) * (quota === "ByteQuota" ? 1n : 4n), durability: "Accepted" } as const;
  const client = await FileClient.open(outbox, target, limits);
  const key = { Task: { task: "task", file: { Request: "one.json" } } } as const;
  // These calls happen in the same JS turn, before the worker can consume either.
  const first = client.enqueue(key, payload);
  const second = client.enqueue(key, payload);
  await expect(second).rejects.toMatchObject({ kind: quota });
  await first;
  // Successful completion releases the waiting reservation, even with a full outbox.
  await client.enqueue(key, payload);
  expect(client.pending()).toBe(1);
  expect(await client.flush()).toBe(1);
  client.free(); outbox.free(); target.free(); stream.free();
});
}

function bounded(before: number, after: number) {
  if (after !== before) throw new Error("oversize admission grew WASM memory");
}

test("oversize enqueue checks its JS buffer length without growing WASM memory", async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const client = await FileClient.open(outbox, target, { commands: 1n, bytes: 3n, durability: "Accepted" });
  const { memory } = await initialize();
  const payload = new Uint8Array(Math.max(16 * 1024 * 1024, memory.buffer.byteLength + 65536));
  const key = { Task: { task: "task", file: { Request: "large.json" } } } as const;
  const before = memory.buffer.byteLength;
  for (let retry = 0; retry < 8; retry++) {
    await expect(client.enqueue(key, payload)).rejects.toEqual({ kind: "ByteQuota", detail: { limit: 3n } });
    bounded(before, memory.buffer.byteLength);
  }
  expect(client.pending()).toBe(0);
  // The former Vec<u8> boundary still exists on an unbounded volume write.
  // The same checker must detect its eager copy, independently of its result.
  const controlBefore = memory.buffer.byteLength;
  await target.write(await target.head(), "Goal", payload, null).catch(() => {});
  expect(() => bounded(controlBefore, memory.buffer.byteLength)).toThrow("oversize admission grew WASM memory");
  client.free(); outbox.free(); target.free(); stream.free();
});

// A rendering-capable local provider must not imply network recovery policy.
async function explicitFlushOracle(priorities: number[], automatic: boolean) {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const payloads = priorities.map((priority, index) => new TextEncoder().encode(JSON.stringify({ operationId: `request-${index}`, authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: { metadata: { priority } } })));
  const client = await FileClient.open(outbox, target, { commands: BigInt(priorities.length), bytes: BigInt(payloads.reduce((sum, bytes) => sum + bytes.length, 0)), durability: "Accepted" });
  try {
    const before = await target.head();
    for (const [index, payload] of payloads.entries()) {
      await client.enqueue({ Task: { task: "task", file: { Request: `${index}.json` } } }, payload);
    }
    // Mutate the policy by publishing without an explicit caller flush. The
    // same oracle must catch the resulting lost pending state and target write.
    if (automatic) await client.flush();
    const retained = client.pending() === priorities.length && (await target.head()).position === before.position;
    await client.flush();
    await client.whenIdle();
    if (!retained) throw new Error("local publication preceded explicit flush");
    expect(client.pending()).toBe(0);
    expect((await target.head()).position).toBe(before.position + BigInt(priorities.length));
  } finally {
    client.close(); client.free(); outbox.free(); target.free(); stream.free();
  }
}
const localRequests = fc.array(fc.integer({ min: 0, max: 65535 }), { minLength: 1, maxLength: 4 });
test("local rendering sessions preserve explicit publication across shrinking request histories", async () => {
  await fc.assert(fc.asyncProperty(localRequests, priorities => explicitFlushOracle(priorities, false)), { numRuns: 16, includeErrorInReport: true });
});
test("negative control: automatic local publication fails the same shrinking oracle", async () => {
  const result = await fc.check(fc.asyncProperty(localRequests, priorities => explicitFlushOracle(priorities, true)), { numRuns: 16 });
  expect(result.failed).toBe(true);
  expect(result.errorInstance).toBeInstanceOf(Error);
  expect(String(result.errorInstance)).toContain("local publication preceded explicit flush");
  expect(result.counterexample).toEqual([[0]]);
});
