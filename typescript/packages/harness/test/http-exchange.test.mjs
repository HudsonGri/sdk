import assert from "node:assert/strict";
import test from "node:test";
import { browserHttpResponse, createBrowserHttpExchange, startBrowserHttpExchange } from "../dist/http.js";

test("shared browser exchange rejects lossy deadlines before host I/O", async () => {
  const request = { method: "GET", url: "https://fixture", headers: {}, body: new Uint8Array() };
  const initialize = await createBrowserHttpExchange(async () => new Response(null), request);
  await initialize.response;
  initialize.cancel();
  let calls = 0;
  for (const timeoutMs of [-1, 0.5, 1.5, 2 ** 32, NaN, Infinity, "10", true]) {
    assert.throws(() => startBrowserHttpExchange(async () => { calls++; return new Response(null); },
      { ...request, timeoutMs }));
  }
  assert.equal(calls, 0);
});

test("shared browser Response cancellation retires its original independent body", async () => {
  let cancelled = 0;
  const exchange = await createBrowserHttpExchange(async () => new Response(new ReadableStream({
    cancel() { cancelled++; },
  })), { method: "GET", url: "https://fixture", headers: {}, body: new Uint8Array() });
  const response = browserHttpResponse(exchange, await exchange.response);
  await response.body.cancel();
  assert.equal(cancelled, 1);
});

test("shared browser exchange retains immutable Blob bodies without materializing them", async () => {
  const blob = new Blob([new Uint8Array(1024 * 1024)]);
  Object.defineProperty(blob, "arrayBuffer", { value() { throw new Error("Blob materialization"); } });
  let calls = 0;
  const exchange = await createBrowserHttpExchange(async (_url, init) => {
    calls++;
    assert.equal(init.body, blob);
    assert.equal(init.body.size, 1024 * 1024);
    return new Response(null);
  }, { method: "POST", url: "https://fixture", headers: {}, body: blob });
  await exchange.response;
  exchange.cancel();
  assert.equal(calls, 1);
});

test("shared browser exchange snapshots mutable bytes before asynchronous fetch admission", async () => {
  const bytes = new Uint8Array([1, 2, 3]);
  const exchange = startBrowserHttpExchange(async (_url, init) => {
    assert.deepEqual(Array.from(init.body), [1, 2, 3]);
    return new Response(null);
  }, { method: "POST", url: "https://fixture", headers: {}, body: bytes });
  bytes.fill(9);
  await exchange.response;
  exchange.cancel();
});

test("shared browser exchange rejects unsupported body owners before host I/O", () => {
  let calls = 0;
  for (const body of [null, undefined, "bytes", {}, new ArrayBuffer(1)]) {
    assert.throws(() => startBrowserHttpExchange(async () => { calls++; return new Response(null); },
      { method: "POST", url: "https://fixture", headers: {}, body }));
  }
  assert.equal(calls, 0);
});
