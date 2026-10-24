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
