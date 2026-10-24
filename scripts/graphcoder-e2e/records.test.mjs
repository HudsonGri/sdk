import assert from "node:assert/strict";
import test from "node:test";
import { recordedRequests, requests, requestStep } from "./assertions.mjs";

test("only flushed acknowledged records survive interruption, with all case assertions retained", () => {
  const record = { sequence: 1, case: "a", step: 0, path: "responses", status: 200, body: {} };
  const event = { event: "request", sequence: 1, case: "a", step: 0 };
  for (const tail of ["", "{", JSON.stringify({ ...record, sequence: 2 })]) {
    const parsed = recordedRequests(JSON.stringify(record) + "\n" + tail, [event]);
    assert.deepEqual(parsed, [record]);
    requests({ assert: { requests: 1 }, model: { steps: [{}] } }, parsed);
    assert.throws(() => requests({ assert: { requests: 2 }, model: { steps: [{}, {}] } }, parsed), /generation count/);
  }
});

test("acknowledged corruption and missing records always fail", () => {
  const event = { event: "request", sequence: 1, case: "a", step: 0 };
  assert.throws(() => recordedRequests("{", [event]), /complete JSONL/);
  assert.throws(() => recordedRequests("{\n", [event]), SyntaxError);
  assert.throws(() => recordedRequests('{"sequence":2}\n', [event]), /sequence/);
});


test("consumed steps reject route drift immediately for terminal and kill gates", () => {
  for (const path of ["messages", "responses", "chat/completions", "models/tiny:streamGenerateContent"]) {
    const gate = { path, step: 1 };
    assert.equal(requestStep([{ path: "count", step: null }, { path, step: 0 }], gate), false);
    assert.equal(requestStep([{ path, step: 1 }], gate), true);
    assert.throws(() => requestStep([{ path: "wrong-provider", step: 1 }], gate), /provider route for script step 1/);
  }
});
