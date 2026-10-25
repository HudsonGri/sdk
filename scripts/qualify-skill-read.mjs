#!/usr/bin/env node
// Shared mock replay owns framing, binding, finite steps and recording.
// Provenance remains pending fixtures/responses-skill-read; this is integration
// evidence until the coordinator supplies the actual provider capture.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { Processes, Events } from "./graphcoder-e2e/process.mjs";
import { recordedRequests } from "./graphcoder-e2e/assertions.mjs";
import { qualified } from "./graphcoder-e2e/qualification.mjs";
import { environment, scratchDirectory } from "./graphcoder-e2e/scratch.mjs";

const [binary, mock, mode = "prompt", selection = "valid"] = process.argv.slice(2);
const command = [process.execPath, ...process.argv.slice(1)];
const recovery = process.platform === "win32"
  ? "& " + command.map(value => "'" + value.replaceAll("'", "''") + "'").join(" ")
  : command.map(value => "'" + value.replaceAll("'", "'\"'\"'") + "'").join(" ");
const mutations = {
  valid: {}, escaped: {},
  ungranted: { "/root/volume/id": "00000000-0000-0000-0000-000000000000" },
  unretained: { "/path": "inputs/skills/unretained/SKILL.md" },
  "wrong-owner": { "/root/volume/owner/id": "00000000-0000-0000-0000-000000000000" },
  "wrong-provider": { "/root/volume/provider/namespace": "ungranted-provider" },
};
let step = "admit-inputs", root, output, passed = false, failure, localCounter;
const begin = performance.now(), processes = new Processes(), captures = [];
let records = [];
try {
  assert.ok(binary && isAbsolute(binary), "absolute shipping GraphCoder required");
  assert.ok(mock && isAbsolute(mock), "absolute source-admitted harness-mock-model required");
  assert.ok(["prompt", "jsonl"].includes(mode), "mode must be prompt or jsonl");
  assert.ok(Object.hasOwn(mutations, selection), "unknown selector mutation");
  const providerTests = process.env.E2E_PROVIDER_TEST_BINARY;
  assert.ok(providerTests && isAbsolute(providerTests), "E2E_PROVIDER_TEST_BINARY must name the admitted provider test executable");
  root = scratchDirectory("skill-read-");
  const workspace = join(root, "workspace"), settings = join(root, "settings");
  const skillDirectory = join(settings, ".agents/skills/private-skill");
  for (const path of [workspace, skillDirectory]) mkdirSync(path, { recursive: true });
  output = process.env.E2E_OUTPUT ? resolve(process.env.E2E_OUTPUT) : join(root, "proof");
  mkdirSync(output, { recursive: true });
  const header = "---\nname: private-skill\ndescription: LAZY_READ_DISCOVERY\n---\nPRIVATE_SKILL_BODY_λ\n";
  const body = selection === "escaped" ? header + '"'.repeat(65536 - Buffer.byteLength(header)) : header;
  writeFileSync(join(skillDirectory, "SKILL.md"), body);
  const env = environment(settings);
  for (const key of Object.keys(env)) if (/API_KEY$|AUTH_TOKEN$|^GRAPHCODER_MODEL$/.test(key)) delete env[key];
  // Never redirect the Codex-login endpoint. Exercise its actual local counter
  // and no-count-route assertion separately from the custom loopback endpoint.
  step = "assert-codex-local-counter";
  localCounter = await processes.run(providerTests,
    ["codex_dispatch_tests::stock::codex_stock_execution_counts_locally_without_a_backend_count_route", "--exact"],
    { env, cwd: workspace });
  qualified(localCounter, "libtest", 1);
  const authorized = ["valid", "escaped", "unretained"].includes(selection);
  const validRead = ["valid", "escaped"].includes(selection);
  const script = { cases: { skill: { steps: [
    { when: { path: "responses", user: "use the private skill" }, tools: [{ id: "read-private", name: "read",
      arguments: { source: { $json: { text: { $request: "/input/2/content/0/text" },
        pointer: "/skills/0/source", set: mutations[selection] } } } }] },
    ...(authorized ? [{ when: { path: "responses", tool_result: validRead ? "PRIVATE_SKILL_BODY" : "retained identity" },
      text: [validRead ? "SKILL_READ_DONE" : "SKILL_READ_REJECTED"] }] : []),
  ] } } };
  const scriptPath = join(output, "script.json"), recordPath = join(output, "requests.jsonl");
  writeFileSync(scriptPath, JSON.stringify(script));
  step = "start-shared-finite-replay";
  const server = processes.start(mock, ["--script", scriptPath, "--record", recordPath], { env });
  const events = new Events(server.child.stdout, server.closed);
  const { port } = await events.wait(event => event.event === "ready");
  const catalog = join(root, "catalog.json"), operation = randomUUID();
  // Custom loopback endpoints require a counter. The existing shared mock owns
  // this simulated route; the actual Codex local-counter assertion is above.
  writeFileSync(catalog, JSON.stringify({ fixture: { adapter: "responses", authentication: "optional",
    base_url: `http://127.0.0.1:${port}/skill/v1`,
    models: { tiny: { capacity: { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: false } } },
    max_event_bytes: 65536,
    token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
  } }));
  step = "run-shipping-finite-operation";
  const args = ["--workspace", workspace, "--settings-root", settings, "--catalog", catalog, "--model", "fixture/tiny", "--operation", operation];
  args.push(...(mode === "prompt" ? ["-p", "use the private skill"] : ["--input-format", "jsonl"]));
  const running = processes.start(binary, args, { env, cwd: workspace });
  running.child.stdin.end(mode === "prompt" ? "" : JSON.stringify({ key: { Session: { session: operation, file: { Request: operation } } },
    channel: null, bytes: { text: JSON.stringify({ kind: "prompt", text: "use the private skill", source: null, mentions: [] }) } }) + "\n");
  // Finite, non-held steps settle the operation. Owner death is observed by the
  // shared lifecycle and finally drains all children; no adapter-local timer.
  const result = await Promise.race([running.closed, server.closed.then(result => {
    throw new Error(`shared replay exited before operation settlement: ${JSON.stringify(result)}`);
  })]);
  captures.push(result);
  step = "verify-selector-and-terminal-outcome";
  records = recordedRequests(readFileSync(recordPath, "utf8"), events.seen);
  const requests = records.filter(record => record.path === "responses");
  assert.ok(records.every(record => record.status === 200), "every finite request must match");
  assert.equal(requests.length, authorized ? 2 : 1);
  assert.deepEqual(requests.map(record => record.step), authorized ? [0, 1] : [0]);
  const first = requests[0].body;
  assert.ok(!JSON.stringify(first).includes("PRIVATE_SKILL_BODY"), "discovery must keep the body lazy");
  const catalogs = first.input.flatMap(item => item.content ?? []).flatMap(item => {
    try { const value = JSON.parse(item.text); return Array.isArray(value.skills) ? [value] : []; } catch { return []; }
  });
  assert.equal(catalogs.length, 1, "one shared model-facing skill catalog");
  assert.deepEqual(JSON.parse(first.input[2].content[0].text), catalogs[0], "bind the actual catalog");
  assert.ok(catalogs[0].instructions.includes('"source"'));
  const source = catalogs[0].skills[0].source;
  assert.equal(catalogs[0].skills[0].name, "private-skill");
  const contract = first.tools.find(tool => tool.name === "read");
  assert.ok(JSON.stringify(contract).includes("ReadSourceInput"));
  assert.equal(contract.parameters.type, "object", "read parameters require an object root");
  const frames = result.stdout.trim().split("\n").map(JSON.parse);
  const files = [...frames[0][1].files, ...frames.slice(1).flatMap(change => [change.file, ...(change.files ?? [])].filter(Boolean))];
  const outcomes = files.filter(file => file.key.Session?.file.ExecutionResult === operation).map(file => JSON.parse(file.bytes.text));
  assert.equal(outcomes.length, 1, "one authoritative terminal outcome");
  assert.equal(result.code, authorized ? 0 : 1, result.stderr);
  if (authorized) {
    assert.equal(outcomes[0].completion, "completed");
    assert.equal(outcomes[0].ok.text, validRead ? "SKILL_READ_DONE" : "SKILL_READ_REJECTED");
    const outputs = requests[1].body.input.filter(item => item.type === "function_call_output");
    assert.equal(outputs.length, 1);
    if (validRead) {
      const result = JSON.parse(outputs[0].output), value = result.kind === "json" ? result.value : result;
      assert.equal(value.text, body); assert.equal(value.file.path, source.path);
      assert.deepEqual(value.file.volume, source.root.volume);
    } else assert.match(outputs[0].output, /tool executor rejected the admitted call \(invalid\).*retained identity/);
  } else {
    assert.equal(outcomes[0].completion, "failed");
    assert.match(outcomes[0].error, selection === "wrong-provider"
      ? /pinned context path is outside its declared root/ : /original private source grant/);
  }
  if (!validRead) assert.ok(!JSON.stringify(requests).includes("PRIVATE_SKILL_BODY"));
  assert.equal(readFileSync(join(skillDirectory, "SKILL.md"), "utf8"), body);
  passed = true;
} catch (error) { failure = { step, error: error.stack, recovery }; }
finally {
  try { await processes.close(); } catch (error) {
    passed = false; failure = { step: "drain-shared-process-scope", error: error.stack, recovery };
  }
  if (passed && process.env.E2E_OUTPUT && !output.startsWith(root)) {
    try { rmSync(root, { recursive: true }); }
    catch (error) { passed = false; failure = { step: "retire-owned-fixture", error: error.stack, recovery }; }
  }
  try {
    if (output) writeFileSync(join(output, "skill-read.json"), JSON.stringify({ passed, mode, selection,
      seconds: (performance.now() - begin) / 1000, fixture_origin: "pending-real-capture",
      counting: { codex: "actual local counter; zero backend count requests", replay: "shared simulated loopback counter" },
      mutations: mutations[selection], localCounter, records, captures, failure,
      fixture: passed ? null : root, children_after_drain: processes.children.size }));
  } catch (error) { passed = false; failure = { step: "retain-replay-proof", error: error.stack, recovery }; }
  if (passed) console.log(`PASS lazy private skill read (${mode}, ${selection})`);
  else {
    console.error(`STEP ${failure.step}: ${failure.error}\nRECOVERY: ${failure.recovery}`);
    if (root) console.error(`SKILL_READ_FAILURE_FIXTURE ${root}`);
    process.exitCode = 1;
  }
}
