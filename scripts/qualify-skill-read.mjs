#!/usr/bin/env node
// Exercise lazy discovery and its authenticated read through the shipping binary.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { Processes } from "./graphcoder-e2e/process.mjs";

const [binary, mode = "prompt", selection = "valid"] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute shipping GraphCoder required");
assert.ok(["prompt", "jsonl"].includes(mode));
assert.ok(["valid", "ungranted", "unretained"].includes(selection));
const begin = performance.now(), processes = new Processes();
const root = mkdtempSync(join(process.env.E2E_SCRATCH_ROOT ?? tmpdir(), "skill-read-"));
const workspace = join(root, "workspace"), settings = join(root, "settings");
const skillDirectory = join(settings, ".agents/skills/private-skill");
for (const path of [workspace, skillDirectory, ...[".codex", "local", "roaming"].map(p => join(settings, p))]) mkdirSync(path, { recursive: true });
const body = "---\nname: private-skill\ndescription: LAZY_READ_DISCOVERY\n---\nPRIVATE_SKILL_BODY_λ\n";
writeFileSync(join(skillDirectory, "SKILL.md"), body);
const captures = [], requests = [];
let step = 0, source, rejectFailure;
const failed = new Promise((_, reject) => { rejectFailure = reject; });
failed.catch(() => {});
const sse = value => `data: ${JSON.stringify(value)}\n\n`;
const server = createServer(async (request, response) => {
  try {
    let bytes = ""; for await (const chunk of request) bytes += chunk;
    const requestBody = JSON.parse(bytes);
    if (request.url === "/count") {
      response.setHeader("content-type", "application/json"); response.end(JSON.stringify({ input_tokens: 64 })); return;
    }
    assert.equal(request.url, "/responses");
    requests.push(requestBody);
    const outputs = requestBody.input.filter(item => item.type === "function_call_output");
    const current = step++;
    response.setHeader("content-type", "text/event-stream");
    let selected;
    if (current === 0) {
      assert.ok(!bytes.includes("PRIVATE_SKILL_BODY"), "discovery must keep the body lazy");
      const catalogs = requestBody.input.flatMap(item => item.content ?? [])
        .filter(item => item.type === "input_text")
        .flatMap(item => { try { return [JSON.parse(item.text)]; } catch { return []; } })
        .filter(item => Array.isArray(item.skills));
      assert.equal(catalogs.length, 1, "one shared model-facing skill catalog");
      assert.ok(catalogs[0].instructions.includes('"source"'), "catalog explains the portable read contract");
      const skill = catalogs[0].skills.find(skill => skill.name === "private-skill");
      assert.ok(skill); source = skill.source;
      const contract = requestBody.tools.find(tool => tool.name === "read");
      assert.ok(JSON.stringify(contract).includes("ReadSourceInput"), "the offered read schema accepts lazy sources");
      selected = structuredClone(source);
      if (selection === "ungranted") selected.root.volume.id += "-ungranted";
      if (selection === "unretained") selected.path = "inputs/skills/unretained/SKILL.md";
    } else {
      assert.notEqual(selection, "ungranted", "a forged grant must terminate before another model request");
      assert.equal(current, 1);
      assert.equal(outputs.length, 1);
      if (selection === "valid") {
        const result = JSON.parse(outputs.at(-1).output);
        const value = result.kind === "json" ? result.value : result;
        assert.equal(value.text, body);
        assert.equal(value.file.path, source.path);
        assert.deepEqual(value.file.volume, source.root.volume);
      } else {
        assert.match(outputs[0].output, /tool executor rejected the admitted call \(invalid\).*retained identity/);
        assert.ok(!bytes.includes("PRIVATE_SKILL_BODY"), "an unretained source must not disclose a body");
      }
      response.write(sse({ type: "response.output_item.done", output_index: 0,
        item: { type: "message", role: "assistant", content: [{ type: "output_text", text: selection === "valid" ? "SKILL_READ_DONE" : "SKILL_READ_REJECTED" }] } }));
    }
    if (selected) response.write(sse({ type: "response.output_item.done", output_index: 0,
      item: { type: "function_call", call_id: `read-${current}`, name: "read", arguments: JSON.stringify({ source: selected }) } }));
    response.end(sse({ type: "response.completed", response: { status: "completed", usage: { input_tokens: 64, output_tokens: 8 } } }));
  } catch (error) { rejectFailure(error); if (!response.headersSent) response.writeHead(500); response.end("fixture assertion failed"); }
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const catalog = join(root, "catalog.json"), operation = randomUUID();
writeFileSync(catalog, JSON.stringify({ fixture: { adapter: "responses", authentication: "optional",
  base_url: `http://127.0.0.1:${server.address().port}`,
  models: { tiny: { capacity: { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: false } } }, max_event_bytes: 65536,
  token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 } } }));
const env = { ...process.env, HOME: settings, USERPROFILE: settings, CODEX_HOME: join(settings, ".codex"),
  APPDATA: join(settings, "roaming"), LOCALAPPDATA: join(settings, "local"),
  XDG_CONFIG_HOME: settings, XDG_DATA_HOME: settings, XDG_CACHE_HOME: settings, XDG_STATE_HOME: settings };
for (const key of Object.keys(env)) if (/API_KEY$|AUTH_TOKEN$|^GRAPHCODER_MODEL$/.test(key)) delete env[key];
let passed = false;
const timer = setTimeout(() => rejectFailure(new Error("skill read exceeded the existing nine-second work budget")), 9000);
try {
  const args = ["--workspace", workspace, "--settings-root", settings, "--catalog", catalog, "--model", "fixture/tiny", "--operation", operation];
  args.push(...(mode === "prompt" ? ["-p", "use the private skill"] : ["--input-format", "jsonl"]));
  const running = processes.start(binary, args, { env, cwd: workspace });
  running.child.stdin.end(mode === "prompt" ? "" : JSON.stringify({ key: { Session: { session: operation, file: { Request: operation } } },
    channel: null, bytes: { text: JSON.stringify({ kind: "prompt", text: "use the private skill", source: null, mentions: [] }) } }) + "\n");
  const result = await Promise.race([failed, running.closed]); captures.push(result);
  assert.equal(result.code, selection === "ungranted" ? 1 : 0, result.stderr + result.stdout.slice(-2500));
  const frames = result.stdout.trim().split("\n").map(JSON.parse);
  const files = [...frames[0][1].files, ...frames.slice(1).flatMap(change => [change.file, ...(change.files ?? [])].filter(Boolean))];
  const outcomes = files.filter(file => file.key.Session?.file.ExecutionResult === operation).map(file => JSON.parse(file.bytes.text));
  assert.equal(outcomes.length, 1);
  if (selection !== "ungranted") {
    assert.equal(outcomes[0].completion, "completed");
    assert.equal(outcomes[0].ok.text, selection === "valid" ? "SKILL_READ_DONE" : "SKILL_READ_REJECTED");
    assert.equal(step, 2);
  } else {
    assert.equal(outcomes[0].completion, "failed");
    assert.equal(typeof outcomes[0].error, "string");
    if (selection === "ungranted") assert.match(outcomes[0].error, /original private source grant/);
    assert.equal(step, 1);
    assert.ok(!JSON.stringify(requests).includes("PRIVATE_SKILL_BODY"));
  }
  if (selection === "unretained") assert.ok(!JSON.stringify(requests).includes("PRIVATE_SKILL_BODY"));
  assert.equal(readFileSync(join(skillDirectory, "SKILL.md"), "utf8"), body);
  passed = true; console.log(`PASS lazy private skill read (${mode}, ${selection})`);
} finally {
  clearTimeout(timer); await processes.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  const proof = { passed, mode, selection, seconds: (performance.now() - begin) / 1000, requests, captures, fixture: passed ? null : root };
  if (process.env.E2E_OUTPUT) { const output = resolve(process.env.E2E_OUTPUT); mkdirSync(output, { recursive: true }); writeFileSync(join(output, "skill-read.json"), JSON.stringify(proof)); }
  if (passed) rmSync(root, { recursive: true, force: true }); else console.error(`SKILL_READ_FAILURE_FIXTURE ${root}`);
}
