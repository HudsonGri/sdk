import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { test } from "bun:test";
import { createAgentSession, DefaultResourceLoader, SessionManager,
  type AgentSession, type ExtensionAPI } from "@earendil-works/pi-coding-agent";

test("Pi registration delegates execution and exposes only native coordination", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".shim-contract-"));
  try {
  const path = join(directory, "index.ts");
  writeFileSync(path, readFileSync(new URL("../../pi/index.ts", import.meta.url)));
  const { default: acyclic, drainNativeSession } = await import(pathToFileURL(path).href);
  const hooks: string[] = [];
  const tools: string[] = [];
  const forbidden = () => { throw new Error("registration executed host work"); };
  const api = { on: (event: string) => { hooks.push(event); },
    registerTool: (tool: {name: string}) => { tools.push(tool.name); },
    registerProvider: forbidden, exec: forbidden } as unknown as ExtensionAPI;
  acyclic(api);
  assert.deepEqual(tools, ["spawn_agent", "wait_agent", "cancel_agent", "reopen_agent"]);
  assert.deepEqual(hooks, ["session_start", "tool_call", "tool_result", "session_shutdown"]);
  assert(!tools.some(name => ["bash", "read", "write", "edit"].includes(name)));
  const live = new Set(["child", "grandchild"]);
  const order: string[] = [];
  const native = (name: string, descendants: AgentSession[] = []) => ({
    abort: async () => { live.delete(name); order.push(`abort ${name}`); },
    extensionRunner: { emit: async (event: {type: string; reason: string}) => {
      assert.deepEqual(event, {type: "session_shutdown", reason: "quit"});
      for (const child of descendants) await drainNativeSession(child, Promise.resolve());
      order.push(`captured ${name}`);
    } },
  }) as unknown as AgentSession;
  const child = native("child", [native("grandchild")]);
  await child.abort();
  assert(live.has("grandchild"), "negative control: native abort alone leaves descendants running");
  order.length = 0;
  await drainNativeSession(child, Promise.resolve());
  assert.equal(live.size, 0);
  assert.deepEqual(order, ["abort child", "abort grandchild", "captured grandchild", "captured child"]);
  const broken = { abort: async () => {}, extensionRunner: {emit: async () => {throw new Error("drain failed");}} } as unknown as AgentSession;
  await assert.rejects(drainNativeSession(broken, Promise.resolve()), /drain failed/);
  const cwd = join(directory, "workspace", ".volumes", "agents", "native-child");
  const agentDir = join(directory, "fixture-agent-dir");
  mkdirSync(cwd, {recursive: true}); mkdirSync(agentDir);
  const events: string[] = [];
  const loader = new DefaultResourceLoader({ cwd, agentDir, noExtensions: true,
    noSkills: true, noPromptTemplates: true, noThemes: true,
    extensionFactories: [api => {
      api.on("session_start", async (_event, ctx) => { events.push(`start ${ctx.cwd}`); });
      api.on("session_shutdown", async () => { events.push("shutdown"); });
    }],
  });
  await loader.reload();
  const {session} = await createAgentSession({cwd, agentDir, tools: [],
    sessionManager: SessionManager.inMemory(cwd), resourceLoader: loader});
  try {
    assert.deepEqual(events, [], "negative control: SDK construction does not start native extensions");
    await session.bindExtensions({mode: "print"});
    assert.deepEqual(events, [`start ${cwd}`]);
    await drainNativeSession(session, Promise.resolve());
    assert.deepEqual(events, [`start ${cwd}`, "shutdown"]);
  } finally { session.dispose(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("host manifests and shims fit the native hook budget", () => {
  const source = (relative: string) => readFileSync(new URL(relative, import.meta.url), "utf8");
  const lines = (text: string) => text.trimEnd().split(/\r?\n/).length;
  const codex = source("../../hooks/hooks.json");
  const bridge = source("../../src/codex.rs");
  assert(lines(codex) + lines(bridge) <= 300);
  assert(lines(source("../../pi/index.ts")) <= 300);
  const claude = JSON.parse(source("../../claude/.claude-plugin/plugin.json"));
  assert(lines(source("../../claude/.claude-plugin/plugin.json")) <= 300);
  assert(claude.hooks.WorktreeCreate);
  assert(claude.hooks.WorktreeRemove);
  const hooks = JSON.parse(codex).hooks;
  assert.equal(hooks.PostToolUse[0].hooks[0].input.hook.tool_response, "${tool_response}");
  assert(!source("../../.mcp.json").includes("XDG_STATE_HOME"));
});
