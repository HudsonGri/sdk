import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

test("Pi registration delegates execution and exposes only native coordination", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".shim-contract-"));
  try {
  const path = join(directory, "index.ts");
  writeFileSync(path, readFileSync(new URL("../../pi/index.ts", import.meta.url)));
  const { default: acyclic } = await import(pathToFileURL(path).href);
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
