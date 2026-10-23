import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

test("Pi registration delegates execution and exposes only native coordination", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".shim-contract-"));
  try {
    const path = join(directory, "index.ts");
    writeFileSync(path, readFileSync(new URL("../../pi/index.ts", import.meta.url)));
    const { default: acyclic } = await import(pathToFileURL(path).href);
    const hooks: string[] = [], tools: string[] = [];
    const forbidden = () => {throw new Error("registration executed host work");};
    const api = {on: (event: string) => {hooks.push(event);},
      registerTool: (tool: {name: string}) => {tools.push(tool.name);},
      registerProvider: forbidden, exec: forbidden} as unknown as ExtensionAPI;
    acyclic(api);
    assert.deepEqual(tools, ["spawn", "send", "wait", "archive", "cancel", "resume"]);
    assert.deepEqual(hooks, ["session_start", "agent_end", "session_shutdown"]);
    assert(!tools.some(name => ["bash", "read", "write", "edit"].includes(name)));
  } finally {rmSync(directory, {recursive: true, force: true});}
});

test("host manifests and shims fit the native hook budget without tool ledgers", () => {
  const source = (relative: string) => readFileSync(new URL(relative, import.meta.url), "utf8");
  const lines = (text: string) => text.trimEnd().split(/\r?\n/).length;
  const codex = source("../../hooks/hooks.json");
  const bridge = source("../../src/codex.rs");
  const manifests = ["../../plugin.json", "../../.codex-plugin/plugin.json",
    "../../.mcp.json", "../../.agents/plugins/marketplace.json"].map(source);
  assert(lines(codex) + lines(bridge) + manifests.reduce((sum, manifest) => sum + lines(manifest), 0) <= 300);
  assert(lines(source("../../pi/index.ts")) <= 300);
  assert(lines(source("../../claude/.claude-plugin/plugin.json")) <= 300);
  const claude = JSON.parse(source("../../claude/.claude-plugin/plugin.json"));
  assert.deepEqual(Object.keys(claude.hooks), ["SessionStart", "Stop", "SessionEnd"]);
  assert(claude.mcpServers["acyclic-sessions"]);
  const hooks = JSON.parse(codex).hooks;
  assert.deepEqual(Object.keys(hooks), ["SessionStart", "Stop", "SessionEnd"]);
  assert(Object.values(hooks).every((groups: any) => groups.every((group: any) => group.hooks.every((hook: any) => hook.type === "command"))));
  const mcp = JSON.parse(source("../../.mcp.json"));
  assert.deepEqual(mcp.mcpServers["acyclic-sessions"].env_vars, ["ACYCLIC_HOST_ARGS", "ACYCLIC_WORKSPACE", "ACYCLIC_TASK"]);
  assert(!source("../../.mcp.json").includes("XDG_STATE_HOME"));
});

test("release packaging validates without state access or authentication", () => {
  const directory = mkdtempSync(join(tmpdir(), "shim-package-"));
  const script = (name: string) => fileURLToPath(new URL(`../../scripts/${name}`, import.meta.url));
  const run = (name: string, args: string[]) => spawnSync(process.execPath, [script(name), ...args], {encoding: "utf8", timeout: 2000});
  try {
    const binary = join(directory, "fixture-binary");
    writeFileSync(binary, "fixture executable bytes; packaging never executes the input binary");
    const output = join(directory, "release");
    const built = run("package.mjs", ["--binary", `linux-x64-gnu=${binary}`, "--out", output]);
    assert.equal(built.status, 0, built.stderr);
    const marketplace = JSON.parse(readFileSync(join(output, "marketplace.json"), "utf8"));
    assert.equal(marketplace.plugins[0].policy.authentication, undefined);
    assert.equal(run("validate-package.mjs", [output]).status, 0);
    const path = join(output, "plugin", ".mcp.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    manifest.mcpServers["acyclic-sessions"].env_vars = ["XDG_STATE_HOME"];
    writeFileSync(path, JSON.stringify(manifest));
    const rejected = run("validate-package.mjs", [output]);
    assert.notEqual(rejected.status, 0, "negative control: state-dependent declarations must be rejected");
    assert.match(rejected.stderr, /MCP declaration/);
  } finally {rmSync(directory, {recursive: true, force: true});}
});
