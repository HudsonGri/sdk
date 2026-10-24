#!/usr/bin/env node
// Admit the terminal runtime before compiling or timing acceptance cases.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

export function prepare(directory = dirname(fileURLToPath(import.meta.url))) {
  const lock = join(directory, "bun.lock");
  const original = readFileSync(lock);
  const manifest = JSON.parse(readFileSync(join(directory, "package.json"), "utf8"));
  assert.equal(manifest.name, "graphcoder-pty-qualification");
  const installed = spawnSync(process.env.BUN_BINARY ?? "bun", ["install", "--frozen-lockfile", "--ignore-scripts"], {
    cwd: directory, stdio: "inherit",
  });
  if (installed.error) throw installed.error;
  assert.equal(installed.status, 0, "terminal runtime installation failed");
  assert.deepEqual(readFileSync(lock), original, "terminal runtime changed its frozen lock");
  const imported = spawnSync(process.execPath, ["--input-type=module", "-e",
    `const {Terminal} = await import(${JSON.stringify(pathToFileURL(join(directory, "screen.mjs")).href)}); new Terminal({cols:80,rows:24}).dispose();`],
  { cwd: directory, stdio: "inherit" });
  if (imported.error) throw imported.error;
  assert.equal(imported.status, 0, "terminal runtime import failed");
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) prepare();
