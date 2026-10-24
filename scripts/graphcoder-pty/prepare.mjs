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
  const required = /^bun@([0-9]+\.[0-9]+\.[0-9]+)$/.exec(manifest.packageManager);
  assert.ok(required, "terminal runtime needs an exact Bun packageManager");
  const binary = process.env.BUN_BINARY ?? "bun";
  const version = spawnSync(binary, ["--version"], { encoding: "utf8" });
  if (version.error) throw version.error;
  assert.equal(version.status, 0, "terminal package manager version probe failed");
  assert.equal(version.stdout.trim(), required[1], "terminal package manager version mismatch");
  const installed = spawnSync(binary, ["install", "--frozen-lockfile", "--ignore-scripts"], {
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
