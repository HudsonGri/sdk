#!/usr/bin/env node
"use strict";

// JavaScript only locates and verifies the executable before starting it.
// The shared native installer owns locking, durable publication and recovery.
const { spawnSync } = require("node:child_process");
const { homedir, tmpdir } = require("node:os");
const { existsSync, readFileSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");
const { hostTarget, verifyTarget } = require("./verify.js");

function durableRename(from, to, helper, replace) {
  const args = ["__installer-rename", from, to, replace ? "replace" : "no-replace"];
  const result = spawnSync(helper, args, {
    stdio: "inherit",
    windowsHide: true,
  });
  if (result.error || result.status !== 0) {
    throw new Error(`step=durably rename installed artifact${result.error ? `: ${result.error.message}` : ""}; recovery argv: ${JSON.stringify([helper, ...args])}`);
  }
}

function durableStateDirectory() {
  if (process.platform === "win32" && process.env.LOCALAPPDATA) {
    return join(process.env.LOCALAPPDATA, "Acyclic", "state-v5");
  }
  if (process.platform !== "win32" && process.env.XDG_STATE_HOME) {
    return join(process.env.XDG_STATE_HOME, "acyclic", "state-v5");
  }
  const home = process.env.HOME || homedir();
  if (process.platform !== "win32" && home) {
    return join(home, ".local", "state", "acyclic", "state-v5");
  }
  return join(tmpdir(), "acyclic-state-v5");
}

function installCertification(version, helper) {
  const name = `native-mount-${hostTarget()}.json`;
  const source = join(__dirname, "..", "certification", name);
  if (!existsSync(source)) return;
  const destination = join(durableStateDirectory(), "certification", name);
  const args = ["__publish-certification", source, version, destination];
  const result = spawnSync(helper, args, { stdio: ["ignore", "ignore", "pipe"], encoding: "utf8", windowsHide: true });
  if (result.error || result.status !== 0) {
    throw new Error(`step=verify installed release certification: ${result.error?.message || result.stderr}; recovery argv: ${JSON.stringify([helper, ...args])}`);
  }
}

function warmUp(installed) {
  const test = process.env.NODE_ENV === "test";
  const executable = (test && process.env.ACYCLIC_INSTALL_TEST_WARM_UP_EXECUTABLE) || installed;
  const result = spawnSync(executable, ["--version"], {
    stdio: "ignore", timeout: 30_000, windowsHide: true,
  });
  if (test && process.env.ACYCLIC_INSTALL_TEST_WARM_UP_LOG) {
    const outcome = result.error ? `error ${result.error.code}` : `status ${result.status}`;
    writeFileSync(process.env.ACYCLIC_INSTALL_TEST_WARM_UP_LOG, `${executable} ${outcome}\n`, { flag: "a" });
  }
}

function ensureInstalled() {
  const { source, manifest } = verifyTarget(__dirname);
  const version = JSON.parse(readFileSync(join(__dirname, "..", "package.json"), "utf8")).version;
  const target = hostTarget();
  const request = { source, directory: __dirname, identity: { version, target, sha256: manifest.targets[target].sha256 } };
  const result = spawnSync(source, ["__install-package"], {
    input: JSON.stringify(request), stdio: ["pipe", "inherit", "pipe"], encoding: "utf8", windowsHide: true,
  });
  if (process.env.NODE_ENV === "test" && result.status === 86) process.exit(86);
  if (result.error || result.status !== 0) {
    throw new Error(`step=install native package: ${result.error?.message || result.stderr}; recovery argv: ${JSON.stringify([process.execPath, __filename])}`);
  }
  const installed = join(__dirname, process.platform === "win32" ? "acyclic.exe" : "acyclic");
  installCertification(version, installed);
  warmUp(installed);
  return installed;
}

if (require.main === module) {
  try { ensureInstalled(); } catch (error) {
    console.error(`step=bootstrap native package: ${error.message}; recovery: npm install -g --allow-scripts=@acyclic-labs/plugin @acyclic-labs/plugin`);
    process.exitCode = 1;
  }
}
module.exports = { durableRename };
