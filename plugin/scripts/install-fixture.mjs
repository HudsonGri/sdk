// Shared package-installer fixtures use the actual admitted shipping executable.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { chmodSync, copyFileSync, cpSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
const plugin = dirname(dirname(fileURLToPath(import.meta.url)));
const require = createRequire(import.meta.url);
const { hostTarget, sha256File } = require("../bin/verify.js");
const target = hostTarget();
const binary = process.env.ACYCLIC_TEST_BINARY;
assert.ok(binary, "step=admit shipping installer fixture; recovery=set ACYCLIC_TEST_BINARY to the source-qualified shipping executable and rerun node --test plugin/scripts/install.test.mjs");
const packageVersion = JSON.parse(readFileSync(join(plugin, "package.json"), "utf8")).version;
const commandVersion = spawnSync(binary, ["--version"], { encoding: "utf8" });
assert.equal(commandVersion.status, 0, commandVersion.stderr);
assert.equal(commandVersion.stdout.trim(), `acyclic ${packageVersion}`);
const nativeVersion = commandVersion.stdout.trim();

const executableName = process.platform === "win32" ? "acyclic.exe" : "acyclic";
const scratch = process.env.ACYCLIC_TEST_TEMP_ROOT
  || join(dirname(plugin), "target", "plugin-installer-tests");
mkdirSync(scratch, { recursive: true });

function digest(path) {
  return sha256File(path);
}

function fixture() {
  const root = mkdtempSync(join(scratch, "install-"));
  const bin = join(root, "bin");
  cpSync(join(plugin, "bin"), bin, {
    recursive: true,
    filter: source => !basename(source).startsWith("acyclic."),
  });
  writeFileSync(join(root, "package.json"), JSON.stringify({ version: packageVersion }));
  const source = join(bin, target, executableName);
  mkdirSync(dirname(source), { recursive: true });
  copyFileSync(binary, source);
  chmodSync(source, 0o755);
  const relocated = spawnSync(source, ["--version"], { encoding: "utf8" });
  assert.equal(relocated.status, 0, relocated.stderr);
  assert.equal(relocated.stdout.trim(), nativeVersion);
  writeFileSync(join(bin, "platform-binaries.json"), JSON.stringify({
    version: 1,
    targets: { [target]: { path: `${target}/${executableName}`, sha256: digest(source) } },
  }));
  return { root, bin, source, installed: join(bin, executableName) };
}

function install(bin, crash, failure, extraEnvironment = {}) {
  return spawnSync(process.execPath, [join(bin, "install.js")], {
    encoding: "utf8",
    env: {
      ...process.env,
      NODE_ENV: "test",
      ACYCLIC_INSTALL_SKIP_DRAIN: "1",
      ...extraEnvironment,
      ...(crash ? { ACYCLIC_INSTALL_TEST_CRASH: crash } : {}),
      ...(failure ? { ACYCLIC_INSTALL_TEST_FAIL: failure } : {}),
    },
  });
}

export { plugin, target, binary, packageVersion, nativeVersion, executableName, scratch, digest, fixture, install };
