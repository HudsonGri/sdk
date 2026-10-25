// Public bootstrap custody regressions use real private files created by the
// native API and the admitted executable; no fabricated OS output or PID probe.
import assert from "node:assert/strict";
import { linkSync, readFileSync, rmSync, statSync, utimesSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { Processes } from "../../scripts/graphcoder-e2e/process.mjs";
import { environment } from "../../scripts/graphcoder-e2e/scratch.mjs";
import { fixture, digest, packageVersion, target } from "./install-fixture.mjs";

function profile(root) {
  const inherited = Object.fromEntries(["PATH", "SystemRoot", "ComSpec", "PATHEXT", "E2E_PROCESS_HOST"]
    .filter(key => process.env[key]).map(key => [key, process.env[key]]));
  return { ...environment(join(root, "profile"), inherited), NODE_ENV: "test" };
}

test("installer retains its stable lock inode across owner exit and recovery", async () => {
  const value = fixture(), processes = new Processes();
  try {
    const env = profile(value.root);
    const run = extra => processes.runOwned(process.execPath, [join(value.bin, "install.js")],
      { cwd: value.root, env: { ...env, ...extra } });
    const interrupted = await run({ ACYCLIC_INSTALL_TEST_CRASH: "staged" });
    assert.equal(interrupted.code, 86, interrupted.stderr);
    const lock = join(value.bin, "install.lock");
    const before = statSync(lock);
    assert.equal(before.isFile(), true, "kernel lock ownership requires a persistent file");
    const recovered = await run({});
    assert.equal(recovered.code, 0, recovered.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    assert.equal(statSync(lock).ino, before.ino, "recovery must never replace the kernel lock inode");
  } finally {
    await processes.close();
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("installer refuses an aliased lock without touching its other name", async () => {
  const value = fixture(), processes = new Processes();
  try {
    const env = profile(value.root);
    // The public API creates the actual OS-private inode, including its Windows
    // DACL. This preparation is independent of the JavaScript under test.
    const input = JSON.stringify({ source: value.source, directory: value.bin,
      identity: { version: packageVersion, target, sha256: digest(value.source) } });
    // The shared owner's stdin is its lifecycle channel. A data-only Node
    // child feeds the command, while the same owner retains the entire tree.
    const feed = `const {spawnSync}=require("node:child_process");
      const [executable,input]=process.argv.slice(1);
      const result=spawnSync(executable,["__install-package"],{input,stdio:["pipe","inherit","inherit"],windowsHide:true});
      if(result.error||result.status!==0)throw new Error("step=feed native fixture input: "+(result.error?.message??"exit "+result.status)+"; recovery argv: "+JSON.stringify([process.execPath,...process.execArgv,...process.argv.slice(1)]));
      process.exitCode=result.status??1;`;
    const prepared = await processes.runOwned(process.execPath, ["-e", feed, value.source, input],
      { cwd: value.root, env });
    assert.equal(prepared.code, 0, prepared.stderr);
    const lock = join(value.bin, "install.lock"), alias = join(value.root, "aliased-lock");
    linkSync(lock, alias);
    // Real filesystem metadata makes the old PID/age reclamation path eligible;
    // the kernel-owned guard must refuse the alias regardless of its age.
    utimesSync(lock, 0, 0);
    const before = readFileSync(alias);
    const refused = await processes.runOwned(process.execPath, [join(value.bin, "install.js")],
      { cwd: value.root, env });
    assert.notEqual(refused.code, 0, "a hard-linked lock cannot establish exclusive namespace ownership");
    assert.match(refused.stderr, /step=publish native package/);
    assert.deepEqual(readFileSync(alias), before);
    assert.equal(statSync(alias).nlink, 2);
  } finally {
    await processes.close();
    rmSync(value.root, { recursive: true, force: true });
  }
});
