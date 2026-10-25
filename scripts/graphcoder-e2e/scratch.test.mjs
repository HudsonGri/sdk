import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { environment, scratchDirectory } from "./scratch.mjs";
import { Processes } from "./process.mjs";

test("rejected missing scratch parents never create source directories through direct paths or aliases", () => {
  const repository = realpathSync(fileURLToPath(new URL("../../", import.meta.url)));
  const root = mkdtempSync(join(tmpdir(), "scratch-missing-contract-"));
  const missing = join(repository, basename(root));
  try {
    const alias = join(root, "source-alias");
    symlinkSync(repository, alias, process.platform === "win32" ? "junction" : "dir");
    for (const source of [repository, alias]) {
      assert.equal(existsSync(missing), false);
      assert.throws(() => scratchDirectory("refused-", {
        E2E_SCRATCH_ROOT: join(source, basename(root), "nested", "scratch"),
      }), /outside the source checkout/);
      assert.equal(existsSync(missing), false, "rejected source paths must have zero filesystem side effects");
    }
    const allowed = join(root, "missing", "outside");
    const owned = scratchDirectory("allowed-", { E2E_SCRATCH_ROOT: allowed });
    assert.equal(dirname(owned), realpathSync(allowed));
  } finally {
    rmSync(missing, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
  }
});

test("scratch roots cannot overlap the source checkout through direct paths or aliases", () => {
  const repository = realpathSync(fileURLToPath(new URL("../../", import.meta.url)));
  const root = mkdtempSync(join(tmpdir(), "scratch-source-contract-"));
  try {
    const alias = join(root, "source-alias");
    symlinkSync(repository, alias, process.platform === "win32" ? "junction" : "dir");
    for (const parent of [repository, join(repository, "scripts"), alias]) {
      let unexpected;
      try {
        assert.throws(() => { unexpected = scratchDirectory("source-overlap-control-", { E2E_SCRATCH_ROOT: parent }); },
          /outside the source checkout/);
      } finally { if (unexpected) rmSync(unexpected, { recursive: true, force: true }); }
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("fixture descendants cannot inherit a temporary directory outside their lease", async () => {
  const root = mkdtempSync(join(tmpdir(), "scratch-contract-")), processes = new Processes();
  try {
    const outside = join(root, "unowned"), settings = join(root, "leased", "settings");
    const inherited = { ...process.env, TEMP: outside, TMP: outside, TMPDIR: outside };
    const env = environment(settings, inherited);
    for (const key of ["TEMP", "TMP", "TMPDIR"]) assert.equal(env[key], join(settings, "tmp"));
    for (const key of ["HOME", "USERPROFILE", "CODEX_HOME", "APPDATA", "LOCALAPPDATA", "TEMP"])
      assert.ok(existsSync(env[key]), `${key} must exist before native host discovery`);
    // Real Node temp discovery and a real child write, rather than an env-only assertion.
    const code = "const p=require('node:path').join(require('node:os').tmpdir(),'child.txt');require('node:fs').writeFileSync(p,'leased');console.log(p)";
    const child = await processes.run(process.execPath, ["-e", code], { env });
    assert.equal(child.code, 0, child.stderr);
    assert.equal(child.stdout.trim(), join(settings, "tmp", "child.txt"));
    assert.equal(readFileSync(child.stdout.trim(), "utf8"), "leased");
    assert.equal(existsSync(outside), false, "the inherited directory must never be touched");
    assert.equal(inherited.TEMP, outside, "the caller's environment stays unchanged");
  } finally { await processes.close(); rmSync(root, { recursive: true, force: true }); }
});

test("Windows scratch requires one explicit root and rejects the competing old roots", () => {
  assert.throws(() => scratchDirectory("refused-", {}, "win32"), /E2E_SCRATCH_ROOT/);
  assert.throws(() => scratchDirectory("refused-", { E2E_SCRATCH_ROOT: "relative" }, "win32"), /absolute/);
  const root = mkdtempSync(join(tmpdir(), "scratch-root-contract-"));
  try {
    for (const key of ["E2E_WORKSPACE_ROOT", "E2E_NTFS_ROOT"]) {
      assert.throws(() => scratchDirectory("refused-", { E2E_SCRATCH_ROOT: root, [key]: join(root, "other") }, "win32"), /all acceptance scratch/);
    }
    for (const platform of ["linux", "darwin", "win32"]) {
      const child = scratchDirectory("owned-", { E2E_SCRATCH_ROOT: root }, platform);
      assert.ok(isAbsolute(child));
      assert.ok(!relative(realpathSync(root), child).startsWith(".."));
    }
    const target = join(root, "destination"), alias = join(root, "alias");
    mkdirSync(target);
    symlinkSync(target, alias, process.platform === "win32" ? "junction" : "dir");
    const child = scratchDirectory("canonical-", { E2E_SCRATCH_ROOT: alias });
    assert.equal(dirname(child), realpathSync(target), "scratch ownership uses filesystem identity across aliases");
  } finally { rmSync(root, { recursive: true, force: true }); }
});
