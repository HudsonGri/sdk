import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative } from "node:path";
import test from "node:test";
import { environment, scratchDirectory } from "./scratch.mjs";
import { Processes } from "./process.mjs";

test("fixture descendants cannot inherit host profiles or temporary directories outside their lease", async () => {
  const root = mkdtempSync(join(tmpdir(), "scratch-contract-")), processes = new Processes();
  try {
    const outside = join(root, "unowned"), settings = join(root, "leased", "settings");
    const keys = ["TEMP", "TMP", "TMPDIR", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "PI_CODING_AGENT_DIR"];
    const inherited = { ...process.env, ...Object.fromEntries(keys.map(key => [key, outside])) };
    const env = environment(settings, inherited);
    for (const key of ["TEMP", "TMP", "TMPDIR"]) assert.equal(env[key], join(settings, "tmp"));
    for (const [key, path] of [["CODEX_HOME", ".codex"], ["CLAUDE_CONFIG_DIR", ".claude"], ["PI_CODING_AGENT_DIR", ".pi/agent"]])
      assert.equal(env[key], join(settings, path), `${key} must stay inside its fixture`);
    for (const key of ["HOME", "USERPROFILE", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "PI_CODING_AGENT_DIR", "APPDATA", "LOCALAPPDATA", "TEMP"])
      assert.ok(existsSync(env[key]), `${key} must exist before native host discovery`);
    // Real Node temp discovery and real writes through every inherited host selector.
    const code = `const fs=require('node:fs'),path=require('node:path');
      const roots=[require('node:os').tmpdir(),...${JSON.stringify(keys)}.map(key=>process.env[key])];
      const files=roots.map((root,index)=>{const file=path.join(root,index+'.txt');fs.writeFileSync(file,'leased');return file});
      console.log(JSON.stringify(files))`;
    const child = await processes.run(process.execPath, ["-e", code], { env });
    assert.equal(child.code, 0, child.stderr);
    const files = JSON.parse(child.stdout);
    assert.deepEqual(files, [join(settings, "tmp"), ...keys.map(key => env[key])].map((root, index) => join(root, `${index}.txt`)));
    for (const file of files) assert.equal(readFileSync(file, "utf8"), "leased");
    assert.equal(existsSync(outside), false, "the inherited directory must never be touched");
    for (const key of keys) assert.equal(inherited[key], outside, "the caller's environment stays unchanged");
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
