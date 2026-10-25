import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { napiCli, napiGeneratorIdentity } from "./napi-cli.mjs";
import { preflight as filesystem } from "./filesystem-napi-types.mjs";
import { preflight as actors } from "./actors-napi-types.mjs";
import { preflight as stream } from "./stream-napi-types.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
import { nativeFamily } from "./native-family.mjs";

test("every declaration family loads the exact attested native generator", async () => {
  const require = createRequire(import.meta.url);
  const loaded = require("@napi-rs/cli");
  for (const load of [napiCli, filesystem, actors, stream]) assert.equal(load(), loaded);
  const identity = await napiGeneratorIdentity(fileURLToPath(new URL("..", import.meta.url)));
  const entry = await readFile(require.resolve("@napi-rs/cli"));
  assert.equal(identity.entry_sha256, `sha256:${createHash("sha256").update(entry).digest("hex")}`);
  assert.equal(typeof loaded.generateTypeDef, "function");
  assert.equal(typeof loaded.NapiCli, "function");
  for (const family of ["actors", "stream"]) {
    assert.deepEqual(await createNativeProducer(nativeFamily(family)).napiGeneratorIdentity(), identity);
  }
});

test("missing declaration dependency rejects before Cargo or target creation", async () => {
  const directory = await mkdtemp(join(tmpdir(), "napi-dependency-admission-"));
  try {
    for (const name of ["napi-cli.mjs", "filesystem-napi-types.mjs"]) {
      await cp(new URL(name, import.meta.url), join(directory, name));
    }
    const target = join(directory, "owned-target");
    const module = pathToFileURL(join(directory, "filesystem-napi-types.mjs")).href;
    const result = spawnSync(process.execPath, ["--input-type=module", "-e",
      `const {renderTypes}=await import(${JSON.stringify(module)});await renderTypes(${JSON.stringify(directory)},"filesystem",[]);`], {
      encoding: "utf8",
      env: { ...process.env, CARGO_TARGET_DIR: target, ACYCLIC_CARGO_BIN: "dependency-must-reject-before-cargo" },
    });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /Cannot find module '@napi-rs\/cli'/u);
    await assert.rejects(stat(target), { code: "ENOENT" });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
