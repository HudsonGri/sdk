import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { stageArtifacts } from "./artifacts.mjs";
import { Processes } from "./process.mjs";
import { verifyNativeImage } from "../verify-release-binary.mjs";

const fixtures = fileURLToPath(new URL("./fixtures/native-images/", import.meta.url));
const origin = JSON.parse(await readFile(join(fixtures, "origin.json"), "utf8"));
const captured = JSON.parse(await readFile(join(fixtures, "cargo-filesystem-artifact.json"), "utf8"));
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
// Fixture inputs are a real tiny crate. Every Cargo record below is captured
// from actual Cargo stdout during this test; no compiler output is invented.
async function cargoFixture(root, testProfile) {
  await writeFile(join(root, "Cargo.toml"), '[package]\nname="native-custody-fixture"\nversion="0.0.0"\nedition="2024"\n[lib]\npath="lib.rs"\ncrate-type=["cdylib"]\n');
  await writeFile(join(root, "lib.rs"), '#[test] fn native_custody() { assert_eq!(2 + 2, 4); }\npub fn fixture() -> u32 { 4 }\n');
  const args = testProfile ? ["test", "--offline", "--lib", "--no-run", "--message-format=json"]
    : ["build", "--offline", "--lib", "--message-format=json"];
  const output = spawnSync("cargo", args, { cwd: root, encoding: "utf8",
    env: { ...process.env, CARGO_TARGET_DIR: join(root, "target") } });
  assert.equal(output.status, 0, output.stdout + output.stderr);
  await writeFile(join(root, "cargo.jsonl"), output.stdout);
  return { name: testProfile ? "proof" : "binding.node", target: "native_custody_fixture",
    test: testProfile, kind: testProfile ? "executable" : "cdylib", records: output.stdout };
}

test("Cargo-selected native artifact retains custody after target-cache eviction", async () => {
  const root = await mkdtemp(join(tmpdir(), "e2e-artifact-custody-"));
  const processes = new Processes();
  try {
    const spec = await cargoFixture(root, true);
    const manifest = await stageArtifacts([spec], join(root, "bundle"));
    const artifact = manifest.artifacts.proof;
    await rm(join(root, "target"), { recursive: true });
    await assert.rejects(processes.run(artifact.source, ["--exact", "native_custody"]), /ENOENT/u,
      "former target-path handoff survived eviction");
    const result = await processes.run(artifact.path, ["--exact", "native_custody"]);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, /test result: ok\. 1 passed; 0 failed; 0 ignored;/u);
    assert.equal(artifact.sha256, hash(await readFile(artifact.path)));
    assert.deepEqual(JSON.parse(await readFile(join(root, "bundle", "artifacts.json"))), manifest);
    await assert.rejects(stageArtifacts([spec], join(root, "bundle")), /EEXIST/);
  } finally { await processes.close(); await rm(root, { recursive: true, force: true }); }
});

test("actual Cargo cdylib is admitted by the same native image parser", async () => {
  const root = await mkdtemp(join(tmpdir(), "e2e-library-custody-"));
  try {
    const spec = await cargoFixture(root, false);
    const manifest = await stageArtifacts([spec], join(root, "bundle"));
    assert.equal(manifest.artifacts["binding.node"].kind, "cdylib");
    assert.match(verifyNativeImage(await readFile(manifest.artifacts["binding.node"].path)), /^(?:linux|darwin|win32)-/u);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("captured Cargo-zero corrupt image is retained but cannot publish a runnable manifest", async () => {
  const root = await mkdtemp(join(tmpdir(), "e2e-corrupt-image-"));
  try {
    const bytes = await readFile(join(fixtures, "invalid-filesystem-image"));
    assert.equal(hash(bytes), origin.invalid.image_sha256);
    const input = join(root, "captured-image"); await copyFile(join(fixtures, "invalid-filesystem-image"), input);
    // Only relocate the real captured record's paths. Package/target/profile,
    // features and fresh=false remain the exact production Cargo evidence.
    const record = { ...captured, executable: input, filenames: [input] };
    const spec = { name: "filesystem", target: captured.target.name, test: captured.profile.test, records: JSON.stringify(record) };
    const bundle = join(root, "bundle");
    await assert.rejects(stageArtifacts([spec], bundle), /kind=ARTIFACT_IMAGE step=staged-native-image.*retained=.*recovery=/u);
    await assert.rejects(readFile(join(bundle, "artifacts.json")), /ENOENT/u);
    assert.equal(hash(await readFile(join(bundle, "filesystem"))), origin.invalid.image_sha256);
    const former = await readFile(join(fixtures, "artifacts-before.mjs.txt"));
    assert.equal(hash(former), origin.old_production.sha256);
    const oldModule = join(root, "former.mjs"); await writeFile(oldModule, former);
    const old = await import(pathToFileURL(oldModule));
    const oldManifest = await old.stageArtifacts([spec], join(root, "old-bundle"));
    assert.equal(oldManifest.artifacts.filesystem.sha256, origin.invalid.image_sha256,
      "actual former production publishes the same non-native image");
    for (const bad of ["", spec.records + "\n" + spec.records,
      JSON.stringify({ ...record, executable: "relative-image" }),
      JSON.stringify({ ...record, profile: { ...record.profile, test: false } })])
      await assert.rejects(stageArtifacts([{ ...spec, records: bad }], join(root, "bad-selection")));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("actual ELF, PE and universal Mach-O images share release and staging validation", async () => {
  const pe = await readFile(join(fixtures, "windows-where.exe"));
  const mach = await readFile(join(fixtures, "macos-true"));
  assert.equal(hash(pe), origin.windows.sha256); assert.equal(hash(mach), origin.macos.sha256);
  const elf = await readFile(join(fixtures, "linux-true"));
  assert.equal(hash(elf), origin.linux.sha256);
  assert.equal(verifyNativeImage(elf), "linux-x64");
  assert.match(verifyNativeImage(await readFile(process.execPath)), /^(?:linux|darwin|win32)-/u);
  assert.equal(verifyNativeImage(pe), "win32-x64");
  assert.match(verifyNativeImage(mach), /^darwin-/u);
  for (const image of [pe, mach]) assert.throws(() => verifyNativeImage(image.subarray(0, image.length >>> 1)), /bounds|truncated/u);
  assert.throws(() => verifyNativeImage(pe, "win32-arm64"), /machine does not match/u);
});
