import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { prepare } from "./prepare.mjs";

test("a missing frozen lock is rejected before starting the installer", () => {
  const directory = mkdtempSync(join(process.env.E2E_SCRATCH_ROOT ?? tmpdir(), "terminal-prepare-"));
  const previous = process.env.BUN_BINARY;
  try {
    writeFileSync(join(directory, "package.json"), readFileSync(new URL("./package.json", import.meta.url)));
    process.env.BUN_BINARY = join(directory, "installer-must-not-start");
    assert.throws(() => prepare(directory), error =>
      error.code === "ENOENT" && error.path === join(directory, "bun.lock"));
  } finally {
    if (previous === undefined) delete process.env.BUN_BINARY;
    else process.env.BUN_BINARY = previous;
    rmSync(directory, { recursive: true, force: true });
  }
});
