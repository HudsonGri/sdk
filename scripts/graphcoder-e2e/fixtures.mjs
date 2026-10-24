import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";

// Fixture disk latency must not block the process/event owner or serialize
// independent files before the concurrent cases have even started.
export async function seed(root, files = {}, io = { mkdir, writeFile }) {
  await Promise.all(Object.entries(files).map(async ([path, value]) => {
    assert.ok(!isAbsolute(path) && !path.split(/[\\/]/).includes(".."), `unsafe seed path ${path}`);
    const target = join(root, path);
    await io.mkdir(dirname(target), { recursive: true });
    let bytes = value;
    if (typeof value !== "string") {
      if (value.repeat !== undefined) {
        assert.ok(Number.isSafeInteger(value.count) && value.count >= 0 && value.count <= 4 * 1024 * 1024,
          "fixture repetition requires a bounded byte count");
        assert.ok(typeof value.repeat === "string" && Buffer.byteLength(value.repeat) === 1,
          "fixture repetition requires one byte");
        bytes = Buffer.alloc(value.count, value.repeat);
      } else bytes = Buffer.from(value.base64, "base64");
    }
    await io.writeFile(target, bytes);
  }));
}
