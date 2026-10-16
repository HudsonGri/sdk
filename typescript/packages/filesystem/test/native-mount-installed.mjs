import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const installed = process.env.FS_INSTALLED_PACKAGE_ROOT ?? process.argv[2];
assert.ok(installed, "pass the installed @acyclic-labs/fs package root");
const native = await import(pathToFileURL(join(resolve(installed), "dist/native.js")).href);
const root = await mkdtemp(join(tmpdir(), "acyclic-authorized-napi-mount-"));
const engine = await native.openNativeFs({
  root: join(root, "engine"), objectCache: { ...native.DEFAULT_OBJECT_CACHE_OPTIONS },
});
try {
  const workspace = await engine.createWorkspace("authenticated-mount");
  await workspace.write("/content", new TextEncoder().encode("original"));
  for (const writable of [false, true]) {
    const destination = await mkdtemp(join(root, writable ? "write-" : "read-"));
    const mount = await workspace.mount(destination, {
      writable, subdirectory: "/", publication: "close-and-sync",
    });
    try {
      assert.equal(await readFile(join(destination, "content"), "utf8"), "original");
      if (writable) {
        await writeFile(join(destination, "content"), "updated");
        await mount.sync();
        assert.equal(new TextDecoder().decode(await workspace.read("/content", 64n)), "updated");
      } else {
        await assert.rejects(writeFile(join(destination, "content"), "forbidden"));
      }
    } finally {
      assert.equal(await mount.unmount(), true);
    }
  }
} finally {
  engine.cancel();
  await rm(root, { recursive: true, force: true });
}
