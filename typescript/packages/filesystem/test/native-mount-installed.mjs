import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const installed = process.env.FS_INSTALLED_PACKAGE_ROOT ?? process.argv[2];
assert.ok(installed, "pass the installed @acyclic-labs/fs package root");
const native = await import(pathToFileURL(join(resolve(installed), "dist/native.js")).href);
const child = promisify(execFile);
const read = (path) => child(process.execPath, ["-e",
  "process.stdout.write(require('node:fs').readFileSync(process.argv[1]))", path,
], { windowsHide: true }).then(({ stdout }) => stdout);
const write = (path, value) => child(process.execPath, ["-e",
  "require('node:fs').writeFileSync(process.argv[1],process.argv[2])", path, value,
], { windowsHide: true });
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
      assert.equal(await read(join(destination, "content")), "original");
      if (writable) {
        await write(join(destination, "content"), "updated");
        await mount.sync();
        assert.equal(new TextDecoder().decode(await workspace.read("/content", 64n)), "updated");
      } else {
        await assert.rejects(write(join(destination, "content"), "forbidden"));
      }
    } finally {
      assert.equal(await mount.unmount(), true);
    }
  }
} finally {
  engine.cancel();
  await rm(root, { recursive: true, force: true });
}
