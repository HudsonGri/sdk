import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";

const require = createRequire(import.meta.url);

// The maintained CommonJS entry bundles its dependencies. All producers and
// declaration generators must load the same entry whose bytes we attest.
export function napiCli() {
  return require("@napi-rs/cli");
}

export async function napiGeneratorIdentity(root) {
  napiCli();
  const packageBytes = await readFile(require.resolve("@napi-rs/cli/package.json"));
  const entryBytes = await readFile(require.resolve("@napi-rs/cli"));
  const manifest = JSON.parse(packageBytes.toString("utf8"));
  if (typeof manifest.version !== "string" || manifest.version.length === 0) {
    throw new Error("@napi-rs/cli package version is unavailable");
  }
  const digest = bytes => `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  return {
    package: "@napi-rs/cli",
    version: manifest.version,
    package_sha256: digest(packageBytes),
    entry_sha256: digest(entryBytes),
    lock_sha256: digest(await readFile(resolve(root, "bun.lock"))),
  };
}
