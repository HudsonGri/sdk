import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../scripts/graphcoder-e2e/fixture-directory.mjs";

const bootstrap = fileURLToPath(new URL("../bin/install.js", import.meta.url));
const binary = process.env.ACYCLIC_TEST_BINARY;
const recovery = "set ACYCLIC_TEST_BINARY to the source-qualified shipping executable and run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/install-durable-rename.test.mjs";
assert.ok(binary && isAbsolute(binary), `step=admit native installer rename fixture; recovery=${recovery}`);

test("installer rename preserves occupied destinations without replacement in every mode", async () => {
  const processes = new Processes();
  const root = hostFixtureDirectory("installer-rename-");
  try {
    const home = join(root, "home"), temporary = join(root, "tmp");
    mkdirSync(home); mkdirSync(temporary);
    const env = { PATH: process.env.PATH ?? "", HOME: home, USERPROFILE: home,
      CODEX_HOME: join(home, "unused-codex-profile"), TMPDIR: temporary, TEMP: temporary, TMP: temporary,
      LANG: "C.UTF-8" };
    for (const key of ["SystemRoot", "ComSpec", "PATHEXT", "E2E_PROCESS_HOST"]) {
      if (process.env[key]) env[key] = process.env[key];
    }
    const call = "const [bootstrap,from,to,helper,replace]=process.argv.slice(1);require(bootstrap).durableRename(from,to,helper,replace==='replace');";
    for (const mode of ["test", "production"]) {
      // Internal file contents exercise publication semantics; they are not
      // fabricated output from an external tool or host.
      const from = join(root, `${mode} source with spaces`);
      const to = join(root, `${mode} destination with spaces`);
      writeFileSync(from, "new artifact"); writeFileSync(to, "occupied artifact");
      const run = replace => processes.runOwned(process.execPath,
        ["-e", call, bootstrap, from, to, binary, replace], { cwd: root, env: { ...env, NODE_ENV: mode } });
      const refused = await run("no-replace");
      assert.notEqual(refused.code, 0, `${mode}: must not replace occupied destination`);
      assert.match(refused.stderr, /step=durably rename installed artifact/);
      assert.ok(refused.stderr.includes(`recovery argv: ${JSON.stringify([binary, "__installer-rename", from, to, "no-replace"])}`), refused.stderr);
      assert.equal(readFileSync(from, "utf8"), "new artifact");
      assert.equal(readFileSync(to, "utf8"), "occupied artifact");
      const replaced = await run("replace");
      assert.equal(replaced.code, 0, replaced.stderr);
      assert.equal(readFileSync(to, "utf8"), "new artifact");
      assert.equal(existsSync(from), false);
      const created = join(root, `${mode} fresh destination`);
      const published = await processes.runOwned(process.execPath,
        ["-e", call, bootstrap, to, created, binary, "no-replace"], { cwd: root, env: { ...env, NODE_ENV: mode } });
      assert.equal(published.code, 0, published.stderr);
      assert.equal(readFileSync(created, "utf8"), "new artifact");
      assert.equal(existsSync(to), false);
    }
  } catch (cause) {
    throw new Error(`step=verify installer rename policy: ${cause.message}; recovery=${recovery}`, { cause });
  } finally {
    try {
      await processes.close();
      rmSync(root, { recursive: true, force: true });
    } catch (cause) {
      throw new Error(`step=drain installer rename processes and retire fixture: ${cause.message}; recovery=${recovery}`, { cause });
    }
  }
});
