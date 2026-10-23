import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

// Each sequential Windows suite owns its normal deadline and process scope.
// The matrix only schedules suites; time spent in one never consumes another's.
export async function windowsMatrix(cases, binary, mock, output, entrypoint) {
  const suites = [];
  for (const test of cases) {
    const destination = join(output, test.id);
    const code = await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [entrypoint, binary, mock], {
        windowsHide: true, stdio: "inherit",
        env: { ...process.env, E2E_OUTPUT: destination, E2E_CASE_INSTANCE: test.id },
      });
      child.on("error", reject);
      child.on("close", resolve);
    });
    const suite = JSON.parse(await readFile(join(destination, "matrix.json"), "utf8"));
    assert.equal(suite.results.length, 1);
    suites.push({ ...suite, exit: code });
  }
  const results = suites.flatMap(suite => suite.results);
  const report = { platform: process.platform, limit_seconds: 10, suites, results, output };
  await writeFile(join(output, "matrix.json"), JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify(report, null, 2));
  return suites.every(suite => suite.exit === 0) ? 0 : 1;
}
