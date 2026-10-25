import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

export function alive(pid) {
  assert.ok(Number.isSafeInteger(pid) && pid > 1 && pid !== process.pid, "invalid fixture process identity");
  try { process.kill(pid, 0); } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
  // An unreaped Linux zombie cannot act. It must not hide a live descendant,
  // which is checked independently using its own committed process identity.
  if (process.platform === "linux") {
    try {
      const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
      return stat.slice(stat.lastIndexOf(")") + 2, stat.lastIndexOf(")") + 3) !== "Z";
    } catch (error) { if (error.code === "ENOENT") return false; throw error; }
  }
  if (process.platform === "darwin") {
    // launchd can retain an exited generation as a zombie. Read the kernel's
    // process state once; an unexpected inspection error fails the oracle.
    const observed = spawnSync("/bin/ps", ["-p", String(pid), "-o", "stat="], { encoding: "utf8" });
    if (observed.error) throw observed.error;
    if (observed.status === 1 && !observed.stdout.trim() && !observed.stderr.trim()) return false;
    assert.equal(observed.status, 0, observed.stderr);
    const state = observed.stdout.trim();
    assert.ok(state, "missing process state");
    return !state.startsWith("Z");
  }
  return true;
}

export function fenced(records, path) {
  const probes = records.filter(record => record.path === path);
  assert.equal(probes.length, 1, "orphan kill barrier must be committed exactly once");
  const pids = [probes[0].body.pid, probes[0].body.parent_pid];
  assert.equal(new Set(pids).size, 2, "probe must be a real descendant tree");
  try {
    for (const pid of pids) assert.equal(alive(pid), false, `native tool process ${pid} survived owner recovery`);
  } finally {
    // These identities came from this suite's loopback fixture. Reap any
    // survivor even on a failing negative control, leaving no lane-owned work.
    for (const pid of pids) if (Number.isSafeInteger(pid) && pid > 1 && pid !== process.pid) {
      try { process.kill(pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
    }
  }
}
