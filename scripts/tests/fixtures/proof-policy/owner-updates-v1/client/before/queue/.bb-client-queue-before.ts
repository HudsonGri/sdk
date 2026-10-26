import { SessionQueue } from "./typescript/packages/stream/src/session-queue.ts";

const rows = [];
for (const count of [1_000, 10_000, 100_000]) {
  Bun.gc(true);
  const before = process.memoryUsage();
  const queue = new SessionQueue<Uint8Array>(1);
  await queue.send(new Uint8Array(1024));
  let pending = 0;
  const started = performance.now();
  const writes = Array.from({ length: count }, () => {
    pending++;
    return queue.send(new Uint8Array(1024)).then(() => { pending--; }, () => { pending--; });
  });
  await Promise.resolve();
  Bun.gc(true);
  const retained = process.memoryUsage();
  rows.push({ count, capacity: 1, pending, milliseconds: performance.now() - started,
    heap_delta: retained.heapUsed - before.heapUsed,
    array_buffer_delta: retained.arrayBuffers - before.arrayBuffers,
    rss_delta: retained.rss - before.rss });
  queue.end(new Error("measurement owner finished"));
  await Promise.all(writes);
  if (pending !== 0) throw new Error("measurement writers did not settle");
}
const report = { bun: Bun.version, rows };
await Bun.write(".bb-client-queue-before-records/metrics.json", JSON.stringify(report, null, 2));
console.log(JSON.stringify(report));
