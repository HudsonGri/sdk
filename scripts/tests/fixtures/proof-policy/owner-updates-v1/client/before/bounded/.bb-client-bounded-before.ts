import { PageWindow } from "./typescript/packages/harness/src/pagination.ts";
const rows = [];
for (const count of [100, 1_000, 10_000]) {
  const items = Array.from({ length: count }, (_, id) => ({ id }));
  const window = new PageWindow(item => String(item.id), async () => ({
    generation: "one", items: [], hasMoreBefore: false, hasMoreAfter: false,
  }), count);
  window.reset({ generation: "one", items, hasMoreBefore: false, hasMoreAfter: false });
  let previous = window.items;
  let arrays = 0;
  let residentReferences = 0;
  const start = performance.now();
  for (let read = 0; read < 100; read++) {
    const next = window.items;
    if (next !== previous) { arrays++; residentReferences += next.length; }
    previous = next;
  }
  rows.push({ count, reads: 100, arrays, residentReferences, milliseconds: performance.now() - start });
}
const source = await Bun.file("typescript/packages/harness/src/pagination.ts").arrayBuffer();
const report = { bun: Bun.version, source_sha256: new Bun.CryptoHasher("sha256").update(source).digest("hex"), rows };
await Bun.write(".bb-client-bounded-before-records/pagination-metrics.json", JSON.stringify(report, null, 2));
console.log(JSON.stringify(report));
