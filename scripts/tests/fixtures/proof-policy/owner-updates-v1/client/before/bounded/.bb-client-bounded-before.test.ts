import { expect, test } from "bun:test";
import { PageWindow } from "./typescript/packages/harness/src/pagination.ts";

const all = ["a", "b", "c", "d", "e"];
const page = (items: readonly string[]) => ({
  generation: "one", items, before: items[0], after: items.at(-1),
  hasMoreBefore: items[0] !== all[0], hasMoreAfter: items.at(-1) !== all.at(-1),
});
test("exclusive reverse load recovers the evicted neighbor without a hole", async () => {
  const requests: unknown[] = [];
  const window = new PageWindow<string, string>(item => item, async (direction, cursor) => {
    requests.push({ direction, cursor });
    const anchor = all.indexOf(cursor!);
    const index = direction === "before" ? anchor - 1 : anchor + 1;
    return page(index < 0 || index >= all.length ? [] : [all[index]!]);
  }, 2);
  window.reset(page(["b", "c"]));
  await window.load("after");
  expect(window.items).toEqual(["c", "d"]);
  await window.load("before");
  console.log(JSON.stringify({ requests, actual: window.items, expected: ["b", "c"] }));
  expect(window.items).toEqual(["b", "c"]);
});
test("reading an unchanged resident window retains its array identity", () => {
  const window = new PageWindow<string, string>(item => item, async () => page([]), 3);
  window.reset(page(["a", "b", "c"]));
  expect(window.items).toBe(window.items);
});
