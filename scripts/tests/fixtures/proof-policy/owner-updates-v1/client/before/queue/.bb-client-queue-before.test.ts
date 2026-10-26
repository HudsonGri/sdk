import { expect, test } from "bun:test";
import { SessionQueue } from "./typescript/packages/stream/src/session-queue.ts";

for (const capacity of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 1.5]) {
  test(`queue rejects invalid capacity ${capacity} before admission`, () => {
    expect(() => new SessionQueue(capacity)).toThrow();
  });
}
