import { SseDecoder } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";

export interface SseFrame { readonly id: string; readonly event: string; readonly data: string }

/** Thin host adapter over the shared Rust SSE decoder. */
export async function createSseDecoder(maximum: number) {
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("invalid SSE byte bound");
  await ensureHarnessWasm();
  const decoder = new SseDecoder(maximum);
  return {
    push(chunk: Uint8Array) { decoder.push(chunk); },
    next(): SseFrame | undefined { return decoder.nextFrame() as SseFrame | undefined; },
    finish(): SseFrame | undefined { return decoder.finish() as SseFrame | undefined; },
    free() { decoder.free(); },
  };
}
