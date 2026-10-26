/**
 * Internal custody of the original FileClient recovery/publication oracles.
 * These bindings are deliberately absent from the product client API. Keep the
 * existing tests until the actor host supplies their durable submission seam;
 * actor-record append fixtures do not qualify that behavior.
 *
 * This file only names the existing Rust implementation. It adds no transport,
 * adapter state, persistence, retry, queue or alternative client loop.
 */
export {
  WasmFileClient as FileClient,
  WasmClientQuery as ClientQuery,
  WasmTaskVolume as TaskVolume,
  type FileClientLimits,
  type FileKey,
  type Cursor,
  type QueryDelivery,
} from "../generated/wasm/acyclic_harness_wasm.js";
