/** Browser host binding; fetch, cancellation and redirect policy live in Rust. */
import { startBrowserHttpExchange } from "./http.js";
import type { McpBrowserHttpProvider } from "../generated/wasm/acyclic_harness_wasm.js";

/** Supplies host-selected HTTP I/O for journal-admitted MCP operations. */
export function createBrowserMcpHttpProvider(fetcher: typeof fetch = globalThis.fetch,
  signal?: AbortSignal): McpBrowserHttpProvider {
  return (_operation, request) => startBrowserHttpExchange(fetcher, {
    method: request.method, url: request.endpoint, headers: request.headers,
    body: new Uint8Array(request.body), timeoutMs: request.timeout_ms,
  }, signal);
}
