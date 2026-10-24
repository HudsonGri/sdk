/** Browser host binding; fetch, cancellation and redirect policy live in Rust. */
import { browserMcpHttpExchange } from "../generated/wasm/acyclic_harness_wasm.js";
import type { McpBrowserHttpProvider, McpBrowserExchange } from "../generated/wasm/acyclic_harness_wasm.js";

/** Supplies host-selected HTTP I/O for journal-admitted MCP operations. */
export function createBrowserMcpHttpProvider(fetcher: typeof fetch = globalThis.fetch,
  signal?: AbortSignal): McpBrowserHttpProvider {
  return (_operation, request) => browserMcpHttpExchange(fetcher, request, signal) as McpBrowserExchange;
}
