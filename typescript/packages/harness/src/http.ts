import { browserHttpExchange, SseDecoder } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";

export interface SseFrame { readonly id: string; readonly event: string; readonly data: string }

export interface BrowserHttpHead { readonly status: number; readonly headers: Record<string, string> }
export interface BrowserHttpExchange {
  readonly response: Promise<BrowserHttpHead>;
  read(): Promise<Uint8Array | null>;
  /** Inspect after the response head, before reading the body. */
  hasBody(): boolean;
  cancel(): void;
}
export interface BrowserHttpRequest {
  readonly method: string;
  readonly url: string;
  readonly headers: ReadonlyMap<string, string> | Record<string, string>;
  readonly body: Uint8Array | Blob;
  readonly timeoutMs?: number;
}

/** Synchronous binding for hosts which have already initialized the shared WASM. */
export function startBrowserHttpExchange(fetcher: typeof fetch, request: BrowserHttpRequest,
  signal?: AbortSignal): BrowserHttpExchange {
  const exchange = browserHttpExchange(fetcher, request.method, request.url, request.headers,
    request.body, request.timeoutMs, signal) as BrowserHttpExchange;
  return {
    response: exchange.response.catch(error => {
      signal?.throwIfAborted();
      throw new TypeError("HTTP request failed", { cause: error });
    }),
    async read() {
      try { return await exchange.read(); }
      catch (error) { signal?.throwIfAborted(); throw error; }
    },
    hasBody() { return exchange.hasBody(); },
    cancel() { exchange.cancel(); },
  };
}

/** Initializes the shared instance before starting the same exchange. */
export async function createBrowserHttpExchange(fetcher: typeof fetch, request: BrowserHttpRequest,
  signal?: AbortSignal): Promise<BrowserHttpExchange> {
  await ensureHarnessWasm();
  return startBrowserHttpExchange(fetcher, request, signal);
}

/** Response-shaped host adapter; reader and cancellation ownership stay in Rust. */
export function browserHttpResponse(exchange: BrowserHttpExchange, head: BrowserHttpHead): Response {
  try {
    if (!exchange.hasBody()) {
      exchange.cancel();
      return new Response(null, head);
    }
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const chunk = await exchange.read();
          if (chunk === null) { controller.close(); exchange.cancel(); }
          else controller.enqueue(chunk);
        } catch (error) { exchange.cancel(); controller.error(error); }
      },
      cancel() { exchange.cancel(); },
    }, { highWaterMark: 0 });
    return new Response(body, head);
  } catch (error) { exchange.cancel(); throw error; }
}

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
