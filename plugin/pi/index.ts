import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { spawn } from "node:child_process";

// Installation substitutes one JSON string; no shell interprets it.
const executable = "__ACYCLIC_EXECUTABLE__";
const maximum = 64 * 1024;
// Canonical value once, its escaped text (at most twice), plus native fields.
const maximumAnswer = 3 * maximum + 1024;
type Answer = Record<string, unknown>;
export type Invoke = (event: string, input: Answer, signal?: AbortSignal) => Promise<Answer>;

/** The common Rust client owns normalization, authentication, append and follow. */
const invokeProcess: Invoke = (event, input) => new Promise((resolve, reject) => {
  const child = spawn(executable, event === "catalog" ? ["__catalog"] : ["__hook", "pi", event], { windowsHide: true, stdio: "pipe" });
  const output: Buffer[] = [];
  let bytes = 0;
  child.on("error", reject);
  child.stdin.on("error", reject);
  child.stdout.on("data", (chunk: Buffer) => {
    bytes += chunk.length;
    if (bytes > maximumAnswer) { child.kill(); reject(new Error("Harness hook answer exceeds its bound")); }
    else output.push(chunk);
  });
  // Diagnostics go to the native host; task output comes only from followed files.
  child.stderr.on("data", (chunk: Buffer) => process.stderr.write(chunk));
  child.on("close", code => {
    try {
      const answer: unknown = JSON.parse(Buffer.concat(output).toString("utf8"));
      if (!answer || typeof answer !== "object" || Array.isArray(answer)) throw new Error("Invalid harness hook answer");
      if (code !== 0 && !("block" in answer && answer.block === true)) throw new Error("Harness hook failed");
      resolve(answer as Answer);
    } catch (error) { reject(error); }
  });
  if (event === "catalog") { child.stdin.end(); return; }
  const payload = JSON.stringify(input);
  if (Buffer.byteLength(payload) > maximum) { child.kill(); reject(new Error("Native hook exceeds its bound")); }
  else child.stdin.end(payload);
});

/** Abort writes a cancellation file; the original process stays until settlement. */
export const invoke: Invoke = async (event, input, signal) => {
  if (signal?.aborted) throw new DOMException("Harness tool aborted before execution", "AbortError");
  type Reply = { answer: Answer } | { error: unknown };
  const observe = (pending: Promise<Answer>): Promise<Reply> => pending.then(
    answer => ({ answer }), error => ({ error }),
  );
  let cancellation: Promise<Reply> | undefined;
  const abort = () => {
    const { sessionId, cwd, toolCallId, agentId, threadId, turnId } = input;
    cancellation ??= observe(invokeProcess("tool_cancel", { sessionId, cwd, toolCallId, agentId, threadId, turnId }));
  };
  const original = observe(invokeProcess(event, input));
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  const reply = await original;
  signal?.removeEventListener("abort", abort);
  const cancelled = await cancellation;
  if (cancelled && "error" in cancelled) throw cancelled.error;
  if ("error" in reply) throw reply.error;
  if (signal?.aborted) throw new DOMException("Harness tool aborted after settlement", "AbortError");
  return reply.answer;
};

function identity(ctx: ExtensionContext) {
  return { sessionId: ctx.sessionManager.getSessionId(), cwd: ctx.cwd };
}

/** Tool definitions and execution both come from the harness registry. */
export default function extension(pi: ExtensionAPI, send: Invoke = invoke) {
  const registered = new Set<string>();
  pi.on("session_start", async (_event, ctx) => {
    await send("session_start", identity(ctx));
    const catalog = await send("catalog", {});
    if (!Array.isArray(catalog.tools)) throw new Error("Invalid harness tool catalog");
    for (const tool of catalog.tools) {
      if (!tool || typeof tool.name !== "string" || typeof tool.description !== "string" ||
          !tool.input_schema || typeof tool.input_schema !== "object") throw new Error("Invalid harness tool definition");
      pi.registerTool({
        name: tool.name, label: tool.name, description: tool.description,
        parameters: tool.input_schema as ToolDefinition["parameters"],
        renderCall: args => new Text(tool.name + " " + JSON.stringify(args), 0, 0),
        renderResult: result => new Text(result.content.map(block => block.type === "text" ? block.text : "").join("\n"), 0, 0),
        async execute(toolCallId, input, signal, _onUpdate, context) {
          const result = await send("tool_call", { toolCallId, toolName: tool.name, input, ...identity(context) }, signal);
          if (!Array.isArray(result.content) || result.isError !== false) throw new Error("Invalid harness tool result");
          return { content: result.content, details: result.details };
        },
      });
      registered.add(tool.name);
    }
  });
  pi.on("tool_call", async (event, ctx) => {
    if (!registered.has(event.toolName)) {
      const answer = await send("tool_call", { ...event, ...identity(ctx), hostExecuted: true });
      if (!answer || typeof answer !== "object" || Array.isArray(answer) ||
          typeof answer.block !== "boolean" ||
          (answer.reason !== undefined && typeof answer.reason !== "string")) {
        throw new Error("Invalid harness native-tool decision");
      }
      if (answer.block) return { block: true, reason: answer.reason as string | undefined };
    }
    return undefined;
  });
  pi.on("tool_result", async (event, ctx) => { await send("tool_result", { ...event, ...identity(ctx) }); });
  pi.on("session_shutdown", async (_event, ctx) => { await send("session_shutdown", identity(ctx)); });
}
