import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { nodeExecutable, pinnedPiCli } from "./pi-fixture.ts";
import type { installedNativeFixture } from "./pi-native-fixture.ts";

// Use the pinned CLI's own LF framing, including its Unicode-separator rules.
const { attachJsonlLineReader, serializeJsonLine } = await import(fileURLToPath(new URL(
  "node_modules/@earendil-works/pi-coding-agent/dist/modes/rpc/jsonl.js", import.meta.url,
))) as {
  attachJsonlLineReader(stream: Readable, accept: (line: string) => void): () => void;
  serializeJsonLine(value: unknown): string;
};

/** One real, persistent native CLI. Readiness and shutdown are both push waits. */
export function nativePiSession(fixture: Awaited<ReturnType<typeof installedNativeFixture>>) {
  const process = spawn(nodeExecutable, [pinnedPiCli,
    "--mode", "rpc", "--session", fixture.session,
    "--provider", "acyclic-native-qualification", "--model", "probe",
    "--no-extensions", "--no-mcp", "--no-builtin-tools",
    "-e", fixture.extension, "-e", fixture.provider,
  ], { cwd: fixture.project, env: fixture.environment, windowsHide: true, stdio: "pipe" });
  const ready = Promise.withResolvers<void>();
  const closed = Promise.withResolvers<void>();
  let closing = false;
  let stderr = "";
  let turn: {
    accepted: ReturnType<typeof Promise.withResolvers<void>>;
    settled: ReturnType<typeof Promise.withResolvers<string>>;
    text?: string;
  } | undefined;
  const fail = (error: Error) => {
    ready.reject(error);
    turn?.accepted.reject(error);
    turn?.settled.reject(error);
  };
  // Failure may precede the caller awaiting readiness or close.
  void ready.promise.catch(() => {});
  void closed.promise.catch(() => {});
  process.stderr.on("data", chunk => { stderr += chunk; });
  const detach = attachJsonlLineReader(process.stdout, line => {
    try {
      const record = JSON.parse(line);
      if (record.type === "extension_error") throw new Error(JSON.stringify(record));
      if (record.type === "response") {
        if (!record.success) throw new Error(JSON.stringify(record));
        if (record.id === "ready") {
          assert.equal(record.data.sessionId, fixture.manager.getSessionId());
          assert.equal(record.data.isStreaming, false);
          ready.resolve();
        } else if (record.id === "turn") {
          assert.equal(record.data.disposition, "started");
          turn?.accepted.resolve();
        }
      } else if (record.type === "message_end" && record.message.role === "assistant") {
        if (record.message.errorMessage) throw new Error(record.message.errorMessage);
        if (turn) turn.text = record.message.content.filter((part: {type:string}) => part.type === "text")
          .map((part: {text:string}) => part.text).join("");
      } else if (record.type === "agent_settled") {
        assert.ok(turn?.text, "the actual native turn ended without an assistant answer");
        turn.settled.resolve(turn.text);
      }
    } catch (error) { fail(error instanceof Error ? error : new Error(String(error))); }
  });
  process.once("error", error => { fail(error); closed.reject(error); });
  process.once("close", (code, signal) => {
    detach();
    const error = new Error(`native Pi exited ${code}/${signal}: ${stderr}`);
    fail(error);
    if (closing && code === 0) closed.resolve();
    else closed.reject(error);
  });
  const send = (record: unknown) => process.stdin.write(serializeJsonLine(record), error => {
    if (error) fail(error);
  });
  send({id:"ready",type:"get_state"});
  return {
    pid: process.pid,
    ready: ready.promise,
    async run(message: string) {
      await ready.promise;
      assert.equal(turn, undefined, "each qualification scenario owns one original native turn");
      turn = {accepted:Promise.withResolvers<void>(),settled:Promise.withResolvers<string>()};
      const completed = Promise.all([turn.accepted.promise,turn.settled.promise]);
      send({id:"turn",type:"prompt",message});
      const [,stdout] = await completed;
      return {stdout,stderr};
    },
    close() {
      if (!closing) { closing = true; process.stdin.end(); }
      return closed.promise;
    },
  };
}
