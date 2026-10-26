/** Identical host-provisioned bytes for the Rust and TS public-client runners. */
import { PreparedAgentCommand, type WasmStream } from "./client-query.js";

export interface ClientControlVector {
  readonly case: string;
  readonly command: readonly number[];
  readonly invocation: readonly number[];
  readonly expected: {readonly kind: "reply"; readonly bytes: readonly number[]}
    | {readonly kind: "actor_status"; readonly status: number};
}
export interface ClientControlFixture { readonly cases: readonly ClientControlVector[] }

const cases = ["root", "prompt", "cancel", "prompt_after_cancel", "spawn", "children_page",
  "approvals_page", "approval_decision", "typed_refusal", "unknown_custody"] as const;
type Outcome = {value: unknown} | {error: {name: string; request?: Uint8Array; status?: number}};
function bytes(value: readonly number[], maximum: number): Uint8Array {
  if (!Array.isArray(value) || value.length > maximum || value.some(n => !Number.isInteger(n) || n < 0 || n > 255)) {
    throw new Error("CLIENT_CONFORMANCE control_fixture: invalid bounded bytes");
  }
  return Uint8Array.from(value);
}
function failed(error: unknown): Outcome {
  if (!(error instanceof Error)) throw error;
  return {error: error as Error & {request?: Uint8Array; status?: number}};
}
function same(left: Outcome, right: Outcome): boolean {
  if ("value" in left && "value" in right) {
    const exact = (_: string, value: unknown): unknown => typeof value === "bigint" ? {$bigint: value.toString()} : value;
    return JSON.stringify(left.value, exact) === JSON.stringify(right.value, exact);
  }
  if (!("error" in left) || !("error" in right) || left.error.name !== right.error.name) return false;
  if (left.error.name === "agent_unknown") {
    const a = left.error.request, b = right.error.request;
    return a instanceof Uint8Array && b instanceof Uint8Array && a.length === b.length && a.every((n, i) => n === b[i]);
  }
  return left.error.status === right.error.status;
}

/** Executes vectors through the public WASM client. Transport interruption is
 * always a failure, never an expected result, retry, skip or zero-case pass. */
export async function verifyClientControls(stream: WasmStream, credential: number,
  fixture: ClientControlFixture): Promise<{passed: string[]}> {
  if (!Array.isArray(fixture.cases) || fixture.cases.length !== cases.length) {
    throw new Error("CLIENT_CONFORMANCE control_fixture: complete finite vector set required");
  }
  const prepared: {name: string; command: PreparedAgentCommand; invocation: Uint8Array; expected: Outcome}[] = [];
  const seen = new Set<string>();
  let retained = 0;
  try {
    for (const vector of fixture.cases) {
      if (vector.case !== cases[prepared.length] || seen.has(vector.case)
        || (vector.case === "prompt_after_cancel" && !seen.has("cancel"))) {
        throw new Error("CLIENT_CONFORMANCE control_fixture: invalid case/order");
      }
      seen.add(vector.case);
      const commandBytes = bytes(vector.command, PreparedAgentCommand.maximumCommandBytes());
      const invocation = bytes(vector.invocation, PreparedAgentCommand.maximumInvocationBytes());
      const encoded = vector.expected.kind === "reply" ? bytes(vector.expected.bytes, PreparedAgentCommand.maximumReplyBytes()) : undefined;
      retained += commandBytes.length + invocation.length + (encoded?.length ?? 0);
      if (retained > 8 * 1024 * 1024) throw new Error("CLIENT_CONFORMANCE control_fixture: byte capacity exceeded");
      const command = new PreparedAgentCommand(commandBytes);
      try {
        command.validateInvocation(invocation);
        let expected: Outcome;
        if (encoded) {
          try { expected = {value: command.reply(encoded)}; } catch (error) {
            expected = failed(error);
            if ("error" in expected && ["invalid_client_command", "client_command_capacity", "agent_exchange_interrupted"].includes(expected.error.name)) throw error;
          }
        } else {
          if (vector.expected.kind !== "actor_status" || vector.case !== "typed_refusal"
            || !Number.isInteger(vector.expected.status) || vector.expected.status < 400 || vector.expected.status > 599) {
            throw new Error("CLIENT_CONFORMANCE control_fixture: expected refusal status required");
          }
          expected = {error: {name: "actor_invocation_refused", status: vector.expected.status}};
        }
        if (vector.case === "unknown_custody" && (!("error" in expected) || expected.error.name !== "agent_unknown")) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: unknown case needs owned unknown outcome");
        }
        if (vector.case === "typed_refusal" && (!("error" in expected) || expected.error.name === "agent_unknown")) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: definitive refusal required");
        }
        if (!["unknown_custody", "typed_refusal"].includes(vector.case) && !("value" in expected)) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: successful result required");
        }
        if ("value" in expected) {
          const kinds: Record<string, readonly [string, string]> = {
            root: ["root", "root"], prompt: ["prompt", "delivered"], prompt_after_cancel: ["prompt", "delivered"],
            cancel: ["cancel", "delivered"], spawn: ["spawn", "spawned"], children_page: ["children", "children"],
            approvals_page: ["approvals", "approvals"], approval_decision: ["decide", "delivered"],
          };
          const kind = kinds[vector.case];
          const value = expected.value as {kind: string; delivery?: {kind: string}};
          if (!kind || command.value().kind !== kind[0] || value.kind !== kind[1]
            || (kind[1] === "delivered" && value.delivery?.kind !== "committed")) {
            throw new Error("CLIENT_CONFORMANCE control_fixture: case differs from its command/result oracle");
          }
        }
        prepared.push({name: vector.case, command, invocation, expected});
      } catch (error) { command.free(); throw error; }
    }
    const passed: string[] = [];
    for (const vector of prepared) {
      let actual: Outcome;
      try { actual = {value: await vector.command.execute(stream, vector.invocation, credential)}; }
      catch (error) { actual = failed(error); }
      if ("error" in actual && actual.error.name === "agent_exchange_interrupted") {
        throw new Error(`CLIENT_CONFORMANCE ${vector.name}: interrupted exchange; not retried`);
      }
      if (!same(vector.expected, actual)) throw new Error(`CLIENT_CONFORMANCE ${vector.name}: public result differs; not retried`);
      passed.push(vector.name);
    }
    return {passed};
  } finally { for (const vector of prepared) vector.command.free(); }
}
