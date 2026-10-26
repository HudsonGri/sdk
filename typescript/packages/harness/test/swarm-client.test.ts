import { expect, test } from "bun:test";
import { AgentClient, WasmStream, initializeClientQueries } from "../src/client.js";
import { exerciseAgents, root } from "./swarm-client.scenario.js";
import { bindLocalSession } from "./session-authority.js";

test("native and browser agent views use the same bounded Rust session", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  try { expect(await exerciseAgents(stream)).toEqual({ selected: 2, liveRecords: 2, recovered: false }); }
  finally { stream.free(); }
});

test("agent view admission refuses oversized and malformed input atomically", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  const client = new AgentClient(stream, root, 0n, 0n, 0);
  try {
    for (const agents of [[], Array(9).fill(root), ["outside/agent"]]) {
      expect(() => client.setView(agents, new BigUint64Array(agents.length), new BigUint64Array(agents.length))).toThrow();
      expect(client.selectedAgents()).toEqual([root]);
    }
    expect(() => client.setView([root], new BigUint64Array(0), new BigUint64Array(1))).toThrow();
    expect(client.selectedAgents()).toEqual([root]);
  } finally { client.close(); client.free(); stream.free(); }
});
