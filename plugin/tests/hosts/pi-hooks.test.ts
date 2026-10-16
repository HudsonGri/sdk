import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "bun:test";
import fc from "fast-check";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { Invoke } from "../../pi/index.ts";

// Pi supplies these modules to installed extensions through its loader. Resolve
// the real pinned TUI here as well; the source shim lives outside this fixture.
const require = createRequire(import.meta.url);
const { createJiti } = require("jiti");
const extension: typeof import("../../pi/index.ts").default = await createJiti(import.meta.url, {
  tryNative: false,
  alias: { "@earendil-works/pi-tui": require.resolve("@earendil-works/pi-tui") },
}).import("../../pi/index.ts", { default: true });

type Handler = (event: Record<string, unknown>, context: ExtensionContext) => Promise<unknown>;
const definitions = ["bash", "fs", "read", "write", "edit"].map(name => ({
  name, description: `Harness ${name}`, input_schema: { type: "object", properties: {} },
}));
function setup(send: Invoke, initial: string[] = ["foreign-mcp"]) {
  const handlers = new Map<string, Handler>();
  const tools = new Map<string, ToolDefinition>();
  let active = [...initial];
  const api = {
    on(event: string, handler: Handler) { assert.ok(!handlers.has(event)); handlers.set(event, handler); },
    registerTool(tool: ToolDefinition) { tools.set(tool.name, tool); },
    getActiveTools() { return [...active]; },
    setActiveTools(names: string[]) { active = [...names]; },
  } as unknown as ExtensionAPI;
  extension(api, send);
  const context = {
    cwd: "/selected-project", sessionManager: { getSessionId: () => "native-session" },
  } as unknown as ExtensionContext;
  return { handlers, tools, context, active: () => active };
}

test("Pi executes every registered tool exactly once through the harness", async () => {
  const calls: { event: string; input: Record<string, unknown> }[] = [];
  const result = { content: [{type: "text", text: "harness result"}], details: {published:true}, isError:false };
  const {handlers,tools,context,active} = setup(async (event,input) => {
    calls.push({event,input});
    return event === "catalog" ? {tools:definitions} : event === "tool_call" ? input.hostExecuted ? {block:false} : result : {};
  });
  await handlers.get("session_start")!({},context);
  assert.deepEqual([...tools.keys()],definitions.map(tool=>tool.name));
  assert.deepEqual(active(),["foreign-mcp",...definitions.map(tool=>tool.name)]);
  for (const [name,tool] of tools) {
    assert.deepEqual(tool.parameters,definitions.find(definition=>definition.name===name)!.input_schema);
    assert.equal(typeof tool.renderCall,"function");
    assert.equal(typeof tool.renderResult,"function");
    const count = calls.length;
    assert.equal(await handlers.get("tool_call")!({toolName:name},context),undefined);
    assert.equal(calls.length,count,"permission hook must not execute a tool");
    const answer = await tool.execute("call",{},undefined,undefined,context);
    assert.deepEqual(answer,{content:result.content,details:result.details});
    assert.deepEqual(calls.at(-1),{event:"tool_call",input:{toolCallId:"call",toolName:name,input:{},sessionId:"native-session",cwd:"/selected-project"}});
    assert.equal(calls.length,count+1);
  }
  const foreign = {toolName:"foreign-mcp",toolCallId:"host-call",input:{query:"native"}};
  assert.equal(await handlers.get("tool_call")!(foreign,context),undefined);
  assert.deepEqual(calls.at(-1),{event:"tool_call",input:{...foreign,sessionId:"native-session",cwd:"/selected-project",hostExecuted:true}});
  const completion = {...foreign,content:[{type:"text",text:"native result"}],isError:false};
  await handlers.get("tool_result")!(completion,context);
  assert.deepEqual(calls.at(-1),{event:"tool_result",input:{...completion,sessionId:"native-session",cwd:"/selected-project"}});
  await handlers.get("session_shutdown")!({},context);
  assert.equal(calls.at(-1)!.event,"session_shutdown");
});

test("session activation preserves every additive tool and enables every harness tool",async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.string({maxLength:16}),{maxLength:16}),async initial => {
    const fixture=setup(async event=>event === "catalog" ? {tools:definitions} : {},initial);
    await fixture.handlers.get("session_start")!({},fixture.context);
    const active=fixture.active();
    for (const name of [...initial,...definitions.map(tool=>tool.name)]) assert.ok(active.includes(name));
    assert.equal(new Set(active).size,active.length);
    for (const name of active) assert.ok(initial.includes(name) || definitions.some(tool=>tool.name === name));
  }),{numRuns:128});
});

test("Pi cannot fall back to a built-in after a missing or failed harness result", async () => {
  for (const answer of [{block:true,reason:"rejected"},{content:[],isError:true},{}]) {
    const {handlers,tools,context} = setup(async event => event === "catalog" ? {tools:definitions} : answer);
    await handlers.get("session_start")!({},context);
    await assert.rejects(()=>tools.get("bash")!.execute("call",{},undefined,undefined,context));
  }
});

test("Pi preserves foreign-tool refusals and rejects malformed decisions", async () => {
  const foreign = {toolName:"foreign-mcp",toolCallId:"host-call",input:{}};
  const denied = setup(async () => ({block:true,reason:"call was not recorded"}));
  assert.deepEqual(await denied.handlers.get("tool_call")!(foreign,denied.context),
    {block:true,reason:"call was not recorded"});
  for (const answer of [null,[],{},false,{block:"false"},{block:false,reason:1},{isError:true}]) {
    const fixture = setup(async () => answer as never);
    await assert.rejects(() => fixture.handlers.get("tool_call")!(foreign,fixture.context),
      /Invalid harness native-tool decision/);
  }
});

test("only an explicit permission can release a foreign tool after a JSON round trip", async () => {
  await fc.assert(fc.asyncProperty(fc.jsonValue(),async value => {
    const answer = JSON.parse(JSON.stringify(value));
    const fixture = setup(async () => answer);
    let result: unknown;
    try { result = await fixture.handlers.get("tool_call")!({toolName:"foreign-mcp"},fixture.context); }
    catch { return; }
    if (result === undefined) assert.equal(answer.block,false);
    else assert.equal((result as {block:unknown}).block,true);
  }),{numRuns:256});
  await fc.assert(fc.asyncProperty(fc.boolean(),fc.string(),async (block,reason) => {
    const fixture = setup(async () => JSON.parse(JSON.stringify({block,reason})));
    assert.deepEqual(await fixture.handlers.get("tool_call")!({toolName:"foreign-mcp"},fixture.context),
      block ? {block:true,reason} : undefined);
  }),{numRuns:256});
});
