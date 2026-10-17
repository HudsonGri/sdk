import assert from "node:assert/strict";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { fileURLToPath } from "node:url";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { invokePi } from "./pi-fixture.ts";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider = `
import { fauxProvider, fauxAssistantMessage, getCurrentTools } from "@earendil-works/pi-ai";
export default function (pi) {
  const faux = fauxProvider({ provider: "acyclic-qualification", models: [{ id: "probe" }] });
  faux.setResponses([context => fauxAssistantMessage(JSON.stringify({
    messages: context.messages,
    tools: getCurrentTools(context.messages).map(tool => tool.name).sort(),
  }))]);
  pi.registerProvider(faux.provider);
}
`;

function probeArguments(session: string, extension: string) {
  return [
    "--print", "--mode", "text", "--session", session,
    "--provider", "acyclic-qualification", "--model", "probe",
    "--no-extensions", "--no-mcp", "--no-builtin-tools", "-e", extension,
  ];
}

const nativeProvider = `
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { writeFileSync } from "node:fs";
export default function(pi) {
  let session;
  pi.on("session_start", (_event, ctx) => { session = ctx.sessionManager; });
  const faux = fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  const result = context => {
    const answer = context.messages.filter(message => message.role === "toolResult").at(-1);
    if (!answer || answer.isError) throw new Error("shared native context command failed: " + JSON.stringify(answer));
    return JSON.parse(answer.content.filter(part => part.type === "text").map(part => part.text).join(""));
  };
  const call = (name, input, id) => fauxAssistantMessage(fauxToolCall(name,input,{id}),{stopReason:"toolUse"});
  faux.setResponses(process.env.ACYCLIC_HARNESS_BINDING ? [context => {
    writeFileSync(process.env.ACYCLIC_COMPACT_CHILD, JSON.stringify({
      native_session_id:session.getSessionId(), session_file:session.getSessionFile(), messages:context.messages,
    }), {flag:"wx"});
    return fauxAssistantMessage("compacted child ready");
  }] : [
    () => call("acyclic_spawn",{id:"compacted-child",context:"native_cut",prompt:"Check inherited compacted context."},"spawn-after-compaction"),
    context => call("acyclic_wait",{child:result(context)},"wait-compacted-child"),
    context => {
      const outcome = result(context);
      if (outcome.success !== true) throw new Error("compacted child did not settle: " + JSON.stringify(outcome));
      return fauxAssistantMessage("compaction child completed");
    },
  ]);
  pi.registerProvider(faux.provider);
}
`;

test("a child consumes the native parent's completed compaction across reopen", async () => {
  const started=performance.now();
  const phase=(name:string)=>console.error(`compaction ${name}: ${(performance.now()-started).toFixed(0)} ms`);
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-compaction-context-", import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    const oracle = join(directory,"compacted-child.json");
    fixture = await installedNativeFixture(directory,nativeProvider,{ACYCLIC_COMPACT_CHILD:oracle});
    phase("installed");
    const {project,manager:parent,session:parentFile} = fixture;
    const settings = join(directory,"agent","settings.json");
    writeFileSync(settings, JSON.stringify({
      ...JSON.parse(readFileSync(settings,"utf8")),
      compaction: { enabled: false, keepRecentTokens: 1, reserveTokens: 1024 },
    }));
    for (const instruction of ["old parent instruction", "recent parent instruction"]) {
      parent.appendMessage({ role: "user", content: instruction, timestamp: 1 });
      parent.appendMessage(fauxAssistantMessage("completed parent response",{stopReason:"stop",timestamp:1}));
    }
    const compactExtension = join(directory, "compact.ts");
    writeFileSync(compactExtension, `
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai";
export default function (pi) {
  const faux = fauxProvider({ provider: "acyclic-qualification", models: [{ id: "probe" }] });
  faux.setResponses([
    fauxAssistantMessage("qualified native parent summary"),
    fauxAssistantMessage("qualified native retained turn summary"),
  ]);
  pi.registerProvider(faux.provider);
  pi.registerCommand("qualify-compact", {
    description: "Run native compaction for qualification",
    async handler(_args, ctx) {
      await new Promise((resolve, reject) => ctx.compact({ onComplete: resolve, onError: reject }));
    },
  });
}
`);
    const compactResult = await invokePi(
      [...probeArguments(parentFile, compactExtension), "/qualify-compact"], project, directory, fixture.environment,
    );
    const compacted = SessionManager.open(parentFile, directory);
    phase("compacted");
    const compaction = compacted.getEntries().find(entry => entry.type === "compaction");
    assert.ok(compaction, `native CLI did not retain its completed compaction: ${compactResult.stdout} ${compactResult.stderr}`);
    assert.match(compaction.summary, /qualified native parent summary/);
    const selectedParent = readFileSync(parentFile,"utf8");
    await fixture.start();
    phase("owner ready");
    const launched = await fixture.runJson("Launch and wait for a child inheriting this exact compacted context.",line=>{
      const event=JSON.parse(line);
      if(event.type.startsWith("tool_")) phase(`${event.type} ${event.toolCallId}`);
    });
    phase("child settled");
    const completed=launched.stdout.trim().split("\n").map(line=>JSON.parse(line))
      .filter(event=>event.type==="message_end" && event.message?.role==="assistant")
      .flatMap(event=>event.message.content.filter(part=>part.type==="text").map(part=>part.text));
    assert.equal(completed.at(-1),"compaction child completed");
    const native = JSON.parse(readFileSync(oracle,"utf8"));
    assert.match(JSON.stringify(native.messages),/qualified native parent summary/);
    assert.doesNotMatch(JSON.stringify(native.messages),/old parent instruction|spawn-after-compaction/);
    assert.notEqual(native.native_session_id,parent.getSessionId());
    assert.equal(typeof native.session_file,"string");
    const child = native.session_file;
    const originalParent = readFileSync(parentFile, "utf8");
    assert.ok(originalParent.startsWith(selectedParent),"selection changed the parent's retained history");
    const extension = join(directory, "provider.ts");
    writeFileSync(extension, provider);
    for (const prompt of ["child after compaction", "resume compacted child"]) {
      const result = await invokePi([...probeArguments(child, extension), prompt], project, directory);
      phase(prompt);
      assert.match(result.stdout, /qualified native parent summary/);
      assert.doesNotMatch(result.stdout, /old parent instruction|spawn-after-compaction/);
      assert.equal(SessionManager.open(child, directory).getSessionId(), native.native_session_id);
      assert.equal(readFileSync(parentFile, "utf8"), originalParent);
    }
  } finally {
    await fixture?.close();
    phase("owner closed");
    rmSync(directory, { recursive: true });
  }
});
