import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider=`
import { fauxProvider, fauxAssistantMessage, fauxToolCall, getCurrentTools } from "@earendil-works/pi-ai";
export default function(pi) {
  const faux=fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  let offered;
  faux.setResponses([
    context=>{
      offered=getCurrentTools(context.messages).map(tool=>tool.name).sort();
      return fauxAssistantMessage(fauxToolCall("write",{
        path:"activated.txt",text:"first model turn",media_type:"text/plain",display_name:"activated.txt"
      },{id:"activated-write"}),{stopReason:"toolUse"});
    },
    context=>{
      const result=context.messages.filter(message=>message.role === "toolResult").at(-1);
      if (!result || result.isError) throw new Error("replacement write did not execute");
      return fauxAssistantMessage(fauxToolCall("acyclic_inspect",{
        task:process.env.ACYCLIC_NATIVE_TASK,
      },{id:"inspect-original-task"}),{stopReason:"toolUse"});
    },
    context=>{
      const result=context.messages.filter(message=>message.role === "toolResult").at(-1);
      if (!result || result.isError) throw new Error("original task inspection did not execute");
      const inspected=JSON.parse(result.content.filter(part=>part.type === "text").map(part=>part.text).join(""));
      return fauxAssistantMessage(JSON.stringify({marker:"replacement-active",offered,inspected}));
    },
  ]);
  pi.registerProvider(faux.provider);
}
`;

test("pinned Pi can call the replacement write on its first turn with built-ins disabled",async () => {
  const directory=mkdtempSync(fileURLToPath(new URL(".pi-active-",import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    fixture=await installedNativeFixture(directory,provider,{});
    const catalog=JSON.parse((await fixture.command(["__catalog"])).stdout);
    for (const tool of catalog.tools) {
      for (const field of ["caller","callerproof","caller_proof","signer","environment","tasklease","approval_digest"]) {
        assert.ok(!Object.hasOwn(tool.input_schema.properties,field),`${tool.name} must not expose ${field}`);
      }
    }
    await fixture.start();
    const answer=await fixture.run("Write activated.txt through the available write tool.");
    assert.ok(answer.stdout.includes("replacement-active"),answer.stdout);
    const report=JSON.parse(answer.stdout.trim());
    assert.deepEqual(report.offered,catalog.tools.map(tool=>tool.name).sort(),"the pinned native loader must expose the complete harness catalog on its first turn");
    assert.equal(report.inspected.task,fixture.environment.ACYCLIC_NATIVE_TASK);
    assert.equal(report.inspected.admission.parent,null);
    assert.equal(readFileSync(join(fixture.project,"activated.txt"),"utf8"),"first model turn");
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
  } finally {
    await fixture?.close();
    rmSync(directory,{recursive:true});
  }
});
