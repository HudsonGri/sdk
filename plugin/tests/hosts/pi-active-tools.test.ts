import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider=`
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  const faux=fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  faux.setResponses([
    fauxAssistantMessage(fauxToolCall("write",{
      path:"activated.txt",text:"first model turn",media_type:"text/plain",display_name:"activated.txt"
    },{id:"activated-write"}),{stopReason:"toolUse"}),
    context=>{
      const result=context.messages.filter(message=>message.role === "toolResult").at(-1);
      if (!result || result.isError) throw new Error("replacement write did not execute");
      return fauxAssistantMessage("replacement-active");
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
    await fixture.start();
    const answer=await fixture.run("Write activated.txt through the available write tool.");
    assert.ok(answer.stdout.includes("replacement-active"),answer.stdout);
    assert.equal(readFileSync(join(fixture.project,"activated.txt"),"utf8"),"first model turn");
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
  } finally {
    await fixture?.close();
    rmSync(directory,{recursive:true});
  }
});
