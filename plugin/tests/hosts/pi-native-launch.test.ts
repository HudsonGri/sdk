import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider = `
import { readFileSync, appendFileSync } from "node:fs";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  const native=Boolean(process.env.ACYCLIC_HARNESS_BINDING);
  const task=native ? JSON.parse(readFileSync(process.env.ACYCLIC_HARNESS_BINDING,"utf8")).task : null;
  let session, descendant=false;
  pi.on("session_start",(_event,ctx)=>{
    session=ctx.sessionManager.getSessionId();
    if(native) appendFileSync(process.env.ACYCLIC_NATIVE_ORACLE,JSON.stringify({phase:"start",session,task,entries:ctx.sessionManager.getBranch()})+"\\n");
  });
  pi.on("before_agent_start",event=>{descendant=event.prompt==="GRANDCHILD_ONLY_NATIVE_INSTRUCTION";});
  const faux=fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  const call=(name,args,id)=>fauxAssistantMessage(fauxToolCall(name,args,{id}),{stopReason:"toolUse"});
  const result=context=>{
    const value=context.messages.filter(message=>message.role==="toolResult").at(-1);
    if(!value || value.isError) throw new Error("native tool failed: "+JSON.stringify(value));
    const text=value.content.filter(part=>part.type==="text").map(part=>part.text).join("");
    return JSON.parse(text);
  };
  let child, first=true;
  const nativeResponse=context=>{
    const prefix=descendant ? "grandchild" : "child";
    const results=context.messages.filter(message=>message.role==="toolResult");
    appendFileSync(process.env.ACYCLIC_NATIVE_ORACLE,JSON.stringify({phase:"model",session,prefix,
      results:results.map(result=>({id:result.toolCallId,error:result.isError,...(result.isError?{content:result.content}:{})}))})+"\\n");
    const done=id=>results.some(message=>message.toolCallId===prefix+"-"+id);
    if(first) {
      first=false;
      const bytes=JSON.stringify(context.messages);
      if(bytes.includes("ROOT_ONLY_NATIVE_INSTRUCTION") !== Boolean(process.env.ACYCLIC_NATIVE_CUT) || !bytes.includes("CHILD_ONLY_NATIVE_INSTRUCTION")) {
        throw new Error("native launch substituted the selected fresh context");
      }
    }
    if(results.length) result(context);
    if(!done("write")) return fauxAssistantMessage([
      fauxToolCall("write",{path:descendant?"automatic-grandchild.txt":"automatic-child.txt",text:"automatic native child",media_type:"text/plain",display_name:"automatic native child"},{id:prefix+"-write"}),
      fauxToolCall("acyclic_inspect",{task},{id:prefix+"-inspect"}),
      ...(descendant ? [fauxToolCall("bash",{command:"cat automatic-child.txt"},{id:prefix+"-read"})] : []),
    ],{stopReason:"toolUse"});
    for(const output of results.filter(message=>message.toolCallId.startsWith(prefix+"-"))) {
      if(output.isError) throw new Error("native tool failed: "+JSON.stringify(output));
    }
    const inspected=JSON.parse(results.find(message=>message.toolCallId===prefix+"-inspect").content.filter(part=>part.type==="text").map(part=>part.text).join(""));
    if(!done("spawn") && !descendant && process.env.ACYCLIC_NATIVE_CUT) {
      return call("acyclic_spawn",{id:"automatic-grandchild",prompt:"GRANDCHILD_ONLY_NATIVE_INSTRUCTION",context:"native_cut"},prefix+"-spawn");
    }
    if(!done("wait") && done("spawn")) {
      return call("acyclic_wait",{child:result(context)},prefix+"-wait");
    }
    if(done("wait") && result(context).success!==true) throw new Error("native descendant did not settle");
    {
      if(inspected.task!==task || !inspected.admission.parent) throw new Error("native child lost its admitted identity");
      appendFileSync(process.env.ACYCLIC_NATIVE_ORACLE,JSON.stringify({phase:"complete",session,task,parent:inspected.admission.parent,results})+"\\n");
      return fauxAssistantMessage("automatic-child-complete");
    }
  };
  faux.setResponses(native ? Array.from({length:8},()=>nativeResponse) : [
    ()=>call("acyclic_spawn",{id:"automatic-child",prompt:"CHILD_ONLY_NATIVE_INSTRUCTION",...(process.env.ACYCLIC_NATIVE_CUT ? {context:"native_cut"} : {})},"native-spawn"),
    context=>{child=result(context);return call("acyclic_wait",{child},"native-wait");},
    context=>{
      const outcome=result(context);
      if(outcome.success!==true) throw new Error("owned native execution did not complete: "+JSON.stringify(outcome));
      return call("fs",{command:"diff agents/"+child},"native-diff");
    },
    context=>{result(context);return fauxAssistantMessage(JSON.stringify({marker:"automatic-native-launch",child}));},
  ]);
  pi.registerProvider(faux.provider);
}
`;

for(const selected of [false,true]) test.concurrent(`the installed Pi spawn tool launches and reaps its own ${selected?"selected":"fresh"} native child`, async () => {
  const directory=mkdtempSync(fileURLToPath(new URL(".pi-native-launch-",import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    const oracle=join(directory,"native-oracle.jsonl");
    fixture=await installedNativeFixture(directory,provider,{ACYCLIC_NATIVE_ORACLE:oracle,...(selected?{ACYCLIC_NATIVE_CUT:"1"}:{})});
    await fixture.start();
    const answer=await fixture.run("ROOT_ONLY_NATIVE_INSTRUCTION Launch and wait for the automatic native child.");
    const reported=JSON.parse(answer.stdout.trim());
    assert.equal(reported.marker,"automatic-native-launch");
    const records=readFileSync(oracle,"utf8").trim().split("\n").map(line=>JSON.parse(line));
    const children=records.filter(record=>record.phase==="start");
    assert.equal(children.length,selected?2:1,"native custody launched a child more than once");
    assert.equal(children[0].task,reported.child);
    assert.match(children[0].session,/^[0-9a-f-]{36}$/);
    assert.notEqual(children[0].session,fixture.manager.getSessionId());
    const inherited=JSON.stringify(children[0].entries);
    assert.equal(inherited.includes("ROOT_ONLY_NATIVE_INSTRUCTION"),selected);
    assert.ok(!inherited.includes("native-spawn"),"the invoking assistant entered the child seed");
    assert.ok(!existsSync(join(fixture.project,"automatic-child.txt")));
    const completion=records.filter(record=>record.phase==="complete");
    assert.equal(completion.length,children.length,"every native session must settle through custody");
    assert.equal(completion.find(record=>record.task===reported.child).parent,fixture.environment.ACYCLIC_NATIVE_TASK);
    if(selected) {
      const grandchild=children[1];
      assert.notEqual(grandchild.session,children[0].session);
      assert.notEqual(grandchild.task,children[0].task);
      assert.equal(completion.find(record=>record.task===grandchild.task).parent,children[0].task);
      // The independent SDK oracle contains the exact original records from
      // both generations, plus complete exchanges before the next spawn.
      for(const entry of children[0].entries) {
        assert.deepEqual(grandchild.entries.find(candidate=>candidate.id===entry.id),entry);
      }
      const raw=JSON.stringify(grandchild.entries);
      assert.ok(raw.includes("ROOT_ONLY_NATIVE_INSTRUCTION"));
      assert.ok(raw.includes("CHILD_ONLY_NATIVE_INSTRUCTION"));
      assert.ok(!raw.includes('"id":"child-spawn"'),"the descendant inherited its invoking assistant");
      for(const id of ["child-write","child-inspect"]) {
        assert.equal(grandchild.entries.filter(entry=>entry.type==="message" && entry.message.role==="toolResult" && entry.message.toolCallId===id).length,1);
        assert.equal(grandchild.entries.filter(entry=>entry.type==="message" && entry.message.role==="assistant" && entry.message.content.some(part=>part.type==="toolCall" && part.id===id)).length,1);
      }
      const read=completion.find(record=>record.task===grandchild.task).results.find(message=>message.toolCallId==="grandchild-read");
      const readResult=JSON.parse(read.content.filter(part=>part.type==="text").map(part=>part.text).join(""));
      assert.equal(readResult.success,true);
      assert.equal(readResult.stdout,"automatic native child");
      assert.ok(!existsSync(join(fixture.project,"automatic-grandchild.txt")));
    }
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
  } catch(error) {
    const oracle=join(directory,"native-oracle.jsonl");
    if(existsSync(oracle)) for(const line of readFileSync(oracle,"utf8").trim().split("\n")) {
      const record=JSON.parse(line);
      console.error(JSON.stringify({phase:record.phase,session:record.session,prefix:record.prefix,
        results:record.phase==="model"?record.results:record.results?.map(result=>({id:result.toolCallId,error:result.isError,...(result.isError?{content:result.content}:{})}))}));
    }
    throw error;
  } finally {
    await fixture?.close();
    rmSync(directory,{recursive:true});
  }
},8000);
