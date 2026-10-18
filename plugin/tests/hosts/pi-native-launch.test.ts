import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";
import { nativePiSession } from "./pi-rpc-fixture.ts";

const provider = `
import { readFileSync, appendFileSync } from "node:fs";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  const trace=record=>appendFileSync(process.env.ACYCLIC_NATIVE_ORACLE,JSON.stringify({at:Date.now(),pid:process.pid,...record})+"\\n");
  const native=Boolean(process.env.ACYCLIC_HARNESS_BINDING);
  const task=native ? JSON.parse(readFileSync(process.env.ACYCLIC_HARNESS_BINDING,"utf8")).task : null;
  let session, descendant=false;
  pi.on("session_start",(_event,ctx)=>{
    session=ctx.sessionManager.getSessionId();
    if(native) trace({phase:"start",session,task,entries:ctx.sessionManager.getBranch()});
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
    trace({phase:"model",session,prefix,
      results:results.map(result=>({id:result.toolCallId,error:result.isError,...(result.isError?{content:result.content}:{})}))});
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
      trace({phase:"complete",session,task,parent:inspected.admission.parent,results});
      return fauxAssistantMessage("automatic-child-complete");
    }
  };
  faux.setResponses(native ? Array.from({length:8},()=>nativeResponse) : [
    ()=>{trace({phase:"root",step:"spawn"});return call("acyclic_spawn",{id:"automatic-child",prompt:"CHILD_ONLY_NATIVE_INSTRUCTION",...(process.env.ACYCLIC_NATIVE_CUT ? {context:"native_cut"} : {})},"native-spawn");},
    context=>{trace({phase:"root",step:"wait"});child=result(context);return call("acyclic_wait",{child},"native-wait");},
    context=>{
      trace({phase:"root",step:"diff"});
      const outcome=result(context);
      if(outcome.success!==true) throw new Error("owned native execution did not complete: "+JSON.stringify(outcome));
      return call("fs",{command:"diff agents/"+child},"native-diff");
    },
    context=>{trace({phase:"root",step:"complete"});result(context);return fauxAssistantMessage(JSON.stringify({marker:"automatic-native-launch",child}));},
  ]);
  pi.registerProvider(faux.provider);
}
`;

// Each case keeps its own checkout, SDK session and LocalHarness. Establish
// authenticated owner readiness once, before the unchanged operation deadlines.
const fixtures=new Map<boolean,{directory:string,oracle:string,passed:boolean,fixture?:Awaited<ReturnType<typeof installedNativeFixture>>,native?:ReturnType<typeof nativePiSession>}>();
beforeAll(async()=>{
  const results=await Promise.allSettled([false,true].map(async selected=>{
    const directory=mkdtempSync(fileURLToPath(new URL(".pi-native-launch-",import.meta.url)));
    const oracle=join(directory,"native-oracle.jsonl");
    const state={directory,oracle,passed:false,fixture:undefined as Awaited<ReturnType<typeof installedNativeFixture>>|undefined,native:undefined as ReturnType<typeof nativePiSession>|undefined};
    fixtures.set(selected,state);
    state.fixture=await installedNativeFixture(directory,provider,{ACYCLIC_NATIVE_ORACLE:oracle,...(selected?{ACYCLIC_NATIVE_CUT:"1"}:{})});
    await state.fixture.start();
    state.native=nativePiSession(state.fixture);
    await state.native.ready;
  }));
  const errors=results.filter(result=>result.status==="rejected").map(result=>result.reason);
  if(errors.length) throw new AggregateError(errors,"native owner fixture readiness failed");
});
afterAll(async()=>{
  const results=await Promise.allSettled([...fixtures.values()].map(async ({fixture,native,directory,oracle,passed})=>{
    // Bun's test deadline does not throw through the pending test promise.
    // Stop writers first, and keep diagnostics from bypassing owned cleanup.
    const stopped=await Promise.allSettled([native?.close(),fixture?.close()]);
    const failures=stopped.filter(result=>result.status==="rejected").map(result=>result.reason);
    if(failures.length) throw new AggregateError(failures,"native CLI and provider shutdown failed");
    try {
      if(!passed) reportTrace(oracle);
    } finally {
      rmSync(directory,{recursive:true});
    }
  }));
  const errors=results.filter(result=>result.status==="rejected").map(result=>result.reason);
  if(errors.length) throw new AggregateError(errors,"native owner fixture shutdown failed");
});

for(const selected of [false,true]) test.concurrent(`the installed Pi spawn tool launches and reaps its own ${selected?"selected":"fresh"} native child`, async () => {
  const {directory,oracle,fixture,native}=fixtures.get(selected)!;
  assert.ok(fixture && native);
  try {
    assert.ok(!existsSync(oracle),"native readiness must not run the scripted model early");
    const answer=await native.run("ROOT_ONLY_NATIVE_INSTRUCTION Launch and wait for the automatic native child.");
    const reported=JSON.parse(answer.stdout.trim());
    assert.equal(reported.marker,"automatic-native-launch");
    const records=readFileSync(oracle,"utf8").trim().split("\n").map(line=>JSON.parse(line));
    const root=records.filter(record=>record.phase==="root");
    assert.deepEqual(root.map(record=>record.step),["spawn","wait","diff","complete"]);
    assert.ok(root.every(record=>record.pid===native.pid),"one native host must own the complete original turn");
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
    await native.close();
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
    fixtures.get(selected)!.passed=true;
  } catch(error) {
    reportTrace(oracle);
    throw error;
  }
},8000);

function reportTrace(oracle:string) {
  if(!existsSync(oracle)) return;
  for(const line of readFileSync(oracle,"utf8").trim().split("\n").filter(Boolean)) {
    const record=JSON.parse(line);
    console.error(JSON.stringify({at:record.at,phase:record.phase,step:record.step,session:record.session,prefix:record.prefix,
      results:record.phase==="model"?record.results:record.results?.map(result=>({id:result.toolCallId,error:result.isError,...(result.isError?{content:result.content}:{})}))}));
  }
}
