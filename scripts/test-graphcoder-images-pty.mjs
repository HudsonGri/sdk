#!/usr/bin/env node
// Physical X11 clipboard + host attach through the installed GraphCoder and shared client.
import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {writeFileSync} from "node:fs";
import {join,isAbsolute} from "node:path";
import {fixture} from "./graphcoder-fixture.mjs";
import {Terminal,screenText} from "./graphcoder-pty/screen.mjs";
const [binary]=process.argv.slice(2);
assert.ok(isAbsolute(binary));
const host=await fixture({images:true});
const png=Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGP4/x8AAwAB//wl3FEAAAAASUVORK5CYII=","base64");
const path=join(host.root,"selected image.png");writeFileSync(path,png);
const clipboard=spawn("xclip",["-selection","clipboard","-t","image/png","-i","-quiet"],{stdio:["pipe","pipe","pipe"]});
clipboard.stdin.end(png);
await new Promise((resolve,reject)=>{clipboard.on("error",reject);clipboard.stderr.once("data",resolve);setTimeout(resolve,100);});
const screen=new Terminal({cols:100,rows:30,allowProposedApi:true});
const quote=text=>`'${text.replaceAll("'", "'\\''")}'`;
const child=spawn("/usr/bin/script",["-q","-e","-f","-c",`stty rows 30 cols 100; exec ${[binary,...host.args].map(quote).join(" ")}`,"/dev/null"],{env:{...process.env,TERM:"xterm-256color"},stdio:["pipe","pipe","pipe"]});
let stage=0,output="",failure;
const timeout=setTimeout(()=>{failure=new Error(`image PTY timed out at ${stage}: ${screenText(screen)}`);child.kill("SIGKILL");},8000);
const submit=text=>child.stdin.write(`\x1b[200~${text}\x1b[201~\r`);
child.stdout.on("data",bytes=>{output+=bytes.toString();screen.write(bytes,()=>{
    if(failure)return;
    try {
        const visible=screenText(screen);
        if(stage===0&&visible.includes("GraphCoder")&&output.includes("\x1b[?25h")){stage=1;submit(`/attach ${path}`);}
        else if(stage===1&&/1 image/i.test(visible)){stage=2;writeFileSync(path,"host file replaced");child.stdin.write("\x16");}
        else if(stage===2&&/2 images/i.test(visible)){stage=3;submit("");}
        else if(stage===3&&visible.includes("test answer")&&visible.includes("○")){stage=4;child.stdin.write("\x04");}
    }catch(error){failure=error;child.kill();}
});});
child.stderr.on("data",bytes=>{output+=bytes.toString();});
try {
    const code=await new Promise((resolve,reject)=>{child.on("error",reject);child.on("exit",(code,signal)=>resolve(signal??code));});
    if(failure)throw failure;
    assert.equal(code,0,output);assert.equal(stage,4,screenText(screen));
    assert.equal(host.imageRequests.length,1);
    for(const request of [...host.imageCounts,...host.imageRequests]) {
        const images=request.input.flatMap(message=>Array.isArray(message.content)?message.content:[]).filter(part=>part.type==="input_image");
        assert.equal(images.length,2,JSON.stringify(request));
        assert.equal(images[0].image_url,`data:image/png;base64,${png.toString("base64")}`);
        assert.match(images[1].image_url,/^data:image\/jpeg;base64,/);
        const jpeg=Buffer.from(images[1].image_url.split(",")[1],"base64");
        assert.equal(jpeg[0],255);assert.equal(jpeg[1],216);
    }
    assert.ok(host.imageCounts.length>=1);
    console.log(`image PTY: attach + physical clipboard paste + pinned private refs + provider parts passed`);
}finally{clearTimeout(timeout);child.kill();clipboard.kill();screen.dispose();await host.close();}
