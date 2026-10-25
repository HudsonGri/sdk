// Credential-free Linux process fixtures. Managed-model qualification is owned
// by the coordinator; these exercise the production adapters and native sandbox.
import assert from 'node:assert/strict';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {stageArtifacts} from '../../../scripts/graphcoder-e2e/artifacts.mjs';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
const [cargo,output,codex,mode,...extra]=process.argv.slice(2);
const recovery='node plugin/tests/hosts/qualify-scripted.mjs CARGO_JSON FRESH_OUTPUT CODEX managed|scale';
assert.ok(cargo&&output&&codex&&!extra.length&&['managed','scale'].includes(mode),
  'step=admit scripted host qualification; recovery: '+recovery);
const directory=resolve(output);await mkdir(directory);
const artifacts=await stageArtifacts(await Promise.all(['acyclic','graphcoder'].map(async name=>({
  name,target:name,test:false,records:await readFile(cargo,'utf8'),
}))),join(directory,'bin'));
const here=fileURLToPath(new URL('.',import.meta.url));
const binary=artifacts.artifacts.acyclic.path, graphcoder=artifacts.artifacts.graphcoder.path;
const cases=mode==='managed' ? [
  ...['lifecycle','recursive','resume'].map(name=>({name,
    args:[join(here,'managed-session.py'),binary,graphcoder,resolve(codex),name],
    accepts:run=>run.stdout.includes('"suite": "managed-native-session-'+name+'"')})),
  {name:'bounded-framing',args:[join(here,'mcp-framing.py'),binary],
    accepts:run=>JSON.parse(run.stdout).cases.length===9},
] : ['codex','claude-code','pi'].map(name=>({name,
  args:[join(here,'shared-mount-scale.py'),binary,graphcoder,name],
  accepts:run=>run.stdout.includes('"sessions_alive_at_both_samples": true')}));
const receipts=[];
for(const entry of cases){
  const home=join(directory,'fixture-'+entry.name);await mkdir(home);
  const env={PATH:process.env.PATH,RUST_TEST_THREADS:'4'};
  for(const key of ['HOME','CODEX_HOME','CLAUDE_CONFIG_DIR','XDG_STATE_HOME','XDG_CONFIG_HOME','XDG_DATA_HOME','TMPDIR']){
    env[key]=join(home,key.toLowerCase());await mkdir(env[key]);
  }
  const scope=new Processes(),started=performance.now();let expired=false,run;
  const deadline=setTimeout(()=>{expired=true;scope.close().catch(error=>console.error(error));},9500);
  try{run=await scope.runOwned('python3',entry.args,{cwd:process.cwd(),env,scopeMountRoot:home});}
  finally{clearTimeout(deadline);await scope.close();}
  const seconds=(performance.now()-started)/1000;
  await writeFile(join(directory,entry.name+'.log'),run.stdout+run.stderr);
  const receipt={name:entry.name,seconds,exit:run.code,expired,
    accepted:!expired&&seconds<10&&run.code===0&&entry.accepts(run)};
  receipts.push(receipt);await writeFile(join(directory,'results.json'),JSON.stringify(receipts,null,2));
  console.log(JSON.stringify(receipt));
}
assert.ok(receipts.every(row=>row.accepted),
  'step=qualify scripted host adapters; recovery: '+recovery);
