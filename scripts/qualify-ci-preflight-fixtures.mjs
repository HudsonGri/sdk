import assert from "node:assert/strict";
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { qualificationGatePlans } from "./qualify-mcp-loom.mjs";

export function artifact(plans, args, directory) {
  const index = plans.findIndex(plan => JSON.stringify(plan.args) === JSON.stringify(args));
  assert.notEqual(index, -1, `unplanned model build: ${JSON.stringify(args)}`);
  return {
    reason: "compiler-artifact", target: { name: plans[index].target },
    profile: { test: true }, executable: join(directory, `model-${index}`),
  };
}

export function modelResult(plan, args) {
  const test = plan.tests.find(test => JSON.stringify(test.args) === JSON.stringify(args));
  assert.ok(test, `unplanned model invocation: ${JSON.stringify(args)}`);
  return `test result: ok. ${test.passed ?? 6} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n`;
}

export function verifyModelCalls(plans, calls) {
  assert.deepEqual(calls, plans.flatMap(plan => plan.tests.map(test => [plan.args, test.args])));
}

export function verifyGate(plans, cargoCalls, flags, modelCalls) {
  const prefix = [
    "check --locked -p acyclic-harness-fuzz --bins",
    "check --locked -p acyclic-harness --features model-checking --lib",
    "check --locked -p acyclic-harness --features filesystem-local,model-checking --lib",
    "check --locked -p acyclic-harness --features filesystem --lib --tests",
    "test -p acyclic-harness --locked --features model-checking --test client-mailbox-model --test client_journal_model --test mcp-loom",
    "test -p acyclic-harness --locked --features model-checking --lib workflow::model_checking::tests::",
  ];
  assert.deepEqual(cargoCalls, [
    ...prefix, ...plans.map(plan => plan.args.join(" ")),
    "nextest run --profile ci --workspace --all-features --locked",
    "test -p acyclic-harness --all-features --locked --example approved-native-process",
  ]);
  assert.equal(flags.length, cargoCalls.length);
  for (const index of [0, 1, 2, 4, 5]) assert.match(flags[index], /-Dwarnings/);
  for (const index of [1, 2, 4, 5]) {
    assert.match(flags[index], /--cfg acyclic_harness_loom/);
    assert.match(flags[index], /--cfg acyclic_loom/);
  }
  for (const [index, plan] of plans.entries()) {
    assert.equal(flags[prefix.length + index].trim(), plan.rustflags.trim());
  }
  verifyModelCalls(plans, modelCalls);
}

const helper = fileURLToPath(import.meta.url);
if (process.argv[1] && resolve(process.argv[1]) === helper) {
  const [mode, directory, ...args] = process.argv.slice(2);
  const planFile = join(directory, "model-plans.json");
  if (mode === "prepare") {
    const plans = qualificationGatePlans();
    mkdirSync(directory, { recursive: true });
    writeFileSync(planFile, JSON.stringify(plans));
    writeFileSync(join(directory, "models.log"), "");
    const quote = value => `'${value.replaceAll("'", "'\\''")}'`;
    for (const index of plans.keys()) {
      writeFileSync(join(directory, `model-${index}`),
        `#!/usr/bin/env bash\nexec node ${quote(helper)} binary ${quote(directory)} ${index} "$@"\n`, { mode: 0o755 });
    }
  } else {
    const plans = JSON.parse(readFileSync(planFile, "utf8"));
    if (mode === "cargo") {
      process.stdout.write(`${JSON.stringify(artifact(plans, args, directory))}\n`);
    } else if (mode === "binary") {
      const index = Number(args.shift());
      const build = plans[index].args;
      const plan = plans.find(plan => JSON.stringify(plan.args) === JSON.stringify(build)
        && plan.tests.some(test => JSON.stringify(test.args) === JSON.stringify(args)));
      assert.ok(plan, `unplanned model invocation: ${JSON.stringify(args)}`);
      appendFileSync(join(directory, "models.log"), `${JSON.stringify([build, args])}\n`);
      process.stdout.write(modelResult(plan, args));
    } else if (mode === "verify") {
      const lines = name => {
        const content = readFileSync(join(directory, name), "utf8");
        assert.ok(content.endsWith("\n"), `incomplete fixture log: ${name}`);
        return content.slice(0, -1).split("\n");
      };
      verifyGate(plans, lines("cargo.log"), lines("flags.log"), lines("models.log").map(line => JSON.parse(line)));
    } else throw new Error(`unknown fixture operation: ${mode}`);
  }
}
