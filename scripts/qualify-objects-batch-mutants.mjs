import { readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
const path = "rust/crates/objects/src/v1/grpc/batch/reply.rs";
const source = readFileSync(path, "utf8");
const controls = [
  ["ordinary-native-storage", "let metadata = batch_vector(native_count, meter)?;", "let metadata = batch_vector(sizes.value.len(), meter)?;", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_ordinary_exchange_has_no_native_storage"]],
  ["oversized-text", "let capacity = text.len();", "let capacity = if self.meter.is_some() { TEXT_BYTES } else { text.len() };", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_copies_only_final_string_values"]],
  ["metadata-handoff", "metadata.value.push(info.metadata);", "metadata.value.push(None);", ["test", "--locked", "-p", "acyclic-objects", "--lib", "batch_exchange_models_metadata_transfer_decode_settlement_and_drop"]],
  ["late-cancellation", "codec.record(bytes.len() as u64, 0)?;", "self.meter.as_ref().unwrap().admit_codec(bytes.len() as u64, 0, 0)?;", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_completion_survives_a_genuinely_cancelled_meter"]],
  ["rejected-item-charge", "reply.failure.get_or_insert(error);", "reply.response._allocation.as_mut().unwrap().absorb(item_credit.as_mut().unwrap())?; reply.failure.get_or_insert(error);", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_rejected_item_retires_capacity_before_the_next_item"]],
  ["lost-reply-result", "reply.response.value.results.push(result);", "reply.response.value.results.push(wire::PutObjectResult::default());", ["bash", "scripts/fuzz-objects-batch-reply.sh", "-runs=0"]],
];
const selected = process.argv.slice(2);
if (selected.some(name => !controls.some(control => control[0] === name))) throw new Error("unknown production mutant");
try {
  for (const [name, before, after, args] of controls) {
    if (selected.length && !selected.includes(name)) continue;
    if (source.split(before).length !== 2) throw new Error(`${name}: mutation site must be unique`);
    writeFileSync(path, source.replace(before, after));
    const command = args[0] === "bash" ? args[0] : "cargo";
    const commandArgs = command === "bash" ? args.slice(1) : args;
    const env = name === "metadata-handoff" ? { ...process.env, RUSTFLAGS: `${process.env.RUSTFLAGS ?? ""} --cfg objects_loom` } : process.env;
    const result = spawnSync(command, commandArgs, { env, encoding: "utf8", timeout: 120000, maxBuffer: 8 * 1024 * 1024 });
    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    const executed = name === "lost-reply-result" ? /assertion.*failed|panicked at/.test(output) : /test result: FAILED/.test(output);
    if (result.status === 0 || !executed || /could not compile/.test(output)) throw new Error(`${name}: mutant was not rejected by its executing oracle\n${output}`);
    console.log(`production mutant rejected: ${name}`);
    writeFileSync(path, source);
  }
} finally {
  writeFileSync(path, source);
}
