//! The production descriptor decoder with the shared socket/custody oracle.
#![no_main]
#[path = "../../src/native_mount/fuse/mount/reply.rs"]
mod reply;
use reply::receive_device;
#[path = "../../src/native_mount/fuse/mount/reply/check.rs"]
mod check;
libfuzzer_sys::fuzz_target!(|input: &[u8]| check::check(input));
