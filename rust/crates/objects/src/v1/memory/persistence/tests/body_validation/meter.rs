use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};
use std::io;

pub(super) struct Meter;

// No asynchronous work or shared mutable state: native batches execute on the
// calling thread, so the body-length counter covers both production entrypoints.
macro_rules! accept {
    ($($name:ident($($arg:ident: $ty:ty),*));* $(;)?) => {
        $(fn $name(&self, $(_: $ty),*) -> io::Result<()> { Ok(()) })*
    };
}
macro_rules! refund {
    ($($name:ident($($arg:ident: $ty:ty),*));* $(;)?) => {
        $(fn $name(&self, $(_: $ty),*) {})*
    };
}

impl NativeReadMeter for Meter {
    accept! {
        check();
        reserve_operations(kind: NativeOperationKind, count: u64);
        record_reserved_operation(kind: NativeOperationKind);
        admit_write_bytes(count: usize);
        reserve_bytes(kind: NativeOperationKind, count: u64);
        record_reserved_bytes(kind: NativeOperationKind, maximum: u64, actual: u64);
        allocate(bytes: u64);
        reserve_allocation(bytes: u64);
        record_reserved_allocation(bytes: u64);
        allocate_refcount_header();
        adopt(bytes: u64);
        admit_codec(encoded: u64, copied: u64, hashed: u64);
        record_codec(encoded: u64, copied: u64, hashed: u64);
        examine(items: u64, copied: u64);
        admit_examination(items: u64, copied: u64);
        record_examination(items: u64, copied: u64);
        admit_bytes(bytes: usize);
        admit_read(bytes: usize);
        record_read(bytes: usize);
        admit_write(bytes: usize);
        record_write(bytes: usize);
        admit_durability();
    }
    refund! {
        cancel_reserved_operations(kind: NativeOperationKind, count: u64);
        cancel_reserved_bytes(kind: NativeOperationKind, count: u64);
        cancel_reserved_allocation(bytes: u64);
        release(bytes: u64);
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
        io::Error::other(error)
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        (bytes, Ok(()))
    }
}
