//! Compile with cfg only on this target for the production reader models.
//! Without cfg, exercise its native worker and buffer reuse directly.

mod sync {
    include!("../src/sync/arc.rs");
    include!("../src/sync/mutex.rs");
    include!("../src/sync/reader.rs");
    pub(crate) use Condvar as SyncCondvar;
    pub(crate) use Mutex as SyncMutex;
    pub(crate) use MutexGuard as SyncGuard;
}

#[path = "../src/client/native_input/reader.rs"]
mod reader;
