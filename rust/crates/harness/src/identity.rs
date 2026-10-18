//! All fresh harness UUIDs cross this boundary; persisted IDs are never reminted.
pub(crate) fn fresh_uuid() -> uuid::Uuid {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return uuid::Builder::from_random_bytes(bytes).into_uuid();
        }
    }
    uuid::Uuid::new_v4()
}
