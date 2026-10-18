//! Fresh filesystem UUIDs share the active simulation's entropy source.
pub(crate) fn fresh_uuid() -> uuid::Uuid {
    #[cfg(feature = "test-support")]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return uuid::Builder::from_random_bytes(bytes).into_uuid();
        }
    }
    uuid::Uuid::new_v4()
}
