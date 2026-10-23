//! Deferred storage remains live; dependent effects wait for the phase cut.
use super::*;

/// Dispatch claims use the provider's pre-admitted durable publication. A
/// barrier constructed after a committed start could strand an unissued effect.
pub(crate) async fn claim<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    return acyclic_stream::requiring_durability(future).await;
    #[cfg(not(all(feature = "filesystem-local", not(target_arch = "wasm32"))))]
    future.await
}

pub(crate) async fn run<T>(
    journal: &dyn ExecutionJournal,
    io: Option<&PreparationIo>,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    if journal.supports_deferred_durability() {
        let result = acyclic_stream::deferring_durability(future).await;
        // Seal even a failed stream's accepted prefix before returning its error.
        journal.durability_barrier(io)?.await?;
        return result;
    }
    let _ = (journal, io);
    future.await
}

pub(crate) async fn seal(journal: &dyn ExecutionJournal, io: Option<&PreparationIo>) -> Result<()> {
    if journal.supports_deferred_durability() {
        journal.durability_barrier(io)?.await?;
    }
    Ok(())
}
