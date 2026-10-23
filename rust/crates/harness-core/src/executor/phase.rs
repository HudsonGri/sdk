//! Deferred storage remains live; dependent effects wait for the phase cut.
use super::*;

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
