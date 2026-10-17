//! Resume inputs are durable work, including after the delivery owner restarts.
use crate::Result;

pub(crate) enum Slot {
    Settled,
    Busy,
    Idle(u64),
}

// The session's one driver owns both consuming a retained wake and supplying
// its next input. Observing only Idle strands an already accepted resume.
pub(crate) async fn step(
    advance: impl AsyncFn() -> Result<()>,
    inspect: impl AsyncFn() -> Result<Slot>,
    resume: impl AsyncFn(u64) -> Result<()>,
) -> Result<bool> {
    advance().await?;
    match inspect().await? {
        Slot::Settled => Ok(true),
        Slot::Busy => Ok(false),
        Slot::Idle(revision) => {
            resume(revision).await?;
            advance().await?;
            Ok(matches!(inspect().await?, Slot::Settled))
        }
    }
}
