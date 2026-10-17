//! Resume inputs are durable work, including after the delivery owner restarts.
use crate::Result;

pub(crate) enum Slot {
    Settled,
    Busy,
    Idle(u64),
}

// The session's one driver owns both consuming a retained wake and supplying
// its next input. Observing only Idle strands an already accepted resume.
pub(crate) async fn step<A, I, R>(
    advance: impl Fn() -> A,
    inspect: impl Fn() -> I,
    resume: impl Fn(u64) -> R,
) -> Result<bool>
where
    A: Future<Output = Result<()>> + Send,
    I: Future<Output = Result<Slot>> + Send,
    R: Future<Output = Result<()>> + Send,
{
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
