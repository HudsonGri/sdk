use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_stream::MemoryStream;
use std::cell::Cell;

#[tokio::test]
async fn generation_retry_keeps_the_original_packing_ledger() -> crate::Result<()> {
    for deny in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let volume = TaskVolume::open(provider, StreamPath::new("funded-chunks")?).await?;
        let generation = volume.head().await?;
        let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        if deny {
            budget.bytes_copied = 0;
        }
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let mut write = Write::new(
            FileKey::parse(".tasks/t/turns/0/chunks/0")?,
            vec![b'x'; 1024],
        );
        write.channel = Some(Channel::Text);
        let calls = Cell::new(0);
        let result = publish(
            &volume,
            generation,
            &write,
            || async {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    // A different file publisher advances the generation between
                    // authorization and append, forcing the real retry path.
                    volume
                        .append(generation, &Write::new(FileKey::Goal, b"goal".to_vec()))
                        .await?;
                }
                Ok::<_, Error>((vec![], None))
            },
            Some(&work),
        )
        .await;
        assert_eq!(calls.get(), 2);
        let snapshot = volume.snapshot(&Query::Root, 2).await?;
        if deny {
            assert!(matches!(result, Err(Error::FileRead(ref error)) if error.is_native_failure()));
            // A native failure retains its funded diagnostic custody until drop.
            drop(result);
            assert_eq!(snapshot.files.len(), 1);
            assert_eq!(work.live_allocation_bytes(), baseline);
        } else {
            result?;
            let file = snapshot
                .files
                .iter()
                .find(|file| file.key == write.key)
                .unwrap();
            assert_eq!(file, &write);
            assert!(file.bytes.matches_meter(&work.runtime_meter()));
            assert!(work.receipt().bytes_copied >= 1024);
        }
    }
    Ok(())
}
