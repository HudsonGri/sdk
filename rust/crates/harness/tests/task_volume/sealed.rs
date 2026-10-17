use super::*;
use futures::FutureExt as _;
use proptest::prelude::*;

async fn sealed_turn(turn: u64, text: &str, tool: &str) -> Result {
    let (_, volume) = memory().await?;
    let mut chunk = write(&format!(".tasks/t/turns/{turn}/chunks/0"), text)?;
    chunk.channel = Some(Channel::Text);
    let output = write(&format!(".tasks/t/turns/{turn}/tools/c/stdout/0"), tool)?;
    let completion = write(&format!(".tasks/t/turns/{turn}/completion.json"), "{}")?;
    let start = volume.head().await?;
    let sealed = append_all(
        &volume,
        start,
        &[chunk.clone(), output.clone(), completion.clone()],
    )
    .await?;
    let snapshot = volume.snapshot(&Query::Root, 3).await?;
    let mut follow = volume.follow(Query::Root, snapshot.cursor).await?;
    // An acknowledgement lost before completion may be retried after it. The
    // immutable identity is checked without publishing another record.
    for prior in [&chunk, &output, &completion] {
        assert_eq!(volume.append(sealed, prior).await?, sealed);
    }
    for mut next in [
        write(&format!(".tasks/t/turns/{turn}/chunks/1"), "late")?,
        write(&format!(".tasks/t/turns/{turn}/tools/c/stderr/0"), "late")?,
        write(
            &format!(".tasks/t/turns/{turn}/tools/new/request.json"),
            "{}",
        )?,
        write(&format!(".tasks/t/turns/{turn}/request.json"), "{}")?,
        write(&format!(".tasks/t/turns/{turn}/errors/1.json"), "{}")?,
    ] {
        if matches!(
            next.key,
            FileKey::Task {
                file: acyclic_harness::task_volume::TaskFile::Chunk { .. },
                ..
            }
        ) {
            next.channel = Some(Channel::Reasoning);
        }
        assert!(matches!(volume.append(sealed, &next).await,
            Err(Error::ImmutableConflict(key)) if key == completion.key));
    }
    assert_eq!(volume.head().await?, sealed);
    assert!(follow.next().now_or_never().is_none());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn completion_seals_every_stream_in_its_turn(turn in any::<u64>(), text in "[a-zλ雪]{0,32}", tool in "[a-z]{0,32}") {
        let result = futures::executor::block_on(sealed_turn(turn, &text, &tool));
        prop_assert!(result.is_ok(), "{result:?}");
    }
}
