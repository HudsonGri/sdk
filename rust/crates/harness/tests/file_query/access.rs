use super::*;
use acyclic_fs::access::{Action, Effect, Policy};
use acyclic_harness::task_volume;
use proptest::prelude::*;

async fn publish_policy(
    fixture: &Fixture,
    read: bool,
    list: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut policy: Policy =
        serde_json::from_slice(acyclic_harness::filesystem::INITIAL_ACCESS_POLICY)?;
    policy.version += 1;
    if read || list {
        let mut deny = policy.layers[0][0].clone();
        deny.id = "restricted-query".into();
        deny.effect = Effect::Deny;
        deny.actions = [(read, Action::Read), (list, Action::List)]
            .into_iter()
            .filter_map(|(denied, action)| denied.then_some(action))
            .collect();
        deny.resource.path = "plans/**".into();
        policy.layers[0].push(deny);
    }
    let workspace = fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .await?
        .value;
    policy_support::publish(&workspace, &policy).await
}

#[tokio::test]
async fn query_snapshots_and_follow_fail_closed_after_disclosure_revocation()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for (read, list) in [(true, false), (false, true)] {
        let fixture = Fixture::new().await?;
        fixture
            .write(&[("plans/a.md", Some("- [ ] private"))])
            .await?;
        let mut observation = fixture
            .source
            .observe_files(Query::Plans, None, 2, 64)
            .await?;
        assert_eq!(observation.snapshot.as_ref().unwrap().files.len(), 1);
        publish_policy(&fixture, read, list).await?;
        assert!(matches!(
            fixture
                .source
                .observe_files(Query::Plans, None, 2, 64)
                .await,
            Err(task_volume::Error::FileRead(
                acyclic_harness::Error::FilesystemAccessConfiguration("file query")
            ))
        ));
        assert!(matches!(
            observation.changes.next().await,
            Some(Err(task_volume::Error::FileRead(
                acyclic_harness::Error::FilesystemAccessConfiguration("file query")
            )))
        ));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Raw query projection can disclose bytes only after the common compiler
    // proves Read and List unrestricted; otherwise its typed error precedes data.
    #[test]
    fn restrictive_queries_never_return_a_snapshot(read in any::<bool>(), list in any::<bool>(), name in "[a-z]{1,8}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let fixture = Fixture::new().await?;
            fixture.write(&[(&format!("plans/{name}.md"), Some("- [ ] private"))]).await?;
            publish_policy(&fixture, read, list).await?;
            let result = fixture.source.observe_files(Query::Plans, None, 2, 64).await;
            if read || list {
                assert!(matches!(result, Err(task_volume::Error::FileRead(
                    acyclic_harness::Error::FilesystemAccessConfiguration("file query")))));
            } else {
                assert_eq!(result?.snapshot.unwrap().files.len(), 1);
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
