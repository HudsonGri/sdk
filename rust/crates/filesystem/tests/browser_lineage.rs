//! The shared lineage store through its browser persistence adapter.
#![cfg(all(target_arch = "wasm32", feature = "browser"))]
use acyclic_fs::{CancellationToken, Digest, WorkBudget, browser::IndexedDbAuthorityStore};
use indexed_db_futures::database::Database;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn lineage_cas_is_atomic_and_survives_browser_authority_reopen() -> Result<(), JsValue> {
    use acyclic_fs::WorkspaceLineageStore as _;
    use acyclic_fs::facade::FsWork as _;
    const NAME: &str = "acyclic-fs-lineage-transaction";
    delete_database(NAME).await?;
    let store = IndexedDbAuthorityStore::open(NAME, 4096)
        .await
        .map_err(js_error)?;
    let id = acyclic_fs::WorkspaceId::from_bytes([61; 16]);
    let generation = acyclic_fs::GenerationId::new(Digest::from_bytes([42; 32]));
    let record = acyclic_fs::WorkspaceLineageRecord {
        version: 2,
        revision: 1,
        ready: true,
        workspace_id: id,
        workspace_name: "browser-lineage-root".into(),
        parent_workspace_id: None,
        parent_workspace_name: None,
        creation_operation: None,
        fork_generation: generation,
        initial_generation: generation,
    };
    let cancellation = CancellationToken::new();
    let mut first = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let mut second = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let (one, two) = futures::join!(
        store.compare_accounted(id, 0, &record, &mut first),
        store.compare_accounted(id, 0, &record, &mut second),
    );
    assert_ne!(one.map_err(js_error)?, two.map_err(js_error)?);
    drop(store);
    let reopened = IndexedDbAuthorityStore::open(NAME, 4096)
        .await
        .map_err(js_error)?;
    let loaded = reopened
        .load_accounted(id, &mut first)
        .await
        .map_err(js_error)?
        .ok_or_else(|| js_error("durable lineage registration disappeared"))?;
    assert_eq!(loaded.value, record);
    loaded.retire(&mut first).map_err(js_error)?;
    let mut other = record.clone();
    other.workspace_id = acyclic_fs::WorkspaceId::from_bytes([62; 16]);
    assert!(
        reopened
            .compare_accounted(id, 1, &other, &mut first)
            .await
            .is_err()
    );
    // Insufficient durability authority must be rejected before the first put,
    // even on a backend that commits automatically after its last request.
    let mut denied_budget = WorkBudget::UNBOUNDED;
    denied_budget.durability_operations = 0;
    let mut denied = denied_budget.accounting(&cancellation);
    let mut next = record.clone();
    next.revision = 2;
    assert!(
        reopened
            .compare_accounted(id, 1, &next, &mut denied)
            .await
            .is_err()
    );
    let unchanged = reopened
        .load_accounted(id, &mut first)
        .await
        .map_err(js_error)?
        .unwrap();
    assert_eq!(unchanged.value, record);
    unchanged.retire(&mut first).map_err(js_error)?;
    Ok(())
}

async fn delete_database(name: &str) -> Result<(), JsValue> {
    Database::delete_by_name(name)
        .map_err(js_error)?
        .await
        .map_err(js_error)
}
fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
