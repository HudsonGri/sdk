//! Admission is proven against the counting contract, not a tokenizer: for
//! the same request, every shipping counter reports at least the input tokens
//! its provider charges. These rows drive each real counter in this crate.
use super::*;
use crate::counting::wire;
use acyclic_harness::context::{CatalogSkill, SkillCatalog, ThresholdCompaction};

/// Admission's own predicate at the tightest capacity it accepts for
/// `counted`: the provider's charge must still leave the response reserve.
fn admitted_charge_fits(counted: u64, charged: u64) -> bool {
    let policy = ThresholdCompaction {
        response_reserve_tokens: 64,
        recent_tokens: 1,
        ..ThresholdCompaction::default()
    };
    let capacity = ModelContextCapacity {
        context_tokens: u32::try_from(counted + 64).expect("fixture counts fit u32"),
        output_tokens: 64,
    };
    assert!(
        !policy.needs_compaction(capacity, counted).unwrap(),
        "admission accepts its own count"
    );
    !policy.needs_compaction(capacity, charged).unwrap()
}

struct Row {
    counter: String,
    counted: u64,
    charged: u64,
}

fn violations(rows: &[Row]) -> Vec<&str> {
    rows.iter()
        .filter(|row| !admitted_charge_fits(row.counted, row.charged))
        .map(|row| row.counter.as_str())
        .collect()
}

fn catalog(skills: usize) -> String {
    SkillCatalog {
        skills: (0..skills)
            .map(|index| CatalogSkill {
                name: format!("s{index}"),
                description: "Draft release notes from merged changes. é λ".into(),
                path: format!(".agents/skills/s{index}/SKILL.md"),
            })
            .collect(),
        unlisted: 0,
    }
    .render(u64::MAX)
    .unwrap_or_else(|| "system".into())
}

fn with_system(original: &PreparedModelRequest, text: &str) -> PreparedModelRequest {
    let mut request = original.request().clone();
    request.messages[0].content = ModelContent::Text(text.into());
    PreparedModelRequest::prepare(request, Limits::default(), None).unwrap()
}

/// What the fixture provider's count route reports for the request: its own
/// tokenizer, one token per encoded count-body byte plus framing.
fn route_tokens(provider: &ResolvedModel, adapter: Adapter, request: &PreparedModelRequest) -> u64 {
    let names = crate::tool_names::Names::new(request.request());
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    let projected = projection::body_named(
        provider,
        request.request(),
        &names,
        &origin,
        &mut crate::files::FileText::default(),
    )
    .unwrap();
    let body = wire::body(&projected, &provider.model.name, adapter);
    serde_json::to_vec(&body).unwrap().len() as u64 + 512
}

fn count(provider: &ResolvedModel, request: &PreparedModelRequest) -> u64 {
    futures::executor::block_on(provider.count_tokens(request))
        .unwrap()
        .input_tokens
}

/// Every shipping counter, each on the requests whose charge is known.
fn rows() -> Vec<Row> {
    let mut rows = Vec::new();
    for (adapter, _) in fixtures() {
        let (provider, _) = provider(adapter, 200);
        let (request, dispatch) = request(&provider).unwrap();
        // Recorded usage: what the provider charged for this exact request.
        let events = futures::executor::block_on(
            provider.generate(request.clone(), dispatch).try_collect::<Vec<_>>(),
        )
        .unwrap();
        let recorded = events
            .iter()
            .find_map(|event| match event {
                ModelEvent::Completed { metadata } => {
                    crate::accounting::charged_input(adapter, metadata)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{adapter:?} fixture records no input usage"));
        rows.push(Row {
            counter: format!("{adapter:?} count route, recorded usage"),
            counted: count(&provider, &request),
            charged: recorded,
        });
        // The provider's own count of each catalog request.
        for skills in [0, 1, 40, 300] {
            let request = with_system(&request, &catalog(skills));
            rows.push(Row {
                counter: format!("{adapter:?} count route, {skills} skills"),
                counted: count(&provider, &request),
                charged: route_tokens(&provider, adapter, &request),
            });
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        // Codex has no count route; its counter anchors on recorded usage.
        let (provider, _) = provider(Adapter::Responses, 200);
        let (request, _) = request(&provider).unwrap();
        for skills in [0, 1, 40, 300] {
            let request = with_system(&request, &catalog(skills));
            let charged = 5 + 7 * skills as u64;
            let counter = crate::codex_count::Counter::default();
            counter
                .observe(&request, &json!({"usage":{"input_tokens":charged,"output_tokens":3}}))
                .unwrap();
            rows.push(Row {
                counter: format!("Codex usage anchor, {skills} skills"),
                counted: counter.count(&request).unwrap(),
                charged,
            });
        }
    }
    rows
}

#[test]
fn every_counter_admits_only_requests_whose_charge_fits() {
    let rows = rows();
    assert_eq!(rows.len(), 4 * 5 + if cfg!(target_arch = "wasm32") { 0 } else { 4 });
    assert_eq!(violations(&rows), Vec::<&str>::new());
    // A counter reporting exactly the charge is admitted at the boundary.
    let exact: Vec<_> = rows
        .iter()
        .map(|row| Row {
            counter: row.counter.clone(),
            counted: row.charged,
            charged: row.charged,
        })
        .collect();
    assert_eq!(violations(&exact), Vec::<&str>::new());
    // Negative control: undercounting any row's charge by one token is rejected.
    let under: Vec<_> = rows
        .iter()
        .map(|row| Row {
            counter: row.counter.clone(),
            counted: row.charged - 1,
            charged: row.charged,
        })
        .collect();
    assert_eq!(violations(&under).len(), under.len());
}
