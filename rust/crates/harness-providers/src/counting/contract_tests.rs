//! Admission arithmetic under an assumed counting contract. These rows drive
//! the real adapters with synthetic count routes and usage. They test wiring
//! and admission boundaries, not live provider tokenization or billing.
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
        output_tokens: Some(64),
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

/// Every shipping counter against the fixture service's synthetic charges.
fn rows() -> Vec<Row> {
    let mut rows = Vec::new();
    for (adapter, _) in fixtures() {
        let (provider, _) = provider(adapter, 200);
        let (request, dispatch) = request(&provider).unwrap();
        // Synthetic usage from the scripted response (independent of input).
        let events = futures::executor::block_on(
            provider
                .generate(request.clone(), dispatch)
                .try_collect::<Vec<_>>(),
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
        // The fixture service's byte-based count of each catalog request.
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
    rows.extend(codex_rows().into_iter().map(|(row, _)| row));
    rows
}

/// New input whose o200k_base token counts were recorded with tiktoken 0.9.0:
/// ASCII prose, then dense text (supplementary CJK, emoji ZWJ sequences and
/// base64) that costs more than one token per three bytes.
#[cfg(not(target_arch = "wasm32"))]
fn recorded_inputs() -> [(&'static str, String, u64, bool); 4] {
    let cjk: String = (0x20000..0x20010).filter_map(char::from_u32).collect();
    let emoji = "\u{1f9d1}\u{200d}\u{1f680}\u{1f469}\u{1f3fd}\u{200d}\u{1f52c}\u{1fae0}\u{1fabf}\u{1f9a9}\u{1f9cc}\u{1faf6}\u{1f3ff}\u{1faac}\u{1f6fb}\u{1f9d1}\u{1f3fb}\u{200d}\u{1f9af}";
    [
        (
            "ASCII",
            "Draft release notes from the merged pull requests and list breaking changes first. "
                .repeat(4),
            57,
            false,
        ),
        ("supplementary CJK", cjk.repeat(2), 96, true),
        ("emoji", emoji.repeat(3), 132, true),
        ("base64", BASE64.into(), 175, true),
    ]
}

#[cfg(not(target_arch = "wasm32"))]
const BASE64: &str = "pKvURIxJVi2CgRXROh/M6pJ/UrTVRZKX+LQ+QtqJI4vBNibkPcs43bCCSIkn7JBPtCBXRDmD6IWFF51QVRr+YqSr1ESMSVYtgoEV0TofzOqSf1K01UWSl/i0PkLaiSOLwTYm5D3LON2wgkiJJ+yQT7QgV0Q5g+iFhRedUFUa/mKkq9REjElWLYKBFdE6H8zqkn9StNVFkpf4tD5C2okji8E2JuQ9yzjdsIJIiSfskE+0IFdEOYPohYUXnVBVGv5i";

/// Tokens Responses charges to frame one more input message.
#[cfg(not(target_arch = "wasm32"))]
const MESSAGE_FRAMING: u64 = 4;

/// The Codex counter after recorded usage anchors a request: exact at the
/// anchor, then each request extending it with recorded input. A dense row
/// also carries its anchor's usage.
#[cfg(not(target_arch = "wasm32"))]
fn codex_rows() -> Vec<(Row, Option<u64>)> {
    let (provider, _) = provider(Adapter::Responses, 200);
    let (anchor, _) = request(&provider).unwrap();
    let mut rows = Vec::new();
    for skills in [0, 1, 40, 300] {
        let anchor = with_system(&anchor, &catalog(skills));
        // Synthetic anchor usage; only the delta text counts below are recorded.
        let usage = 5 + 7 * skills as u64;
        let counter = crate::codex_count::Counter::default();
        counter
            .observe(
                &anchor,
                &json!({"usage":{"input_tokens":usage,"output_tokens":3}}),
            )
            .unwrap();
        rows.push((
            Row {
                counter: format!("Codex usage anchor, {skills} skills"),
                counted: counter.count(&anchor).unwrap(),
                charged: usage,
            },
            None,
        ));
        for (name, text, tokens, dense) in recorded_inputs() {
            let mut extended = anchor.request().clone();
            extended.messages.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(text),
            });
            let extended =
                PreparedModelRequest::prepare(extended, Limits::default(), None).unwrap();
            rows.push((
                Row {
                    counter: format!("Codex delta, {skills} skills + {name}"),
                    counted: counter.count(&extended).unwrap(),
                    charged: usage + tokens + MESSAGE_FRAMING,
                },
                dense.then_some(usage),
            ));
        }
    }
    rows
}

#[test]
fn every_counter_admits_only_requests_whose_charge_fits() {
    let rows = rows();
    assert_eq!(
        rows.len(),
        4 * 5
            + if cfg!(target_arch = "wasm32") {
                0
            } else {
                4 * 5
            }
    );
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

/// Negative control: the former Codex delta, one token per three new bytes,
/// undercounts every dense recorded input and is rejected.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn a_third_of_a_token_per_byte_undercounts_dense_input() {
    let dense: Vec<_> = codex_rows()
        .into_iter()
        .filter_map(|(row, usage)| {
            usage.map(|usage| Row {
                counter: row.counter,
                counted: usage + (row.counted - usage).div_ceil(3),
                charged: row.charged,
            })
        })
        .collect();
    assert_eq!(dense.len(), 4 * 3);
    assert_eq!(violations(&dense).len(), dense.len());
}
