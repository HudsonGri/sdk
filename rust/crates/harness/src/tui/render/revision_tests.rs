use super::tail_tests::{TASK, answer, fixture, owner, put};
use super::*;

fn visits<const FULL: bool>(tools: usize) {
    let mut views = fixture(tools);
    answer(&mut views, 0);
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &owner());
    let before = renderer.task.tool_visits;
    for sequence in 1..=8 {
        answer(&mut views, sequence);
        if FULL {
            renderer.task.cut = 0;
        }
        renderer.update(&views, &owner());
    }
    assert_eq!(
        renderer.task.tool_visits, before,
        "answer appends must not visit unchanged tools"
    );
}

#[test]
fn answer_appends_skip_every_unchanged_tool() {
    for tools in [8, 64, 128] {
        visits::<false>(tools);
    }
}
#[test]
#[should_panic(expected = "answer appends must not visit unchanged tools")]
fn negative_control_refreshing_the_full_tool_index_visits_every_tool() {
    visits::<true>(64);
}

#[test]
fn one_changed_tool_matches_fresh_projection() {
    let mut views = fixture(64);
    let mut renderer = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    renderer.update(&views, &owner());
    let before = renderer.task.tool_visits;
    put(
        &mut views,
        &format!(".tasks/{TASK}/turns/0/tools/call031/stdout/1"),
        b" appended".to_vec(),
        None,
    );
    renderer.update(&views, &owner());
    assert_eq!(renderer.task.tool_visits - before, 1);
    let mut fresh = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    fresh.update(&views, &owner());
    assert_eq!(renderer.screen().tools, fresh.screen().tools);
}
