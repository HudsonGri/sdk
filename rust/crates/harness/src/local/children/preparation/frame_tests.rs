use super::*;

fn future_bytes<F: Future>(_: impl FnOnce() -> F) -> usize {
    std::mem::size_of::<F>()
}

#[test]
fn child_preparation_dispatch_frame_stays_bounded() {
    let arguments: Option<(
        &Commands<'_>,
        TaskContext,
        LeaseFence,
        WorkflowCommand,
        Value,
    )> = None;
    let bytes = future_bytes(|| {
        let (commands, context, fence, command, payload) = arguments.unwrap();
        commands.run(context, fence, command, payload, false)
    });
    eprintln!("child preparation dispatch future: {bytes} bytes");
    assert!(bytes <= 8192, "child preparation dispatch future: {bytes}");
}
