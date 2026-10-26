use super::*;

fn future_bytes<A, F: Future>(_: impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[test]
fn child_preparation_dispatch_frame_stays_bounded() {
    let bytes = future_bytes(
        |(commands, context, fence, command, payload): (
            &Commands<'_>,
            TaskContext,
            LeaseFence,
            WorkflowCommand,
            Value,
        )| { commands.run(context, fence, command, payload, false) },
    );
    eprintln!("child preparation dispatch future: {bytes} bytes");
    assert!(bytes <= 8192, "child preparation dispatch future: {bytes}");
}
