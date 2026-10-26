use super::*;

fn future_bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[test]
fn checkout_dispatch_frame_stays_bounded() {
    let bytes = future_bytes(
        |(tools, command, context): (&CheckoutTools, PreparedCommand, &ToolContext)| {
            tools.run_checkout_command(command, context)
        },
    );
    eprintln!("checkout dispatch future: {bytes} bytes");
    assert!(bytes <= 8192, "checkout dispatch future: {bytes}");
}
