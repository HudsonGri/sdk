use super::*;

fn future_bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
    std::mem::size_of::<F>()
}

#[test]
fn checkout_dispatch_frame_stays_bounded() {
    let arguments: Option<(&CheckoutTools, PreparedCommand, &ToolContext)> = None;
    let bytes = future_bytes(|| {
        let (tools, command, context) = arguments.unwrap();
        tools.run_checkout_command(command, context)
    });
    eprintln!("checkout dispatch future: {bytes} bytes");
    assert!(bytes <= 8192, "checkout dispatch future: {bytes}");
}
