use crate::{
    AppendRequest, MemoryStream, OwnedBytes, StreamPath, StreamProvider, test_meter::Meter,
};
use std::sync::{Arc, atomic::Ordering};

type Result = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn qualify(provider: &impl StreamProvider) -> Result {
    let path = StreamPath::new("native-watermark")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"entry").into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let ordinary = provider.watermarks(path.clone()).await?;
    let original = Arc::new(Meter::default());
    let output = provider
        .native_watermarks(path.clone(), original.original())
        .await??;
    assert_eq!(*output.value(), ordinary);
    assert!(original.examined.load(Ordering::SeqCst) > 0);
    drop(output);
    original.assert_refunded();

    let denied = Arc::new(Meter::default());
    denied.deny_allocation.store(true, Ordering::SeqCst);
    let failure = provider
        .native_watermarks(path.clone(), denied.original())
        .await
        .err()
        .ok_or("native watermark bypassed original allocation denial")?;
    assert!(failure.get_ref().is_some_and(|cause| {
        cause
            .downcast_ref::<crate::test_meter::NativeDenial>()
            .is_some()
    }));
    assert_eq!(denied.examined.load(Ordering::SeqCst), 0);
    denied.assert_refunded();

    // The former ordinary observation ignores the denied original invocation.
    // This real provider call is the bypass negative control.
    assert_eq!(provider.watermarks(path.clone()).await?, ordinary);
    let denied = Arc::new(Meter::default());
    denied.examination_limit.store(0, Ordering::SeqCst);
    assert!(
        provider
            .native_watermarks(path, denied.original())
            .await
            .is_err()
    );
    denied.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn memory_watermarks_retain_original_admission() -> Result {
    qualify(&MemoryStream::default()).await
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_watermarks_retain_original_admission() -> Result {
    let directory = tempfile::tempdir()?;
    let provider =
        crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default()).await?;
    qualify(&provider).await
}
