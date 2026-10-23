use super::*;

/// One current materializer output per root and path, independent of history length.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NativePostimage {
    pub(crate) canonical: [u8; 32],
    pub(crate) native: [u8; 32],
    pub(crate) metadata: crate::WorkspaceMetadata,
}

impl LocalCoreStateStore {
    pub(crate) fn same_namespace(&self, other: &Self) -> bool {
        self.root == other.root
    }

    pub(crate) async fn native_postimage(
        &self,
        key: [u8; 32],
    ) -> Result<Option<NativePostimage>, LocalCoreStateStoreError> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record("native-postimages", &key)?;
            with_lock(&mut paths, |paths| {
                read_recoverable(paths).map(|value| value.map(|(value, _)| value))
            })
        })
        .await
    }

    pub(crate) async fn record_native_postimage(
        &self,
        key: [u8; 32],
        image: NativePostimage,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record("native-postimages", &key)?;
            with_lock(&mut paths, |paths| write_journaled(paths, &image))
        })
        .await
    }
}
