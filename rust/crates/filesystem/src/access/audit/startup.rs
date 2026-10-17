use crate::sync::Mutex;

// Terminal startup lock: factory must not take the queue lock. Worker startup
// owns only the queue; it never calls back into this cell or the recorder.
pub(super) struct Startup<T>(Mutex<Option<T>>);
impl<T> Default for Startup<T> {
    fn default() -> Self {
        Self(Mutex::new(None))
    }
}
impl<T: Clone> Startup<T> {
    pub(super) fn started(&self) -> Result<Option<T>, super::AuditError> {
        Ok(self.0.lock().map_err(|_| super::AuditError)?.clone())
    }

    pub(super) fn get(
        &self,
        start: impl FnOnce() -> Result<T, super::AuditError>,
    ) -> Result<T, super::AuditError> {
        let mut value = self.0.lock().map_err(|_| super::AuditError)?;
        if let Some(value) = value.as_ref() {
            return Ok(value.clone());
        }
        let created = start()?;
        *value = Some(created.clone());
        Ok(created)
    }
    #[cfg(all(test, acyclic_fs_loom))]
    pub(super) fn get_split(
        &self,
        start: impl FnOnce() -> Result<T, super::AuditError>,
    ) -> Result<T, super::AuditError> {
        if let Some(value) = self.0.lock().map_err(|_| super::AuditError)?.as_ref() {
            return Ok(value.clone());
        }
        let created = start()?;
        let mut value = self.0.lock().map_err(|_| super::AuditError)?;
        Ok(value.get_or_insert(created).clone())
    }
}
