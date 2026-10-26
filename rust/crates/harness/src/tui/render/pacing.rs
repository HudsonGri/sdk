use super::ClientRenderer;
use std::time::Duration;
use tokio::time::Instant;

pub(super) struct Pacer {
    deadline: Instant,
    dirty: bool,
}

impl Default for Pacer {
    fn default() -> Self {
        Self {
            deadline: Instant::now(),
            dirty: false,
        }
    }
}

impl Pacer {
    /// Wait for the next presentation slot without polling an idle terminal.
    pub(super) async fn wait_frame(&self) {
        tokio::time::sleep_until(self.deadline).await;
    }

    /// Successful presentation starts the next 60 Hz interval. Failed output
    /// never consumes a slot or clears a pending publication.
    pub(super) fn frame_presented(&mut self) {
        self.deadline = Instant::now() + Duration::from_nanos(16_666_667);
        self.dirty = false;
    }

    pub(super) fn request_frame(&mut self) {
        self.dirty = true;
    }

    pub(super) fn frame_deadline(&self) -> Option<Instant> {
        self.dirty.then_some(self.deadline)
    }

    pub(super) fn frame_ready(&mut self) {
        self.dirty = false;
    }
}

impl ClientRenderer {
    /// Wait for the one shared presentation clock's next frame.
    pub async fn wait_frame(&self) {
        self.pacing.wait_frame().await;
    }

    /// Successful device output consumes a presentation slot.
    pub fn frame_presented(&mut self) {
        self.pacing.frame_presented();
    }

    pub(crate) fn request_frame(&mut self) {
        self.pacing.request_frame();
    }

    pub(crate) fn frame_deadline(&self) -> Option<Instant> {
        self.pacing.frame_deadline()
    }

    pub(crate) fn frame_ready(&mut self) {
        self.pacing.frame_ready();
    }
}
