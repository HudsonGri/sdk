//! One coordinator follow routes ownership changes to interested native commands.
use super::*;
use crate::scheduler::SchedulerEvent;
use std::sync::{Mutex, MutexGuard};
use tokio::sync::Notify;

#[cfg(test)]
mod tests;

#[derive(Default)]
struct Subscribers {
    next: u64,
    by_operation: BTreeMap<OperationId, BTreeMap<u64, Arc<Notify>>>,
    failed: Option<String>,
}

pub(super) struct OwnerChangeHub {
    subscribers: Arc<Mutex<Subscribers>>,
    driver: tokio::task::JoinHandle<()>,
}

fn locked(subscribers: &Mutex<Subscribers>) -> MutexGuard<'_, Subscribers> {
    subscribers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl OwnerChangeHub {
    async fn open<P: StreamProvider + 'static>(stream: &Arc<P>, after: u64) -> Result<Self> {
        let mut changes = DistributedCoordinator::event_changes(stream, after).await?;
        let subscribers = Arc::new(Mutex::new(Subscribers::default()));
        let active = Arc::clone(&subscribers);
        let driver = tokio::spawn(async move {
            loop {
                match changes.try_next().await {
                    Ok(Some(event)) => locked(&active).publish(&event),
                    result => {
                        let cause = match result {
                            Err(cause) => cause.to_string(),
                            _ => "coordinator ownership follow ended".into(),
                        };
                        locked(&active).fail(cause);
                        return;
                    }
                }
            }
        });
        Ok(Self {
            subscribers,
            driver,
        })
    }

    fn subscribe(&self, operations: Vec<OperationId>) -> Result<Registration> {
        let ready = Arc::new(Notify::new());
        let mut subscribers = locked(&self.subscribers);
        subscribers.check()?;
        let id = subscribers.next;
        subscribers.next = id
            .checked_add(1)
            .ok_or_else(|| Error::Storage("owner subscription identity exhausted".into()))?;
        for operation in &operations {
            subscribers
                .by_operation
                .entry(*operation)
                .or_default()
                .insert(id, Arc::clone(&ready));
        }
        Ok(Registration {
            subscribers: Arc::clone(&self.subscribers),
            id,
            operations,
            ready,
        })
    }
}

impl Drop for OwnerChangeHub {
    fn drop(&mut self) {
        locked(&self.subscribers).fail("coordinator ownership host closed".into());
        self.driver.abort();
    }
}

impl Subscribers {
    fn check(&self) -> Result<()> {
        match &self.failed {
            Some(cause) => Err(Error::Storage(cause.clone())),
            None => Ok(()),
        }
    }

    fn notify(&self, operation: OperationId) {
        if let Some(subscribers) = self.by_operation.get(&operation) {
            for ready in subscribers.values() {
                ready.notify_one();
            }
        }
    }

    fn publish(&self, event: &SchedulerEvent) {
        // Model claims and checkpoints preserve the fence and cancellation.
        // Parent activity therefore does not wake every running descendant.
        if matches!(
            event,
            SchedulerEvent::ModelDispatchClaimed { .. } | SchedulerEvent::Checkpointed { .. }
        ) {
            return;
        }
        self.notify(event.operation_id());
        if let SchedulerEvent::Orchestrated { cancel, .. } = event {
            for operation in cancel {
                self.notify(*operation);
            }
        }
    }

    fn fail(&mut self, cause: String) {
        self.failed = Some(cause);
        for subscribers in self.by_operation.values() {
            for ready in subscribers.values() {
                ready.notify_one();
            }
        }
    }
}

struct Registration {
    subscribers: Arc<Mutex<Subscribers>>,
    id: u64,
    operations: Vec<OperationId>,
    ready: Arc<Notify>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut subscribers = locked(&self.subscribers);
        for operation in &self.operations {
            if let Some(entries) = subscribers.by_operation.get_mut(operation) {
                entries.remove(&self.id);
                if entries.is_empty() {
                    subscribers.by_operation.remove(operation);
                }
            }
        }
    }
}

/// A native command's live ownership subscription. Wakes are hints: dispatch
/// and publication still authenticate the original journal fence.
#[doc(hidden)]
pub struct OwnerChanges {
    registration: Registration,
    cancellation: Option<acyclic_fs::CancellationToken>,
    deadline: Option<u64>,
    clock: Arc<dyn UnixMillisClock>,
}

#[derive(Clone)]
pub(crate) struct OwnerSignals {
    cancellation: Option<acyclic_fs::CancellationToken>,
    deadline: Option<u64>,
    clock: Arc<dyn UnixMillisClock>,
}

impl OwnerSignals {
    pub(crate) fn needs_verification(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(acyclic_fs::CancellationToken::is_cancelled)
            || self
                .deadline
                .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
    }
}

impl OwnerChanges {
    pub(crate) fn signals(&self) -> OwnerSignals {
        OwnerSignals {
            cancellation: self.cancellation.clone(),
            deadline: self.deadline,
            clock: Arc::clone(&self.clock),
        }
    }

    /// Waits for this task, its ancestors, invocation cancellation or deadline.
    pub async fn changed(&mut self) -> Result<()> {
        locked(&self.registration.subscribers).check()?;
        let cancelled = async {
            match &self.cancellation {
                Some(token) => token.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let deadline = async {
            match self.deadline {
                Some(deadline) => {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        deadline.saturating_sub(self.clock.now_unix_millis()),
                    ))
                    .await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = self.registration.ready.notified() => {},
            () = cancelled => {},
            () = deadline => {},
        }
        locked(&self.registration.subscribers).check()
    }
}

impl<P: StreamProvider + 'static> TaskJournalOwner<P> {
    /// Registers before the caller's initial fence verification, closing the
    /// gap between verifying an owner and starting its native command.
    #[doc(hidden)]
    pub async fn owner_changes(&self) -> Result<OwnerChanges> {
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        let hub = self
            .host
            .owner_changes
            .get_or_try_init(|| OwnerChangeHub::open(&self.host.stream, coordinator.revision()))
            .await?;
        let mut operations = Vec::new();
        let mut operation = Some(self.operation_id());
        while let Some(current) = operation {
            operations.push(current);
            operation = coordinator
                .scheduler()
                .operation(current)
                .ok_or_else(|| Error::NotFound("owner subscription operation".into()))?
                .spec
                .parent
                .as_ref()
                .map(|parent| parent.operation_id);
        }
        let registration = hub.subscribe(operations)?;
        Ok(OwnerChanges {
            registration,
            cancellation: self.invocation_cancellation.clone(),
            deadline: self.admission.run_limits.deadline_epoch_ms,
            clock: Arc::clone(&self.host.clock),
        })
    }
}
