//! Single event-loop owner; no shared mutable client or lock crosses provider I/O.
use super::{
    file_client::{admission_error, error},
    query::WasmTaskVolume,
};
use crate::client::{mailbox, publication_recovery::PublicationRecovery};
use crate::{
    client::{FileClient, FileClientError},
    task_volume::{Cursor, FileKey, Query},
};
use acyclic_stream::{
    BoxProviderFuture, BoxProviderStream, StreamError, StreamProvider,
    runtime::{Executor, PlatformExecutor},
    wire,
};
use futures::{FutureExt as _, StreamExt as _};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use wasm_bindgen::prelude::*;

type Inner = FileClient<dyn StreamProvider, dyn StreamProvider>;
enum Wake {
    Command(Option<Command>),
    Published(Option<Result<(), StreamError>>),
    Retry,
}
pub(super) enum Command {
    Enqueue {
        key: FileKey,
        bytes: Vec<u8>,
        reply: mailbox::Sender<Result<(), JsValue>>,
    },
    Flush(mailbox::Sender<Result<usize, JsValue>>),
    Idle(mailbox::Sender<Result<(), JsValue>>),
}
pub(super) struct State {
    pub(super) pending: Cell<usize>,
    pub(super) cursor: RefCell<Cursor>,
    pub(super) error: RefCell<JsValue>,
}
impl State {
    fn refresh(&self, inner: &Inner) {
        self.pending.set(inner.pending());
        *self.cursor.borrow_mut() = inner.cursor().clone();
    }
}
fn retry_after(
    error: &FileClientError,
    remote: bool,
    recovery: &mut PublicationRecovery,
) -> Option<BoxProviderFuture<'static, ()>> {
    let delay = recovery.failed(error, remote)?;
    Some(PlatformExecutor.sleep(u32::try_from(delay.as_millis()).ok()?))
}

// Subscribe from the captured cut before the worker can admit more commands.
// Both endpoints can delay durability, independently of data or reconnects.
async fn publications(
    volume: &WasmTaskVolume,
) -> Result<BoxProviderStream<'static, Result<(), StreamError>>, JsValue> {
    // Local providers also expose sessions for rendering. They have no remote
    // recovery to drive: their file clients retain explicit-flush semantics.
    let Some(connections) = volume.connections.clone() else {
        return Ok(Box::pin(futures::stream::pending()));
    };
    let frames: BoxProviderStream<'static, Result<(), StreamError>> =
        if let Some(session) = &volume.session {
            let cursor = volume.volume.cut(&Query::Root).await.map_err(error)?;
            let frames = session
                .subscribe(wire::SessionSubscription {
                    credential: 0,
                    filter: None,
                    channel: None,
                    resume: Some(wire::SessionCursor {
                        next: cursor.position,
                        opaque: Default::default(),
                    }),
                    kind: Some(wire::session_subscription::Kind::Follow(
                        wire::SessionFollow {
                            resource: acyclic_stream::wire_codec::session::stream_resource(
                                volume.volume.path().as_str(),
                            ),
                            delivery: wire::SessionDelivery::All as i32,
                            preceding_commit_id: if cursor.position == 0 {
                                Default::default()
                            } else {
                                cursor.commit.to_vec().into()
                            },
                        },
                    )),
                })
                .await
                .map_err(error)?;
            // Retain standalone watermarks, including a save after the last record.
            Box::pin(frames.map(|frame| frame.map(|_| ())))
        } else {
            Box::pin(futures::stream::pending())
        };
    let recovered =
        futures::stream::try_unfold((connections, 0), |(connections, generation)| async move {
            let next = connections.changed(generation).await?;
            Ok(Some(((), (connections, next))))
        });
    Ok(Box::pin(futures::stream::select(frames, recovered)))
}

pub(super) async fn start(
    mut inner: Inner,
    outbox: &WasmTaskVolume,
    target: &WasmTaskVolume,
) -> Result<(mailbox::Sender<Command>, Rc<State>), JsValue> {
    let remote = outbox.connections.is_some() || target.connections.is_some();
    // One pool multiplexes both follows; neither needs a renderer to consume credit.
    let mut changes: BoxProviderStream<'static, Result<(), StreamError>> = Box::pin(
        futures::stream::select(publications(outbox).await?, publications(target).await?),
    );
    let state = Rc::new(State {
        pending: Cell::new(inner.pending()),
        cursor: RefCell::new(inner.cursor().clone()),
        error: RefCell::new(JsValue::UNDEFINED),
    });
    let (commands, mut receiver) = mailbox::channel();
    let current = state.clone();
    wasm_bindgen_futures::spawn_local(async move {
        let mut recovery = PublicationRecovery::new();
        let mut retry: Option<BoxProviderFuture<'static, ()>> = None;
        let mut idle: Option<mailbox::Sender<Result<(), JsValue>>> = None;
        loop {
            let event = {
                let retry_ready = async {
                    match &mut retry {
                        Some(deadline) => deadline.await,
                        None => futures::future::pending().await,
                    }
                }
                .fuse();
                futures::pin_mut!(retry_ready);
                futures::select! {
                    command = receiver.recv().fuse() => Wake::Command(command),
                    change = changes.next().fuse() => Wake::Published(change),
                    () = retry_ready => Wake::Retry,
                }
            };
            let automatic = match event {
                Wake::Command(command) => {
                    let Some(command) = command else {
                        return;
                    };
                    match command {
                        Command::Enqueue { key, bytes, reply } => {
                            let result = inner
                                .enqueue_with(key, bytes.len() as u64, || {
                                    crate::client::validate_offline_request(&bytes)?;
                                    Ok(bytes.into())
                                })
                                .await;
                            if let Err(failure @ FileClientError::Unavailable { .. }) = &result {
                                retry = retry_after(failure, remote, &mut recovery);
                                *current.error.borrow_mut() = error(failure);
                            }
                            current.refresh(&inner);
                            let admitted = result.is_ok();
                            let _ = reply.reply(result.map_err(admission_error));
                            admitted && remote && recovery.on_publication()
                        }
                        Command::Flush(reply) => {
                            let result = inner.flush().await;
                            match &result {
                                Ok(_) => {
                                    retry = None;
                                    recovery.succeeded();
                                    *current.error.borrow_mut() = JsValue::UNDEFINED;
                                }
                                Err(failure) => {
                                    retry = retry_after(failure, remote, &mut recovery);
                                    *current.error.borrow_mut() = error(failure);
                                }
                            }
                            current.refresh(&inner);
                            let _ = reply.reply(result.map_err(admission_error));
                            false
                        }
                        Command::Idle(reply) => {
                            if inner.pending() == 0 {
                                let _ = reply.reply(Ok(()));
                            } else if recovery.stopped() {
                                let _ = reply.reply(Err(current.error.borrow().clone()));
                            } else if idle.as_ref().is_some_and(|reply| !reply.is_closed()) {
                                let _ =
                                    reply.reply(Err(error("an idle wait is already registered")));
                            } else {
                                idle = Some(reply);
                            }
                            false
                        }
                    }
                }
                Wake::Retry => recovery.on_timer(),
                Wake::Published(change) => {
                    let ended = change.is_none();
                    if let Some(Err(failure)) = change {
                        *current.error.borrow_mut() = error(failure);
                    }
                    if ended {
                        changes = Box::pin(futures::stream::pending());
                    }
                    !ended && inner.pending() > 0 && recovery.on_publication()
                }
            };
            if automatic {
                // Rejections retire in Rust, then later requests proceed.
                // Indeterminate HTTP publication retries the same immutable identity
                // with bounded error backoff. Durability waits remain push-driven.
                retry = None;
                loop {
                    match inner.flush().await {
                        Ok(_) => {
                            recovery.succeeded();
                            *current.error.borrow_mut() = JsValue::UNDEFINED;
                            break;
                        }
                        Err(failure @ FileClientError::Rejected { .. }) => {
                            *current.error.borrow_mut() = admission_error(failure);
                        }
                        Err(failure) => {
                            retry = retry_after(&failure, true, &mut recovery);
                            *current.error.borrow_mut() = admission_error(failure);
                            break;
                        }
                    }
                }
                current.refresh(&inner);
            }
            if inner.pending() == 0
                && let Some(idle) = idle.take()
            {
                let _ = idle.reply(Ok(()));
            } else if recovery.stopped()
                && let Some(idle) = idle.take()
            {
                // A terminal failure has no future timer or publication that can
                // finish this wait. Return its retained request identity to the
                // caller instead of leaving an unowned idle waiter behind.
                let _ = idle.reply(Err(current.error.borrow().clone()));
            }
        }
    });
    Ok((commands, state))
}
