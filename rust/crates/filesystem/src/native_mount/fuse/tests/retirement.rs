use super::*;
use crate::native_mount::NativeSessionStopWork;
use std::sync::mpsc::{Receiver, Sender};

enum Event {
    Polling,
    Retired(Result<(), NativeMountError>),
}

#[derive(Clone, Copy)]
enum Retirement {
    Meter,
    AcceptedWait,
    LastOwner,
}

fn waiting_mount(
    polling: Sender<Event>,
    acknowledged: Receiver<()>,
    mut interrupted: bool,
) -> std::io::Result<super::super::mount::Mount> {
    super::super::mount::Mount::accepted_unmount(move |timeout| {
        if timeout == 0 {
            // A probe observes the connection before the later acknowledgment.
            return Ok(false);
        }
        assert_eq!(timeout, -1);
        if std::mem::take(&mut interrupted) {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        polling
            .send(Event::Polling)
            .map_err(|_| std::io::ErrorKind::BrokenPipe)?;
        acknowledged
            .recv()
            .map_err(|_| std::io::ErrorKind::BrokenPipe)?;
        Ok(true)
    })
}

// The kernel accepts unmount before its last connection reference disappears.
// Drive those two events separately through the real session and its original
// finite cleanup reservation, with no scheduler timing or live FUSE dependency.
#[test]
fn accepted_unmount_waits_for_kernel_ack_using_birth_credit()
-> Result<(), Box<dyn std::error::Error>> {
    for interrupted in [false, true] {
        interleaving(Retirement::AcceptedWait, interrupted)?;
    }
    Ok(())
}

#[test]
fn accepted_unmount_waits_for_kernel_ack_on_meter() -> Result<(), Box<dyn std::error::Error>> {
    for interrupted in [false, true] {
        interleaving(Retirement::Meter, interrupted)?;
    }
    Ok(())
}

#[test]
fn last_session_drop_waits_for_kernel_ack_using_birth_credit()
-> Result<(), Box<dyn std::error::Error>> {
    for interrupted in [false, true] {
        interleaving(Retirement::LastOwner, interrupted)?;
    }
    Ok(())
}

fn interleaving(mode: Retirement, interrupted: bool) -> Result<(), Box<dyn std::error::Error>> {
    let meter = matches!(mode, Retirement::Meter);
    let (source, _) = shared_sources(posix_config())?;
    let core = projection_core(
        Arc::new(source),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 1,
            backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let cleanup = original.reserve_work(crate::WorkCounters {
        backend_read_operations: u64::from(!meter),
        backend_write_operations: 1,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let allocation = original.claim(
        std::mem::size_of::<Mutex<FuseSessionState>>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let (events, observed) = std::sync::mpsc::channel();
    let polling = events.clone();
    let (acknowledge, acknowledged) = std::sync::mpsc::channel();
    // Model a successful first unmount, with acknowledgment still withheld.
    cleanup.started_io(
        crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        None,
    )?;
    let mount = waiting_mount(polling, acknowledged, interrupted)?;
    core.stop();
    if !meter {
        // Losing the original caller cannot revoke already accepted cleanup.
        original.cancellation().cancel();
    }
    let state = Arc::new(Mutex::new(FuseSessionState {
        path: super::super::FuseOwned::empty(vec![0]),
        before: Some((0, 0)),
        mount,
        worker: None,
        device: Some(std::fs::File::open("/dev/null")?.into()),
        notifier: None,
        identity: None,
        retirement: None,
        cleanup,
        failure: None,
        _allocation: allocation,
    }));
    let mut facade = super::super::FuseSession {
        core,
        state: Arc::clone(&state),
        invalidator: None,
    };
    let caller = original.clone();
    let worker = std::thread::spawn(move || {
        let result = match mode {
            Retirement::Meter => facade.stop(&NativeSessionStopWork::Meter(&caller)),
            Retirement::AcceptedWait => {
                let wait = crate::CancellationToken::new();
                facade.stop(&NativeSessionStopWork::AcceptedWait(&wait))
            }
            Retirement::LastOwner => {
                let mut owner = crate::NativeMountSession::new(
                    crate::NativeMountRequest {
                        mount_id: crate::MountId::new(),
                        volume_id: crate::VolumeId::new(),
                        destination: Path::new("/not-mounted").to_path_buf(),
                        writable: false,
                    },
                    None,
                    None,
                );
                owner.driver = Some(facade);
                drop(owner);
                Ok(())
            }
        };
        events.send(Event::Retired(result)).unwrap();
    });
    match observed.recv()? {
        Event::Polling => {
            assert!(
                matches!(state.try_lock(), Err(std::sync::TryLockError::WouldBlock)),
                "session ownership escaped before the kernel acknowledgment"
            );
            acknowledge.send(())?;
        }
        Event::Retired(result) => {
            worker.join().map_err(|_| "retirement worker panicked")?;
            panic!("retirement returned before the kernel acknowledgment: {result:?}");
        }
    }
    let Event::Retired(result) = observed.recv()? else {
        panic!("retirement polled again instead of awaiting acknowledgment");
    };
    worker.join().map_err(|_| "retirement worker panicked")?;
    result?;
    assert!(
        state
            .lock()
            .map_err(|_| "retirement state poisoned")?
            .is_retired()
    );
    let receipt = original.receipt();
    assert_eq!(receipt.backend_write_operations, 1);
    assert_eq!(receipt.backend_read_operations, 1);
    Ok(())
}
