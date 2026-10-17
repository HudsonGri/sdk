use super::*;
use crate::verification::{Fault, check, fault_is, set_fault};

pub(crate) fn verify(broken: bool) {
    check(move || {
        let stream = super::query_model::fixture(false)?;
        let path = StreamPath::new("approval")?;
        // Each actor has already flushed its own snapshot (1 or 2). Only the
        // real receipt publication is raced; journal I/O adds no interleavings
        // to this boundary and is covered by the crash-cut tests.
        let newer = loom::thread::spawn({
            let stream = stream.clone();
            let path = path.clone();
            move || {
                stream
                    .receipt_for_flushed_snapshot(path, 2, 2)
                    .unwrap()
                    .unwrap()
            }
        });
        if broken {
            set_fault(Fault::MixedReceiptCut);
        }
        let older = stream.receipt_for_flushed_snapshot(path.clone(), 1, 1)?;
        newer.join().unwrap();
        assert!(older.is_some(), "flushed snapshot lost its receipt");
        assert_eq!(
            stream.inner.durable_tails.lock().unwrap().get(&path),
            Some(&2)
        );
        assert!(stream.receipt_for_flushed_snapshot(path, 1, 2)?.is_none());
        assert_eq!(fault_is(Fault::MixedReceiptCut), broken);
        Ok(())
    });
}
