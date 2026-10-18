//! Deepest stack use of a callback on its calling thread.
//!
//! The thread's stack is painted with a pattern below the callback's first
//! frame; afterwards the deepest overwritten byte is the most stack any
//! frame of the callback, or anything it polled, ever used. The result is
//! the real frame depth on the target being built, including every poll
//! frame of the futures a callback drives, which a type size cannot show.

/// Bytes painted below the callback's first frame: the largest measurable use.
pub const PAINTED: usize = 512 << 10;
const PATTERN: u8 = 0xA5;

#[inline(never)]
fn paint<const BYTES: usize>() -> usize {
    let mut region = [PATTERN; BYTES];
    std::hint::black_box(&mut region);
    region.as_ptr() as usize
}

/// Runs `callback` on the calling thread and returns its result with the deepest
/// stack it used, in bytes. Exhausting the painted region returns `usize::MAX`,
/// so saturation cannot pass a budget equal to [`PAINTED`].
pub fn deepest_stack<T>(callback: impl FnOnce() -> T) -> (T, usize) {
    measure::<PAINTED, _>(callback)
}

#[inline(never)]
fn measure<const BYTES: usize, T>(callback: impl FnOnce() -> T) -> (T, usize) {
    let lowest = paint::<BYTES>();
    let result = callback();
    let untouched = (0..BYTES)
        .take_while(|offset| {
            // SAFETY: the region is this thread's own stack below
            // every live frame, still mapped, and written by
            // nothing else; it is only read, byte by byte.
            let byte = unsafe { std::ptr::read_volatile((lowest + offset) as *const u8) };
            byte == PATTERN
        })
        .count();
    (
        result,
        if untouched == 0 {
            usize::MAX
        } else {
            BYTES - untouched
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline(never)]
    fn hold<const BYTES: usize>() -> u8 {
        let mut frame = [1_u8; BYTES];
        std::hint::black_box(&mut frame);
        frame[BYTES / 2]
    }

    #[test]
    fn a_held_frame_is_measured_and_an_empty_callback_is_not() {
        let ((), empty) = deepest_stack(|| ());
        let (_, held) = deepest_stack(hold::<{ 256 << 10 }>);
        assert!(empty < 16 << 10, "an empty callback used {empty} bytes");
        assert!(held >= 256 << 10, "a 256 KiB frame measured {held} bytes");
        assert!(held < 320 << 10, "a 256 KiB frame measured {held} bytes");
    }
}
