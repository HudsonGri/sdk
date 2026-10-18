//! Line selection and `cat -n` rendering for model-facing text reads.
use super::super::edit::{charge, reserve_string};
use crate::{NativeCustody, Result};

/// Width of the right-aligned line-number column, as in `cat -n`.
const NUMBER_WIDTH: usize = 6;

/// Rendering of a selection that contains no lines (only an empty file).
pub(super) const EMPTY_FILE: &str = "(empty file)";

/// Renders selected lines as `cat -n` text: a right-aligned one-based number,
/// a tab and the line without its `\n` terminator, joined by `\n`.
pub(super) fn numbered(text: &str, offset: u64, work: &mut NativeCustody) -> Result<String> {
    if text.is_empty() {
        return Ok(EMPTY_FILE.to_owned());
    }
    let capacity = rendered_len(text, offset);
    let mut output = String::new();
    reserve_string(work, &mut output, capacity)?;
    charge(work, capacity as u64, 0)?;
    for (index, line) in (offset..).zip(lines(text)) {
        if !output.is_empty() {
            output.push('\n');
        }
        use std::fmt::Write as _;
        // Writing to a reserved String cannot fail.
        let _ = write!(output, "{index:>NUMBER_WIDTH$}\t{line}");
    }
    Ok(output)
}

/// Exact byte length of `numbered(text, offset)`, so callers can bound the
/// rendered projection before producing it.
pub(super) fn rendered_len(text: &str, offset: u64) -> usize {
    if text.is_empty() {
        return EMPTY_FILE.len();
    }
    (offset..)
        .zip(lines(text))
        .map(|(index, line)| digits(index).max(NUMBER_WIDTH) + 1 + line.len() + 1)
        .sum::<usize>()
        - 1
}

fn lines(text: &str) -> std::str::Split<'_, char> {
    text.strip_suffix('\n').unwrap_or(text).split('\n')
}

fn digits(mut value: u64) -> usize {
    let mut count = 1;
    while value >= 10 {
        value /= 10;
        count += 1;
    }
    count
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn model_lines(source: &str) -> Vec<&str> {
        source.split_inclusive('\n').collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        // Property: removing the number column restores the exact selected
        // text (up to its final terminator), and numbers count up from offset.
        #[test]
        fn numbering_round_trips(source in "[ab\\n\\ré🦀]{1,24}", offset in 1_u64..2_000_000) {
            let rendered = numbered(&source, offset, &mut None).unwrap();
            prop_assert_eq!(rendered.len(), rendered_len(&source, offset));
            let mut restored = Vec::new();
            for (expected, line) in (offset..).zip(rendered.split('\n')) {
                let (number, text) = line.split_once('\t').unwrap();
                prop_assert!(number.len() >= NUMBER_WIDTH);
                prop_assert_eq!(number.trim_start().parse::<u64>().unwrap(), expected);
                restored.push(text);
            }
            let mut joined = restored.join("\n");
            if source.ends_with('\n') {
                joined.push('\n');
            }
            prop_assert_eq!(joined, source);
        }
    }

    #[test]
    fn empty_files_render_a_marker_that_no_numbered_line_can_equal() -> Result<()> {
        assert_eq!(numbered("", 1, &mut None)?, EMPTY_FILE);
        assert_eq!(rendered_len("", 1), EMPTY_FILE.len());
        assert_eq!(
            numbered("(empty file)", 1, &mut None)?,
            "     1\t(empty file)"
        );
        assert_eq!(
            numbered("a\r\nb\n", 9, &mut None)?,
            "     9\ta\r\n    10\tb"
        );
        Ok(())
    }
}
