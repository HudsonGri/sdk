//! Finds heap moves on budgeted paths that state no reason.
//!
//! A box on a path that holds an allocation budget is made through a charged
//! box, or is preceded within three lines by an `// Unbudgeted:` or a
//! `// Charged above:` comment that says why it needs no charge.

/// The one-based lines of `source`, before its first `#[cfg(test)]`, that
/// make a raw box or heap frame without a stated reason.
#[must_use]
pub fn unpaid(source: &str) -> Vec<usize> {
    let source = source.split("#[cfg(test)]").next().unwrap_or_default();
    let lines: Vec<&str> = source.lines().collect();
    let mut unpaid = Vec::new();
    for (number, line) in lines.iter().enumerate() {
        let code = line.split("//").next().unwrap_or_default();
        let boxes = [
            "Box::new(",
            "Box::pin(",
            "(Box::new)",
            "acyclic_fs::in_heap(",
        ]
        .iter()
        .any(|raw| {
            code.match_indices(raw).any(|(at, _)| {
                !code
                    .get(..at)
                    .is_some_and(|before| before.ends_with("Charged") || before.ends_with("::"))
            })
        });
        let justified = lines
            .get(number.saturating_sub(3)..number)
            .unwrap_or_default()
            .iter()
            .any(|line| line.contains("// Unbudgeted:") || line.contains("// Charged above:"));
        if boxes && !justified {
            unpaid.push(number + 1);
        }
    }
    unpaid
}

/// Fails with every unpaid line of each named source.
///
/// # Panics
///
/// When any source makes an unpaid box.
pub fn assert_all_paid(sources: &[(&str, &str)]) {
    let unpaid: Vec<(&str, Vec<usize>)> = sources
        .iter()
        .map(|(name, source)| (*name, unpaid(source)))
        .filter(|(_, lines)| !lines.is_empty())
        .collect();
    assert!(
        unpaid.is_empty(),
        "boxes with no charge or reason: {unpaid:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::unpaid;

    #[test]
    fn rejects_each_raw_form_and_accepts_each_reason() {
        for raw in [
            "    let value = Box::new(value);",
            "    future: Box::pin(build()),",
            "    .map(Box::new)",
            "    acyclic_fs::in_heap(|| run()).await",
        ] {
            assert_eq!(unpaid(raw), vec![1], "{raw}");
        }
        assert!(unpaid("    ChargedBox::new(Some(io), value)?").is_empty());
        assert!(unpaid("    // Unbudgeted: nothing is live.\n    Box::new(value)").is_empty());
        assert!(unpaid("    // Charged above: frame.\n\n    Box::pin(f)").is_empty());
        assert!(unpaid("fn a() {}\n#[cfg(test)]\nfn b() { Box::new(1); }").is_empty());
    }
}
