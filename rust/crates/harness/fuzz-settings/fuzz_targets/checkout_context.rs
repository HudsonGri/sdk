#![no_main]

// Include the private production parsers without enlarging the Harness API or
// compiling the entire native runtime under the fuzzer's instrumented toolchain.
#[derive(Debug)]
pub enum Error {
    Invalid(String),
}
pub type Result<T> = std::result::Result<T, Error>;
#[path = "../../src/local/context/skills/catalog.rs"]
mod catalog;
#[path = "../../src/context/discovery/frontmatter.rs"]
mod context;
#[path = "../../src/local/context/environment/porcelain.rs"]
mod porcelain;
#[path = "../../src/local/context/text.rs"]
mod text;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    // The production read boundary is 64 KiB.
    if input.len() > 65_536 {
        return;
    }
    if let Some(skill) = catalog::listed(input, "SKILL.md".into()) {
        let canonical = format!(
            "---\n{}---\n",
            serde_saphyr::to_string(
                &serde_json::json!({"name":skill.name,"description":skill.description})
            )
            .unwrap()
        );
        assert_eq!(
            catalog::listed(canonical.as_bytes(), "SKILL.md".into()),
            Some(skill)
        );
        let skill = catalog::listed(input, String::from_utf8_lossy(input).into_owned()).unwrap();
        let rendered = catalog::frame(&[skill]).unwrap();
        assert_eq!(rendered.matches('<').count(), 2);
        assert_eq!(rendered.lines().count(), 6);
        assert!(!rendered.contains(['\u{B}', '\u{C}', '\r', '\u{85}', '\u{2028}', '\u{2029}']));
    }
    assert!(porcelain::summarize(input, true).ends_with("more changes not counted"));
    let _ = porcelain::summarize(input, false);
});
