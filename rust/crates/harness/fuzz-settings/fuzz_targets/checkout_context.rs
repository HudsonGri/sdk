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
#[path = "../../../harness-core/src/context/discovery/frontmatter.rs"]
mod frontmatter;
#[path = "../../src/local/context/environment/porcelain.rs"]
mod porcelain;
#[path = "../../../harness-core/src/context/skill_catalog.rs"]
mod skill_catalog;
#[path = "../../../harness-core/src/context/text.rs"]
mod text;
mod context {
    pub(crate) use crate::frontmatter::parse_frontmatter;
    pub(crate) use crate::skill_catalog::CatalogSkill;
}

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    // The production read boundary is 64 KiB.
    if input.len() > 65_536 {
        return;
    }
    // Manifest catalogs bypass checkout frontmatter validation. Exercise their
    // arbitrary fields and counts through the same renderer as projection.
    if let Ok(catalog) = serde_json::from_slice::<skill_catalog::SkillCatalog>(input) {
        for budget in [input.len() as u64, u64::MAX] {
            if let Some(rendered) = catalog.render(budget) {
                assert!(serde_json::to_vec(&rendered).unwrap().len() as u64 <= budget);
                assert!(rendered.matches('<').count() <= 2);
                assert!(!rendered.contains(['\u{B}', '\u{C}', '\r', '\u{85}', '\u{2028}', '\u{2029}']));
            }
        }
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
        let rendered = skill_catalog::SkillCatalog {
            skills: vec![skill],
            unlisted: 0,
        }
        .render(u64::MAX)
        .unwrap();
        assert_eq!(rendered.matches('<').count(), 2);
        assert_eq!(rendered.lines().count(), 7);
        assert!(!rendered.contains(['\u{B}', '\u{C}', '\r', '\u{85}', '\u{2028}', '\u{2029}']));
    }
    assert!(porcelain::summarize(input, true).ends_with("more changes not counted"));
    let _ = porcelain::summarize(input, false);
});
