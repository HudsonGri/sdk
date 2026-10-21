//! The skill catalog a context manifest pins as data.
//!
//! The prompt block is never stored rendered. Each model request derives it
//! against that request's own allowance: a pointer line counting every skill
//! and naming where to list them, the longest prefix in discovery order that
//! fits, then one line counting exactly the skills left out. Every skill stays
//! a file under its root, so an unlisted one is still found by listing. When
//! the allowance cannot hold the framed block, a one-line pointer remains, so
//! a model with a small context still learns that skills exist.
#[cfg(test)]
use super::text::push_value;
use serde::{Deserialize, Serialize};

/// Repository skill roots, in precedence order for duplicate names.
#[cfg(any(test, all(feature = "filesystem-local", not(target_arch = "wasm32"))))]
#[doc(hidden)]
pub const SKILL_ROOTS: [&str; 2] = [".agents/skills", ".claude/skills"];

/// One listed skill: validated name, single-line description and file path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSkill {
    /// Frontmatter name.
    pub name: String,
    /// Frontmatter description, whitespace-normalized to one line.
    pub description: String,
    /// The skill file, relative to the working directory.
    pub path: String,
}

/// Accepted skills in discovery order. `unlisted` counts accepted skills the
/// manifest file itself had no room for; they are omitted from every request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillCatalog {
    /// Skills carried by the manifest, in discovery order.
    pub skills: Vec<CatalogSkill>,
    /// Further accepted skills, counted but not carried.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unlisted: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl SkillCatalog {
    /// Exact count, or no representable count for malformed manifest data.
    pub(crate) fn total(&self) -> Option<u64> {
        (self.skills.len() as u64).checked_add(self.unlisted)
    }

    /// True when discovery accepted no skill.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.skills.is_empty() && self.unlisted == 0
    }

    /// Carries the longest prefix of `skills` whose JSON array (brackets and
    /// separators included) fits `available` bytes, and counts the rest.
    #[cfg(test)]
    pub(crate) fn bounded(skills: Vec<CatalogSkill>, available: u64) -> Self {
        let mut catalog = Self::default();
        let mut used = 2;
        for skill in skills {
            catalog.retain_prefix(skill, available, &mut used);
        }
        catalog
    }

    /// Retains one entry only while the discovery prefix fits the manifest.
    /// Later entries are counted and dropped immediately, including descriptions.
    #[cfg(any(test, all(feature = "filesystem-local", not(target_arch = "wasm32"))))]
    #[doc(hidden)]
    pub fn retain_prefix(&mut self, skill: CatalogSkill, available: u64, used: &mut u64) {
        if self.unlisted == 0 {
            let size = serde_json::to_vec(&skill).map_or(u64::MAX, |bytes| bytes.len() as u64);
            let next = used
                .saturating_add(size)
                .saturating_add(u64::from(!self.skills.is_empty()));
            if next <= available {
                *used = next;
                self.skills.push(skill);
                return;
            }
        }
        self.unlisted = self.unlisted.saturating_add(1);
    }

    /// The catalog block whose JSON string encoding, quotes included, fits
    /// `budget` bytes. The framed block opens with the pointer line and lists
    /// every skill, or the longest prefix in discovery order followed (when
    /// one is listed) by a line counting exactly the rest. Below the framed
    /// block's size only the minimal pointer remains. `None` when there is no
    /// skill, the total count overflows, or not even the minimal pointer fits.
    #[must_use]
    pub fn render(&self, budget: u64) -> Option<String> {
        let mut text = String::new();
        let rendered = render_into(
            self,
            budget,
            &mut text,
            |text, part| {
                text.push_str(part);
                Ok::<(), std::convert::Infallible>(())
            },
            || Ok(()),
            |text, bytes| {
                text.reserve_exact(usize::try_from(bytes).expect("catalog capacity fits usize"));
                Ok(())
            },
        );
        matches!(rendered, Ok(true)).then_some(text)
    }
}

/// Share of a model's usable input (context minus the response reserve) that
/// the catalog may occupy.
pub(crate) const CATALOG_SHARE_DIVISOR: u64 = 20;

/// The catalog's token allowance for one model: 5% of its usable input.
///
/// The block is sized in bytes, without a provider round trip; this sizes the
/// catalog's share and is not what keeps a request within capacity. Admission
/// counts the complete request, catalog included, with the selected
/// provider's counter, whose contract is to report at least the input tokens
/// the provider charges, and compacts or rejects a request that does not fit
/// (`acyclic-harness-providers` `counting::contract_tests`).
#[must_use]
#[doc(hidden)]
pub fn token_allowance(context_tokens: u32, reserve_tokens: u32) -> u64 {
    u64::from(context_tokens.saturating_sub(reserve_tokens)) / CATALOG_SHARE_DIVISOR
}

pub(crate) const HEADER: &str = "<skills_instructions>\n## Skills\nA skill is a set of local instructions to follow that is stored in a `SKILL.md` file. Below is the list of skills that can be used. Each entry includes a name, description, and a path relative to the working directory. Read a skill's file before following it.\n### Available skills\n";
pub(crate) const FOOTER: &str = "</skills_instructions>";
/// Follows the total count on the framed block's first line. Names every root
/// used for repository discovery.
pub(crate) const POINTER: &str =
    " skills available (list .agents/skills/ and .claude/skills/ to see all)\n";
/// Surrounds the total count when the framed block does not fit.
pub(crate) const MINIMAL: [&str; 2] = [".agents/skills/ and .claude/skills/ have ", " skills"];
/// Follows the count of skills a listed prefix leaves out.
#[doc(hidden)]
pub const OMITTED: &str = " more skills omitted\n";

/// Bytes `text` occupies inside a JSON string. Escaping is per character, so
/// a concatenation occupies the sum of its parts.
pub(crate) fn json_bytes(text: &str) -> u64 {
    text.chars()
        .map(|character| match character {
            '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
            '\0'..='\u{1f}' => 6,
            character => character.len_utf8() as u64,
        })
        .sum()
}

#[cfg(test)]
pub(crate) fn entry(skill: &CatalogSkill, line: &mut String) {
    line.push_str("- ");
    push_value(line, &skill.name);
    line.push_str(": ");
    push_value(line, &skill.description);
    line.push_str(" (file: ");
    push_value(line, &skill.path);
    line.push_str(")\n");
}

// Stop at the allowance boundary: an omitted description is never fully
// scanned, escaped, or allocated. Every inspected scalar pays for its work.
fn entry_bytes<E>(
    skill: &CatalogSkill,
    maximum: u64,
    visit: &mut impl FnMut() -> Result<(), E>,
) -> Result<Option<u64>, E> {
    let mut bytes = json_bytes("- :  (file: )\n");
    if bytes > maximum {
        return Ok(None);
    }
    let mut buffer = arrayvec::ArrayString::<12>::new();
    for value in [&skill.name, &skill.description, &skill.path] {
        for character in value.chars() {
            visit()?;
            let encoded = super::text::escaped_character(character, &mut buffer);
            bytes = bytes.saturating_add(json_bytes(encoded));
            if bytes > maximum {
                return Ok(None);
            }
        }
    }
    Ok(Some(bytes))
}

fn push_entry<E>(
    skill: &CatalogSkill,
    text: &mut String,
    push: &mut impl FnMut(&mut String, &str) -> Result<(), E>,
    visit: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    let mut buffer = arrayvec::ArrayString::<12>::new();
    for (before, value) in [
        ("- ", &skill.name),
        (": ", &skill.description),
        (" (file: ", &skill.path),
    ] {
        push(text, before)?;
        let mut start = 0;
        for (index, character) in value.char_indices() {
            visit()?;
            if matches!(character, '&' | '<' | '>' | '\u{2028}' | '\u{2029}')
                || character.is_control()
            {
                if start < index {
                    push(text, &value[start..index])?;
                }
                let encoded = super::text::escaped_character(character, &mut buffer);
                push(text, encoded)?;
                start = index + character.len_utf8();
            }
        }
        if start < value.len() {
            push(text, &value[start..])?;
        }
    }
    push(text, ")\n")
}

fn decimal(value: u64, buffer: &mut [u8; 20]) -> &str {
    let mut start = buffer.len();
    let mut rest = value;
    loop {
        start -= 1;
        buffer[start] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    std::str::from_utf8(&buffer[start..]).expect("ASCII digits")
}

fn digits(value: u64) -> u64 {
    decimal(value, &mut [0; 20]).len() as u64
}

fn omission_bytes(omitted: u64) -> u64 {
    2 + digits(omitted) + json_bytes(OMITTED)
}

/// Appends the block to `text` (expected empty) through `push`, the caller's
/// allocation boundary, so that its JSON string encoding, quotes included,
/// fits `budget` bytes. Returns false, leaving `text` empty, when there is no
/// skill or not even the minimal pointer fits. The raw text never exceeds
/// `budget` bytes while it is built.
pub(crate) fn render_into<E>(
    catalog: &SkillCatalog,
    budget: u64,
    text: &mut String,
    mut push: impl FnMut(&mut String, &str) -> Result<(), E>,
    mut visit: impl FnMut() -> Result<(), E>,
    mut reserve: impl FnMut(&mut String, u64) -> Result<(), E>,
) -> Result<bool, E> {
    let Some(total) = catalog.total() else {
        return Ok(false);
    };
    if total == 0 {
        return Ok(false);
    }
    let mut count = [0; 20];
    let count = decimal(total, &mut count);
    let mut used =
        2 + json_bytes(HEADER) + digits(total) + json_bytes(POINTER) + json_bytes(FOOTER);
    if used > budget {
        if 2 + json_bytes(MINIMAL[0]) + digits(total) + json_bytes(MINIMAL[1]) > budget {
            return Ok(false);
        }
        push(text, MINIMAL[0])?;
        push(text, count)?;
        push(text, MINIMAL[1])?;
        return Ok(true);
    }
    push(text, HEADER)?;
    push(text, count)?;
    push(text, POINTER)?;
    // The pointer alone already counts every skill.
    let mut prefix = (text.len(), 0u64);
    for (index, skill) in catalog.skills.iter().enumerate() {
        let Some(size) = entry_bytes(skill, budget.saturating_sub(used), &mut visit)? else {
            break;
        };
        used = used.saturating_add(size);
        // The measured JSON bytes bound the raw entry. Admit its capacity once
        // before emission so escaped scalars cannot trigger repeated growth.
        reserve(text, size)?;
        push_entry(skill, text, &mut push, &mut visit)?;
        let listed = index as u64 + 1;
        if listed == total {
            push(text, FOOTER)?;
            return Ok(true);
        }
        // Remember the longest prefix with room for an exact omission count.
        // Keep trying: a short remaining tail may fit without an omission line.
        if used.saturating_add(omission_bytes(total - listed)) <= budget {
            prefix = (text.len(), listed);
        }
    }
    let (end, listed) = prefix;
    text.truncate(end);
    if listed > 0 {
        push(text, "- ")?;
        push(text, decimal(total - listed, &mut [0; 20]))?;
        push(text, OMITTED)?;
    }
    push(text, FOOTER)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The line standing in for the `omitted` skills that did not fit.
    fn omission(omitted: u64) -> String {
        format!("- {omitted}{OMITTED}")
    }

    /// The framed block's first line, counting all `total` skills.
    fn pointer(total: u64) -> String {
        format!("{total}{POINTER}")
    }

    /// The whole block when the framed one does not fit.
    fn minimal(total: u64) -> String {
        format!("{}{total}{}", MINIMAL[0], MINIMAL[1])
    }

    fn skill((name, description, path): (String, String, String)) -> CatalogSkill {
        CatalogSkill {
            name,
            description,
            path,
        }
    }

    #[test]
    fn pointers_name_every_root_and_lines_have_their_exact_size() {
        for root in SKILL_ROOTS {
            assert!(POINTER.contains(&format!(" {root}/ ")), "{root}");
            assert!(minimal(1).contains(&format!("{root}/ ")), "{root}");
        }
        for omitted in [0, 1, 9, 10, 99, 100, u64::MAX] {
            let line = omission(omitted);
            assert_eq!(omission_bytes(omitted), json_bytes(&line), "{line}");
            assert_eq!(decimal(omitted, &mut [0; 20]), omitted.to_string());
        }
    }

    #[test]
    fn discovery_drops_each_description_before_accepting_the_next() {
        let mut catalog = SkillCatalog::default();
        let mut used = 2;
        for index in 0..128 {
            catalog.retain_prefix(
                CatalogSkill {
                    name: format!("skill-{index}"),
                    description: "x".repeat(65_536),
                    path: "SKILL.md".into(),
                },
                256,
                &mut used,
            );
            assert!(
                catalog.skills.is_empty(),
                "discarded description retained during discovery"
            );
            assert_eq!(catalog.unlisted, index + 1);
            assert_eq!(used, 2);
        }
    }

    #[test]
    fn omitted_entry_work_is_bounded_and_charged() {
        let catalog = SkillCatalog {
            skills: vec![CatalogSkill {
                name: "fixture".into(),
                description: "&".repeat(1_048_576),
                path: "SKILL.md".into(),
            }],
            unlisted: 0,
        };
        let budget = 2 + json_bytes(HEADER) + 1 + json_bytes(POINTER) + json_bytes(FOOTER) + 32;
        let mut visits = 0;
        let mut text = String::new();
        assert_eq!(
            render_into(
                &catalog,
                budget,
                &mut text,
                |text, part| {
                    text.push_str(part);
                    Ok::<_, ()>(())
                },
                || {
                    visits += 1;
                    Ok(())
                },
                |text, bytes| {
                    text.reserve_exact(bytes as usize);
                    Ok(())
                },
            ),
            Ok(true)
        );
        assert!(
            visits > 0 && visits <= 32,
            "unbounded or unpaid scan: {visits}"
        );
        assert!(!text.contains(" (file: "));
        assert!(json_bytes(&text) + 2 <= budget);
        let mut refused = String::new();
        assert_eq!(
            render_into(
                &catalog,
                budget,
                &mut refused,
                |text, part| {
                    text.push_str(part);
                    Ok::<_, ()>(())
                },
                || Err(()),
                |text, bytes| {
                    text.reserve_exact(bytes as usize);
                    Ok(())
                },
            ),
            Err(())
        );
        assert!(!refused.contains("- fixture"));
    }

    #[test]
    fn accepted_entry_emission_can_be_refused_before_copying_input() {
        let skill = CatalogSkill {
            name: "fixture".into(),
            description: "<".into(),
            path: "SKILL.md".into(),
        };
        let measured = [&skill.name, &skill.description, &skill.path]
            .iter()
            .map(|value| value.chars().count())
            .sum::<usize>();
        let catalog = SkillCatalog {
            skills: vec![skill],
            unlisted: 0,
        };
        let mut visits = 0;
        let mut text = String::new();
        assert_eq!(
            render_into(
                &catalog,
                u64::MAX,
                &mut text,
                |text, part| {
                    text.push_str(part);
                    Ok::<_, ()>(())
                },
                || {
                    visits += 1;
                    if visits > measured { Err(()) } else { Ok(()) }
                },
                |text, bytes| {
                    text.reserve_exact(bytes as usize);
                    Ok(())
                },
            ),
            Err(())
        );
        assert!(!text.contains("- fixture"));
    }

    #[test]
    fn accepted_entry_escaping_uses_the_allocation_boundary() {
        let catalog = SkillCatalog {
            skills: vec![CatalogSkill {
                name: "fixture".into(),
                description: "<".into(),
                path: "SKILL.md".into(),
            }],
            unlisted: 0,
        };
        let mut text = String::new();
        let result = render_into(
            &catalog,
            u64::MAX,
            &mut text,
            |text, part| {
                if part == "&lt;" {
                    return Err(());
                }
                text.push_str(part);
                Ok(())
            },
            || Ok(()),
            |text, bytes| {
                text.reserve_exact(bytes as usize);
                Ok(())
            },
        );
        assert_eq!(result, Err(()));
        assert!(!text.contains("&lt;"));
    }

    #[test]
    fn refused_entry_capacity_builds_no_entry() {
        let catalog = SkillCatalog {
            skills: vec![CatalogSkill {
                name: "fixture".into(),
                description: "&".into(),
                path: "SKILL.md".into(),
            }],
            unlisted: 0,
        };
        let mut text = String::new();
        assert_eq!(
            render_into(
                &catalog,
                u64::MAX,
                &mut text,
                |text, part| {
                    text.push_str(part);
                    Ok::<_, ()>(())
                },
                || Ok(()),
                |_, _| Err(())
            ),
            Err(())
        );
        assert!(!text.contains("- fixture"));
    }

    #[test]
    fn escape_heavy_entries_reserve_capacity_before_scalar_emission() {
        use std::cell::Cell;
        let catalog = SkillCatalog {
            skills: vec![CatalogSkill {
                name: "fixture".into(),
                description: "&<".repeat(4096),
                path: "SKILL.md".into(),
            }],
            unlisted: 0,
        };
        let allocations = Cell::new(0);
        let mut text = String::new();
        let grow = |text: &mut String, bytes: u64| {
            if text.len() + bytes as usize > text.capacity() {
                allocations.set(allocations.get() + 1);
                text.reserve_exact(bytes as usize);
            }
            Ok::<_, ()>(())
        };
        assert_eq!(
            render_into(
                &catalog,
                u64::MAX,
                &mut text,
                |text, part| {
                    grow(text, part.len() as u64)?;
                    text.push_str(part);
                    Ok(())
                },
                || Ok(()),
                grow
            ),
            Ok(true)
        );
        assert!(text.contains(&"&amp;&lt;".repeat(4096)));
        assert!(
            allocations.get() <= 5,
            "scalar-by-scalar allocation: {}",
            allocations.get()
        );
    }

    #[test]
    fn allowance_is_five_percent_of_usable_input() {
        assert_eq!(token_allowance(32_768, 4_096), 1_433);
        assert_eq!(token_allowance(200_000, 16_384), 9_180);
        assert_eq!(token_allowance(1, 2), 0);
    }

    /// The old renderer, which dropped the block whenever no entry fit.
    fn dropping(catalog: &SkillCatalog, budget: u64) -> Option<String> {
        catalog
            .render(budget)
            .filter(|text| text.contains(" (file: "))
    }

    /// Checks one render against the contract, derived independently of the
    /// renderer: the whole catalog, or the longest listed prefix with an exact
    /// count of the rest, under a pointer counting every skill; below the
    /// framed block's size, the minimal pointer; nothing only when there is no
    /// skill or not even the minimal pointer fits.
    fn check(
        render: impl Fn(&SkillCatalog, u64) -> Option<String>,
        catalog: &SkillCatalog,
        budget: u64,
    ) -> std::result::Result<(), TestCaseError> {
        let total = catalog.skills.len() as u128 + u128::from(catalog.unlisted);
        if total > u128::from(u64::MAX) {
            prop_assert_eq!(render(catalog, budget), None);
            return Ok(());
        }
        let total = total as u64;
        let encoded = |text: &str| serde_json::to_string(text).unwrap().len() as u64;
        let entries = catalog
            .skills
            .iter()
            .map(|skill| {
                let mut text = String::new();
                entry(skill, &mut text);
                text
            })
            .collect::<Vec<_>>();
        let framed = |kept: usize| {
            let omitted = total - kept as u64;
            let count = if kept > 0 && omitted > 0 {
                omission(omitted)
            } else {
                String::new()
            };
            format!(
                "{HEADER}{}{}{count}{FOOTER}",
                pointer(total),
                entries[..kept].concat()
            )
        };
        // Preference order: more listed skills first, then the bare pointer.
        let expected = if total == 0 {
            None
        } else {
            (0..=entries.len())
                .rev()
                .map(framed)
                .chain(std::iter::once(minimal(total)))
                .find(|text| encoded(text) <= budget)
        };
        let actual = render(catalog, budget);
        prop_assert_eq!(&actual, &expected);
        if let Some(text) = &actual {
            prop_assert_eq!(encoded(text), json_bytes(text) + 2);
            prop_assert!(encoded(text) <= budget, "{} > {}", encoded(text), budget);
            prop_assert!(text.contains(&format!("{total} skills")), "{}", text);
        }
        Ok(())
    }

    #[test]
    fn a_catalog_too_large_for_one_entry_still_points_at_its_skills() {
        let catalog = SkillCatalog {
            skills: vec![CatalogSkill {
                name: "fixture".into(),
                description: "d".repeat(400),
                path: ".agents/skills/fixture/SKILL.md".into(),
            }],
            unlisted: 2,
        };
        let framed = format!("{HEADER}{}{FOOTER}", pointer(3));
        let budget = serde_json::to_string(&framed).unwrap().len() as u64;
        assert_eq!(catalog.render(budget), Some(framed));
        assert_eq!(catalog.render(budget - 1), Some(minimal(3)));
        let small = serde_json::to_string(&minimal(3)).unwrap().len() as u64;
        assert_eq!(catalog.render(small), Some(minimal(3)));
        assert_eq!(catalog.render(small - 1), None);
        let render = |catalog: &SkillCatalog, budget| catalog.render(budget);
        for budget in [budget, small, small - 1, u64::MAX] {
            assert!(check(render, &catalog, budget).is_ok(), "{budget}");
        }
        // Negative control: a renderer that drops the block when no entry
        // fits fails the property's own check.
        assert!(check(dropping, &catalog, budget).is_err());
        assert!(check(dropping, &catalog, small).is_err());
    }

    proptest! {
        // Property: for any catalog and budget, including budgets below one
        // entry and below the framed block, the render is the contract's first
        // candidate that fits, and its JSON string fits the budget.
        #[test]
        fn rendered_catalog_fits_any_budget(
            skills in prop::collection::vec(
                ("[a-z][a-z0-9-]{0,12}", "[ -~\t\n\"\\\\\u{1}-\u{1f}é→]{1,80}", "[ -~é]{1,40}"),
                0..40,
            ),
            unlisted in prop_oneof![Just(0u64), any::<u64>(), (u64::MAX - 40)..=u64::MAX],
            budget in prop_oneof![0u64..64, 300u64..700, 0u64..8192],
        ) {
            let catalog = SkillCatalog { skills: skills.into_iter().map(skill).collect(), unlisted };
            let render = |catalog: &SkillCatalog, budget| catalog.render(budget);
            check(render, &catalog, u64::MAX)?;
            check(render, &catalog, budget)?;
        }

        // Public and decoded catalogs need the same framing guarantee as
        // checkout-discovered entries: every field is data, including names.
        #[test]
        fn every_catalog_field_is_framed(
            fields in prop::collection::vec(
                prop::collection::vec(any::<char>(), 0..32).prop_map(|chars| chars.into_iter().collect::<String>()),
                3..=3,
            ),
        ) {
            let catalog = SkillCatalog {
                skills: vec![CatalogSkill { name: fields[0].clone(), description: fields[1].clone(), path: fields[2].clone() }],
                unlisted: 0,
            };
            let rendered = catalog.render(u64::MAX).unwrap();
            prop_assert_eq!(rendered.matches('<').count(), 2);
            prop_assert_eq!(rendered.lines().count(), 7);
            prop_assert!(!rendered.contains(['\u{B}', '\u{C}', '\r', '\u{85}', '\u{2028}', '\u{2029}']), "catalog contains a line break");
        }

        // Property: the carried prefix fits the stated room as the manifest
        // encodes it, and every accepted skill is either carried or counted.
        #[test]
        fn manifest_carries_a_counted_prefix(
            skills in prop::collection::vec(("[a-z]{1,8}", "[ -~é]{1,60}", "[ -~]{1,30}"), 0..24),
            available in 0u64..4096,
        ) {
            let skills = skills.into_iter().map(skill).collect::<Vec<_>>();
            let catalog = SkillCatalog::bounded(skills.clone(), available);
            prop_assert_eq!(catalog.skills.len() as u64 + catalog.unlisted, skills.len() as u64);
            prop_assert_eq!(&catalog.skills[..], &skills[..catalog.skills.len()]);
            let bytes = |skills: &[CatalogSkill]| serde_json::to_vec(skills).unwrap().len() as u64;
            prop_assert!(catalog.skills.is_empty() || bytes(&catalog.skills) <= available);
            if catalog.unlisted > 0 {
                prop_assert!(bytes(&skills[..=catalog.skills.len()]) > available);
            }
        }
    }
}
