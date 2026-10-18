//! Each checkout turn carries Codex-framed environment facts and the discovered skills.
use super::*;
use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelRole};

/// Noon UTC on 2026-10-18.
const NOW: u64 = 1_792_324_800_000;

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        NOW
    }
}

fn git(directory: &Path, arguments: &[&str]) -> Result<()> {
    let status = std::process::Command::new("git")
        .current_dir(directory)
        // Isolate the fixture from the developer's and the system's git config.
        .env("GIT_CONFIG_GLOBAL", directory.join(".missing-gitconfig"))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args([
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
        ])
        .args(["-c", "commit.gpgsign=false"])
        .args(arguments)
        .status()
        .map_err(super::super::storage)?;
    assert!(status.success(), "git {arguments:?} failed");
    Ok(())
}

fn write(root: &Path, path: &str, text: &str) -> Result<()> {
    let path = root.join(path);
    std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .map_err(super::super::storage)?;
    std::fs::write(path, text).map_err(super::super::storage)
}

/// System text in request order, and the position of the first project instruction.
fn system_text(request: &crate::model::ModelRequest) -> (Vec<String>, Option<usize>) {
    let mut texts = Vec::new();
    let mut instructions = None;
    for (index, message) in request.messages.iter().enumerate() {
        match &message.content {
            ModelContent::Text(text) if message.role == ModelRole::System => {
                texts.push(text.clone());
            }
            ModelContent::Part(ModelContentPart::File {
                policy: FileProjectionPolicy::ProjectInstructions(_),
                ..
            }) => {
                instructions.get_or_insert(index);
            }
            _ => {}
        }
    }
    (texts, instructions)
}

#[tokio::test]
async fn checkout_turns_carry_environment_facts_and_discovered_skills() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    write(root, "AGENTS.md", "Project rules")?;
    write(
        root,
        ".agents/skills/release-notes/SKILL.md",
        "---\nname: release-notes\ndescription: Draft release notes.\n---\nSECRET BODY",
    )?;
    // Negative controls: a later root cannot shadow a name, and invalid skills stay unlisted.
    write(
        root,
        ".claude/skills/release-notes/SKILL.md",
        "---\nname: release-notes\ndescription: Shadowed.\n---\n",
    )?;
    write(root, ".claude/skills/broken/SKILL.md", "no frontmatter")?;
    write(
        root,
        ".claude/skills/audit/SKILL.md",
        "---\nname: audit\ndescription: Audit dependencies.\n---\n",
    )?;
    git(root, &["init", "--quiet", "--initial-branch=main"])?;
    git(root, &["add", "--all"])?;
    git(root, &["commit", "--quiet", "--message", "fixture"])?;
    let provider = scripted([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("second".into()),
    ]);
    let config = crate::test_support::local_harness_config(root, (model()?, provider.clone()))?
        .state_directory(state.path())
        .clock(Arc::new(Clock));
    let harness = LocalHarness::open(config).await?;
    assert_eq!(harness.run("hello").await?.text, "first");
    write(root, "notes.txt", "untracked")?;
    assert_eq!(harness.run("again").await?.text, "second");

    let workspace = harness.workspace.as_ref().expect("checkout workspace");
    let cwd = crate::local::context::environment::display(&workspace.path);
    let skills = "<skills_instructions>\n## Skills\nA skill is a set of local instructions to follow that is stored in a `SKILL.md` file. Below is the list of skills that can be used. Each entry includes a name, description, and a path relative to the working directory. Read a skill's file before following it.\n### Available skills\n- release-notes: Draft release notes. (file: .agents/skills/release-notes/SKILL.md)\n- audit: Audit dependencies. (file: .claude/skills/audit/SKILL.md)\n</skills_instructions>";
    let conventions: Vec<String> =
        serde_json::from_str(include_str!("../../context/instructions.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let requests = provider.requests();
    for (request, git_status) in [(0, "branch main; clean"), (1, "branch main; 1 untracked")] {
        let environment = format!(
            "<environment_context>\n  <cwd>{cwd}</cwd>\n  <shell>bash</shell>\n  <os>{}</os>\n  <current_date>2026-10-18</current_date>\n  <timezone>UTC</timezone>\n  <git_status>{git_status}</git_status>\n</environment_context>",
            std::env::consts::OS
        );
        let mut expected = conventions.clone();
        expected.extend([
            workspace.context_line().to_owned(),
            environment,
            skills.to_owned(),
        ]);
        let (texts, instructions) = system_text(&requests[request]);
        // The exact system prefix precedes the quoted AGENTS.md instructions.
        assert_eq!(texts[..expected.len()], expected[..], "request {request}");
        assert!(instructions.is_some_and(|index| index >= expected.len()));
        assert!(!texts.iter().any(|text| text.contains("SECRET BODY")));
    }
    Ok(())
}

#[tokio::test]
async fn skill_discovery_exhausts_pages_and_refreshes_after_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    // Empty skill directories must not consume an arbitrary discovery limit.
    for index in 0..129 {
        std::fs::create_dir_all(root.join(format!(".agents/skills/a{index:03}")))
            .map_err(super::super::storage)?;
    }
    write(
        root,
        ".agents/skills/z-last/SKILL.md",
        "---\nname: last\ndescription: original\n---\nBODY",
    )?;
    let provider = scripted([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("second".into()),
    ]);
    let config = || {
        crate::test_support::local_harness_config(root, (model()?, provider.clone()))
            .map(|config| config.state_directory(state.path()).clock(Arc::new(Clock)))
    };
    let harness = LocalHarness::open(config()?).await?;
    let operation = OperationId::new();
    let task = super::context::admitted(harness.submit(operation, "first").await?)?;
    let (reference, before) = super::context::manifest(&harness, operation).await?;
    assert!(
        before
            .conventions
            .iter()
            .any(|text| text.contains("- last: original"))
    );
    write(
        root,
        ".agents/skills/z-last/SKILL.md",
        "---\nname: last\ndescription: replacement\n---\nBODY",
    )?;
    harness.disconnect_unfinished().await?;
    let reopened = LocalHarness::open(config()?).await?;
    let (retained, captured) = super::context::manifest(&reopened, operation).await?;
    assert_eq!(retained, reference);
    assert_eq!(captured, before);
    assert_eq!(reopened.resume(task).await?.text, "first");
    assert_eq!(reopened.run("second").await?.text, "second");
    let requests = provider.requests();
    for (index, expected, absent) in [
        (0, "- last: original", "replacement"),
        (1, "- last: replacement", "original"),
    ] {
        let (texts, _) = system_text(&requests[index]);
        let skills = texts
            .iter()
            .find(|text| text.starts_with("<skills_instructions>"))
            .unwrap();
        assert!(skills.contains(expected));
        assert!(!skills.contains(absent));
    }
    reopened.close().await
}
