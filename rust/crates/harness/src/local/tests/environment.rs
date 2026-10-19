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
async fn discovery_retains_only_the_manifest_prefix_and_counts_later_names() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    for (directory_name, name, description) in [
        (".agents/skills/a", "first", "retained".to_owned()),
        (".agents/skills/b", "large", "x".repeat(32_000)),
        (".agents/skills/c", "later", "small".to_owned()),
        (".claude/skills/duplicate", "large", "duplicate".to_owned()),
    ] {
        write(
            directory.path(),
            &format!("{directory_name}/SKILL.md"),
            &format!("---\nname: {name}\ndescription: {description}\n---\n"),
        )?;
    }
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, scripted([])))?
            .state_directory(state.path()),
    )
    .await?;
    let workspace = harness.workspace.as_ref().unwrap();
    let first = crate::context::CatalogSkill {
        name: "first".into(),
        description: "retained".into(),
        path: ".agents/skills/a/SKILL.md".into(),
    };
    let available = serde_json::to_vec(&vec![first.clone()]).unwrap().len() as u64;
    let catalog = harness
        .checkout_skills(workspace, 65_536, available)
        .await?;
    assert_eq!(
        catalog.skills,
        vec![first],
        "discovery retained a description outside the manifest prefix"
    );
    assert_eq!(
        catalog.unlisted, 2,
        "later names must be counted, with root precedence preserved"
    );
    let empty = harness.checkout_skills(workspace, 65_536, 2).await?;
    assert!(empty.skills.is_empty());
    assert_eq!(empty.unlisted, 3);
    harness.close().await
}

#[tokio::test]
async fn skill_body_size_never_changes_discovery() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let path = ".agents/skills/large/SKILL.md";
    let header = "---\nname: large\ndescription: metadata only\n---\n";
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, scripted([])))?
            .state_directory(state.path()),
    )
    .await?;
    let workspace = harness.workspace.as_ref().unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    // Exercise both sides of the bound and a large body. The old whole-file
    // read is the negative control on the same native view and file.
    for maximum in [header.len() as u64, 65_536] {
        for body in [0, 1, 65_536, 131_072] {
            write(
                directory.path(),
                path,
                &format!("{header}{}", "x".repeat(body)),
            )?;
            let skills = harness
                .checkout_skills(workspace, maximum, u64::MAX)
                .await?;
            assert_eq!(
                skills.skills,
                vec![crate::context::CatalogSkill {
                    name: "large".into(),
                    description: "metadata only".into(),
                    path: path.into(),
                }]
            );
            if header.len() as u64 + body as u64 > maximum {
                assert!(matches!(
                    workspace
                        .view()
                        .read_latest(&format!("/{path}"), maximum, &mut accounting,)
                        .await,
                    Err(acyclic_fs::LazyWorkspaceError::TooLarge)
                ));
            }
        }
    }
    harness.close().await
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
    let skills = "<skills_instructions>\n## Skills\nA skill is a set of local instructions to follow that is stored in a `SKILL.md` file. Below is the list of skills that can be used. Each entry includes a name, description, and a path relative to the working directory. Read a skill's file before following it.\n### Available skills\n2 skills available (list .agents/skills/ and .claude/skills/ to see all)\n- release-notes: Draft release notes. (file: .agents/skills/release-notes/SKILL.md)\n- audit: Audit dependencies. (file: .claude/skills/audit/SKILL.md)\n</skills_instructions>";
    let requests = provider.requests();
    for (request, git_status) in [(0, "branch main; clean"), (1, "branch main; 1 untracked")] {
        let environment = format!(
            "<environment_context>\n  <cwd>{cwd}</cwd>\n  <shell>bash</shell>\n  <os>{}</os>\n  <current_date>2026-10-18</current_date>\n  <timezone>UTC</timezone>\n  <git_status>{git_status}</git_status>\n</environment_context>",
            std::env::consts::OS
        );
        // Neither prompt names GOAL.md or plans, so no goal guidance leads.
        let expected = [
            workspace.context_line().to_owned(),
            environment,
            skills.to_owned(),
        ];
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
            .skill_catalog
            .skills
            .iter()
            .any(|skill| skill.name == "last" && skill.description == "original")
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

// The shipping configuration (automatic compaction) starts a turn in a
// checkout with 300 skills whose catalog exceeds the model's whole capacity. The manifest pins every skill as data; the request
// lists the longest prefix within 5% of the model's usable input and one line
// counts exactly the rest. Negative control: the byte-only budget renders the
// whole catalog into the manifest, and with compactable history behind it the
// next turn still cannot start.
#[tokio::test]
async fn oversized_skill_catalog_fits_the_model_allowance() -> Result<()> {
    const SKILLS: usize = 300;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    let mut skills = Vec::new();
    for index in 0..SKILLS {
        let path = format!(".agents/skills/s{index:03}/SKILL.md");
        let text = format!(
            "---\nname: s{index:03}\ndescription: {}\n---\n",
            "d".repeat(1000)
        );
        write(root, &path, &text)?;
        skills.extend(crate::local::context::skills::listed(text.as_bytes(), path));
    }
    // An invalid entry is skipped without ending the scan.
    write(root, ".agents/skills/r-broken/SKILL.md", "no frontmatter")?;
    let catalog = crate::context::SkillCatalog {
        skills,
        unlisted: 0,
    };
    // A large model: its 5% allowance lists a few entries, while the whole
    // catalog still exceeds its capacity.
    let capacity = ModelContextCapacity {
        context_tokens: 200_000,
        output_tokens: Some(16_384),
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("first".into())])
            .with_context_capacity(capacity),
    );
    let config = || -> Result<LocalHarnessConfig> {
        Ok(
            crate::test_support::local_harness_config(root, (model()?, provider.clone()))?
                .state_directory(state.path())
                .clock(Arc::new(Clock)),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let crate::context::CompactionPolicy::Threshold(policy) = harness.compaction.clone() else {
        panic!("the shipping configuration compacts automatically");
    };
    let allowance = crate::context::skill_catalog::token_allowance(
        capacity.context_tokens,
        policy.response_reserve_tokens,
    );
    let operation = OperationId::new();
    let task = super::context::admitted(harness.submit(operation, "hello").await?)?;
    assert_eq!(harness.resume(task).await?.text, "first");
    let (_, manifest) = super::context::manifest(&harness, operation).await?;
    assert_eq!(manifest.skill_catalog, catalog);
    let request = provider.requests().pop().expect("model request");
    let (system, _) = system_text(&request);
    let block = system
        .iter()
        .find(|text| text.starts_with("<skills_instructions>"))
        .expect("skill catalog");
    let block_bytes = serde_json::to_string(block)
        .map_err(super::super::storage)?
        .len() as u64;
    assert!(block_bytes <= allowance, "{block_bytes} > {allowance}");
    let listed = block
        .lines()
        .filter(|line| line.contains(" (file: "))
        .count();
    assert!(listed > 0 && listed < SKILLS, "{listed}");
    assert_eq!(
        catalog.render(allowance).as_deref(),
        Some(block.as_str()),
        "the longest prefix within the allowance"
    );
    for (line, skill) in block
        .lines()
        .filter(|line| line.contains(" (file: "))
        .zip(&catalog.skills)
    {
        assert!(line.starts_with(&format!("- {}: ", skill.name)));
    }
    assert!(block.contains(&format!(
        "- {}{}",
        SKILLS - listed,
        crate::context::skill_catalog::OMITTED
    )));
    // The admitted request leaves the response reserve free.
    let prepared = crate::model::PreparedModelRequest::prepare(request, harness.limits, None)?;
    let count = provider.count_tokens(&prepared).await?;
    assert!(!policy.needs_compaction(capacity, count.validate(&prepared, &mut None)?)?);

    // Negative control: the byte-only budget sized the block to what the
    // manifest file left of the render bound, and pinned it rendered.
    let mut bytes_only = manifest;
    bytes_only.skill_catalog = crate::context::SkillCatalog::default();
    let limits = harness.limits;
    let used = serde_json::to_vec(&bytes_only)
        .map_err(super::super::storage)?
        .len() as u64;
    bytes_only.conventions.push(
        catalog
            .render(limits.render_bytes - used - 1)
            .expect("byte-only catalog"),
    );
    let reference = bytes_only
        .publish(
            harness.input_publisher()?.as_ref(),
            OperationId::new(),
            "inputs/byte-only.manifest.json",
            limits,
        )
        .await?;
    harness.close().await?;
    let reopened = LocalHarness::open(config()?.context_manifest(reference)).await?;
    let task = super::context::admitted(reopened.submit(OperationId::new(), "hello").await?)?;
    let Err(rejected) = reopened.resume(task).await else {
        panic!("the byte-only catalog was admitted");
    };
    assert_eq!(provider.requests().len(), 1, "nothing was dispatched");
    // Each turn starts its own context, so only the mandatory system
    // content and prompt remain and compaction has nothing to drop.
    assert!(
        rejected
            .to_string()
            .contains("mandatory content leaves no compaction source"),
        "{rejected}"
    );
    assert!(matches!(
        reopened.runtime.task_host().outcome(task).await?,
        Some(crate::Outcome::Failed { .. })
    ));
    reopened.close().await?;
    Ok(())
}

#[tokio::test]
async fn logical_git_switch_never_retains_the_physical_branch_summary() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    write(root, "marker", "base")?;
    git(root, &["init", "--quiet", "--initial-branch=main"])?;
    git(root, &["add", "--all"])?;
    git(root, &["commit", "--quiet", "--message", "fixture"])?;
    let provider = scripted([
        super::checked_commands::call(0, "fs", "git switch -c feature"),
        ScriptedTurn::Text("switched".into()),
        ScriptedTurn::Text("next".into()),
    ]);
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(root, (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    assert_eq!(harness.run("switch").await?.text, "switched");
    let branch = std::process::Command::new("git")
        .current_dir(root)
        .args(["branch", "--show-current"])
        .output()
        .map_err(super::super::storage)?;
    assert!(branch.status.success());
    assert_eq!(
        branch.stdout, b"main\n",
        "fixture needs distinct physical Git state"
    );
    let workspace = harness.workspace.as_ref().unwrap();
    let (selected, _) = workspace.selected_view().await?;
    assert!(selected.is_some(), "logical branch did not switch");
    assert_eq!(harness.run("next").await?.text, "next");
    let requests = provider.requests();
    let (texts, _) = system_text(requests.last().unwrap());
    let environment = texts
        .iter()
        .find(|text| text.starts_with("<environment_context>"))
        .unwrap();
    assert!(
        !environment.contains("<git_status>"),
        "physical Git leaked into the selected view: {environment}"
    );
    harness.close().await
}
