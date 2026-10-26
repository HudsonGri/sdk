use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    #[default]
    Text,
    Jsonl,
}

#[derive(Debug, Parser)]
#[command(version, about = "Coding agent over the harness client")]
pub struct Args {
    #[arg(long, default_value = ".")]
    pub workspace: PathBuf,
    /// Instruction scope relative to the workspace root.
    #[arg(long)]
    pub directory: Option<String>,
    #[arg(long, env = "GRAPHCODER_MODEL")]
    pub model: Option<String>,
    #[arg(long)]
    pub catalog: Option<PathBuf>,
    #[arg(long)]
    pub credentials: Option<PathBuf>,
    #[arg(long)]
    pub settings_root: Option<PathBuf>,
    #[arg(short = 'p', long = "prompt", conflicts_with = "input_format")]
    pub prompt: Option<String>,
    #[arg(long, value_enum, default_value_t)]
    pub input_format: InputFormat,
    /// Stable identity of a prompt submission. JSONL lines carry their own ids.
    #[arg(long, conflicts_with = "input_format")]
    pub operation: Option<String>,
    /// Reopen the last interactive conversation, including after a normal quit.
    #[arg(long, conflicts_with_all = ["prompt", "input_format"])]
    pub resume: bool,
    #[arg(long)]
    pub inline: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headless_modes_use_explicit_jsonl_identity_and_reject_mixed_formats() {
        assert!(
            Args::try_parse_from(["graphcoder", "--resume"])
                .unwrap()
                .resume
        );
        assert!(Args::try_parse_from(["graphcoder", "--resume", "-p", "hello"]).is_err());
        assert!(Args::try_parse_from(["graphcoder", "-p", "hello"]).is_ok());
        assert!(Args::try_parse_from(["graphcoder", "--search-config", "search.json"]).is_err());
        // Every actor JSONL line supplies its own stable IntentId.
        assert!(Args::try_parse_from(["graphcoder", "--input-format", "jsonl"]).is_ok());
        assert!(
            Args::try_parse_from([
                "graphcoder",
                "--input-format",
                "jsonl",
                "--operation",
                "00000000-0000-0000-0000-000000000001"
            ])
            .is_err()
        );
        assert!(
            Args::try_parse_from([
                "graphcoder",
                "-p",
                "hello",
                "--input-format",
                "jsonl",
                "--operation",
                "operation"
            ])
            .is_err()
        );
    }

    #[test]
    fn removed_host_and_fixture_entrypoints_cannot_be_selected() {
        for option in ["--session", "--prepare-scenario", "--fixture", "--headless"] {
            assert!(
                Args::try_parse_from(["graphcoder", option, "unused"]).is_err(),
                "{option}"
            );
        }
    }
}
