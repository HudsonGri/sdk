use super::*;
mod wildmatch;

/// Git-compatible ignore policy used by commit and parent-join capture.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitIgnorePolicy {
    rules: Vec<GitIgnoreRule>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GitIgnoreRule {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
    #[serde(skip)]
    compiled: std::sync::OnceLock<Vec<wildmatch::Token>>,
}

impl PartialEq for GitIgnoreRule {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
            && self.negated == other.negated
            && self.directory_only == other.directory_only
            && self.anchored == other.anchored
    }
}
impl Eq for GitIgnoreRule {}

impl GitIgnorePolicy {
    /// Identifies changed, newly ignored paths whose child binding wins.
    pub async fn newly_ignored_paths_at<A, O>(
        &self,
        generation: &Generation<A, O>,
        changes: &[crate::ChangedPath],
        already_tracked: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, GitCaptureError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut paths = BTreeSet::new();
        let mut nested_policies = BTreeMap::new();
        for change in changes {
            let path = portable_changed_path(&change.path)?;
            let is_directory = change
                .after
                .or(change.before)
                .is_some_and(|record| record.kind == FileKind::Directory);
            // Directory bindings can contain tracked descendants.
            if is_directory {
                continue;
            }
            if !git_path_eligible_with_root_at(
                generation,
                &mut nested_policies,
                self,
                &path,
                is_directory,
                already_tracked,
            )
            .await?
            {
                paths.insert(format!("/{path}"));
            }
        }
        Ok(paths)
    }

    /// Parses Gitignore-shaped UTF-8 lines.
    #[must_use]
    pub fn parse(contents: &str) -> Self {
        let rules = contents
            .lines()
            .filter_map(|line| {
                let mut line = line.trim_end_matches('\r');
                // Git discards unescaped trailing spaces. A space preceded by
                // an odd run of backslashes belongs to the pattern.
                while line.ends_with(' ') {
                    let escapes = line.as_bytes()[..line.len() - 1]
                        .iter()
                        .rev()
                        .take_while(|byte| **byte == b'\\')
                        .count();
                    if escapes % 2 == 1 {
                        break;
                    }
                    line = &line[..line.len() - 1];
                }
                if line.is_empty() || line.starts_with('#') {
                    return None;
                }
                let negated = line.starts_with('!');
                if negated {
                    line = line.strip_prefix('!').unwrap_or(line);
                }
                let anchored = line.starts_with('/');
                if anchored {
                    line = line.strip_prefix('/').unwrap_or(line);
                }
                let directory_only = line.ends_with('/');
                if directory_only {
                    line = line.trim_end_matches('/');
                }
                if line.is_empty() {
                    return None;
                }
                Some(GitIgnoreRule {
                    pattern: line.to_owned(),
                    negated,
                    directory_only,
                    anchored,
                    compiled: std::sync::OnceLock::new(),
                })
            })
            .collect();
        Self { rules }
    }

    /// Adds a later policy at the same directory, preserving rule precedence.
    pub fn extend(&mut self, policy: Self) {
        self.rules.extend(policy.rules);
    }

    /// Returns whether a path participates in compatibility commit/join state.
    #[must_use]
    pub fn eligible(&self, path: &str, is_directory: bool, already_tracked: bool) -> bool {
        if already_tracked {
            return true;
        }
        let path = path.trim_start_matches('/');
        // A child cannot be re-included while one of its parents is ignored.
        // This matters for explicit subtree search roots as well as walking.
        for end in path
            .match_indices('/')
            .map(|(end, _)| end)
            .chain([path.len()])
        {
            let mut ignored = false;
            self.apply(
                &path[..end],
                end != path.len() || is_directory,
                &mut ignored,
            );
            if ignored {
                return false;
            }
        }
        true
    }

    /// Applies directory-local policies in ancestor order. Each policy's
    /// patterns are relative to the directory containing its ignore file.
    #[must_use]
    pub fn eligible_with_nested(
        &self,
        nested: &BTreeMap<String, Self>,
        path: &str,
        is_directory: bool,
    ) -> bool {
        git_path_eligible(self, nested, path, is_directory, false)
    }

    fn apply(&self, path: &str, is_directory: bool, ignored: &mut bool) {
        for rule in &self.rules {
            if ignore_matches(rule, path, is_directory) {
                *ignored = !rule.negated;
            }
        }
    }
}

pub(super) fn git_path_eligible(
    root: &GitIgnorePolicy,
    nested: &BTreeMap<String, GitIgnorePolicy>,
    path: &str,
    is_directory: bool,
    already_tracked: bool,
) -> bool {
    if already_tracked {
        return true;
    }
    let path = path.trim_start_matches('/');
    for end in path
        .match_indices('/')
        .map(|(end, _)| end)
        .chain([path.len()])
    {
        let candidate = &path[..end];
        let directory = end != path.len() || is_directory;
        let mut ignored = false;
        root.apply(candidate, directory, &mut ignored);
        // Look up just this path's ancestors. Scanning every previously read
        // ignore directory for every file makes a large walk quadratic.
        for (split, _) in candidate.match_indices('/') {
            if let Some(policy) = nested.get(&candidate[..split]) {
                policy.apply(&candidate[split + 1..], directory, &mut ignored);
            }
        }
        if ignored {
            return false;
        }
    }
    true
}

fn ignore_matches(rule: &GitIgnoreRule, path: &str, is_directory: bool) -> bool {
    let compiled = rule
        .compiled
        .get_or_init(|| wildmatch::tokens(rule.pattern.as_bytes()));
    let matches = |candidate: &str| {
        if rule.anchored || rule.pattern.contains('/') {
            wildmatch::matches(compiled, candidate)
        } else {
            candidate
                .split('/')
                .any(|component| wildmatch::matches(compiled, component))
        }
    };
    {
        let mut candidate = if !rule.directory_only || is_directory {
            Some(path)
        } else {
            path.rsplit_once('/').map(|(parent, _)| parent)
        };
        while let Some(parent) = candidate {
            if matches(parent) {
                return true;
            }
            candidate = parent.rsplit_once('/').map(|(parent, _)| parent);
        }
        false
    }
}
