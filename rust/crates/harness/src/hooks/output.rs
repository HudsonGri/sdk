use super::{HookEvent, HookFailure};
use crate::{Error, Result};
use serde_json::Value;

/// Applied hook decisions and context for the harness loop.
#[derive(Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookOutput {
    /// Reason the guarded action was blocked.
    pub blocked: Option<String>,
    /// A pre-tool hook requested human approval, with its explanation.
    pub approval: Option<String>,
    /// Extra context to include in the next model request.
    pub additional_context: Vec<String>,
    /// Replacement arguments returned by a pre-tool hook.
    pub updated_input: Option<Value>,
    /// Command failures, including explicit failure-policy blocks.
    pub diagnostics: Vec<HookDiagnostic>,
}

/// Platform-neutral failure data retained with the effect, without parsing prose.
#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookDiagnostic {
    /// Failure classification from process capture or output validation.
    pub kind: HookErrorKind,
    /// Exit code observed before capture stopped, if any.
    pub exit_code: Option<i32>,
    /// The configured failure policy blocks this event.
    pub blocking: bool,
    /// Bounded diagnostic for display.
    pub message: String,
}

/// Stable failure classifications shared by retention and the public error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookErrorKind {
    /// Executable was absent.
    NotFound,
    /// The process exceeded its deadline.
    Timeout,
    /// Host permissions prevented execution or capture.
    PermissionDenied,
    /// Other process IO failed.
    Io,
    /// The process exited unsuccessfully.
    Exit,
    /// Structured stdout failed validation.
    InvalidOutput,
    /// Aggregate capture or retained output exhausted its byte budget.
    OutputLimit,
}

impl HookErrorKind {
    fn name(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Timeout => "timeout",
            Self::PermissionDenied => "permission_denied",
            Self::Io => "io",
            Self::Exit => "exit",
            Self::InvalidOutput => "invalid_output",
            Self::OutputLimit => "output_limit",
        }
    }
}

impl HookDiagnostic {
    fn from_error(error: Error, blocking: bool) -> Self {
        let (kind, exit_code, message) = match error {
            Error::Process {
                kind,
                exit_code,
                message,
            } => (
                match kind {
                    "not_found" => HookErrorKind::NotFound,
                    "timeout" => HookErrorKind::Timeout,
                    "permission_denied" => HookErrorKind::PermissionDenied,
                    "exit" => HookErrorKind::Exit,
                    "output_limit" => HookErrorKind::OutputLimit,
                    _ => HookErrorKind::Io,
                },
                exit_code,
                message,
            ),
            Error::Invalid(message) => (HookErrorKind::InvalidOutput, None, message),
            error => (HookErrorKind::Io, None, error.to_string()),
        };
        Self {
            kind,
            exit_code,
            message,
            blocking,
        }
    }
}

impl HookOutput {
    /// Whether an explicit decision or a configured failure policy blocks.
    pub fn blocks(&self) -> bool {
        self.blocked.is_some() || self.diagnostics.iter().any(|failure| failure.blocking)
    }

    /// Propagates a blocking process/output failure with its retained kind/code.
    /// Explicit permission decisions remain distinct from failed guard commands.
    pub fn into_result(mut self) -> Result<Self> {
        if let Some(index) = self.diagnostics.iter().position(|failure| failure.blocking) {
            let failure = self.diagnostics.remove(index);
            return Err(Error::Process {
                kind: failure.kind.name(),
                exit_code: failure.exit_code,
                message: failure.message,
            });
        }
        Ok(self)
    }

    pub(crate) fn output_limit(blocking: bool) -> Self {
        Self {
            diagnostics: vec![HookDiagnostic {
                kind: HookErrorKind::OutputLimit,
                exit_code: None,
                blocking,
                message: "aggregate hook output limit exceeded".into(),
            }],
            ..Default::default()
        }
    }
    pub(super) fn apply(
        &mut self,
        event: HookEvent,
        policy: HookFailure,
        result: Result<(Option<i32>, Vec<u8>, Vec<u8>)>,
    ) -> bool {
        let block_failure = policy == HookFailure::Block && event != HookEvent::Stop;
        let observed_code = result.as_ref().ok().and_then(|(code, _, _)| *code);
        let observed = match result {
            Ok((code, _, stderr)) if code != Some(0) && (code != Some(2) || block_failure) => {
                Err(exit_error(code, &stderr))
            }
            Ok((code, stdout, stderr)) => self.observe(event, code, &stdout, &stderr),
            Err(error) => Err(error),
        };
        match observed {
            Ok(updated) => updated,
            Err(error) => {
                let mut failure = HookDiagnostic::from_error(error, block_failure);
                if failure.exit_code.is_none() {
                    failure.exit_code = observed_code;
                }
                self.diagnostics.push(failure);
                false
            }
        }
    }

    pub(super) fn observe(
        &mut self,
        event: HookEvent,
        code: Option<i32>,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Result<bool> {
        if code == Some(2) {
            if matches!(event, HookEvent::PreToolUse | HookEvent::Stop) {
                self.blocked
                    .get_or_insert_with(|| String::from_utf8_lossy(stderr).into_owned());
            } else if event == HookEvent::PostToolUse {
                self.additional_context
                    .push(String::from_utf8_lossy(stderr).into_owned());
            } else {
                return Err(exit_error(code, stderr));
            }
            return Ok(false);
        }
        let text = std::str::from_utf8(stdout)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .trim();
        if text.is_empty() {
            return Ok(false);
        }
        let mut value = match serde_json::from_str::<Value>(text) {
            Ok(value) => value,
            Err(error) => {
                if text.starts_with('{')
                    || text.starts_with('[') && event != HookEvent::SessionStart
                {
                    return Err(Error::Invalid(format!(
                        "hook output JSON is invalid: {error}"
                    )));
                }
                if event == HookEvent::SessionStart {
                    self.additional_context.push(text.into());
                }
                return Ok(false);
            }
        };
        validate_output(&value, event)?;
        let mut specific = value
            .get_mut("hookSpecificOutput")
            .map(Value::take)
            .unwrap_or(Value::Null);
        if value.get("continue").and_then(Value::as_bool) == Some(false) {
            self.blocked.get_or_insert_with(|| {
                take_string(&mut value, "stopReason").unwrap_or_else(|| "Stopped by hook".into())
            });
        }
        if event == HookEvent::PostToolUse
            && value.get("decision").and_then(Value::as_str) == Some("block")
            && let Some(reason) = take_string(&mut value, "reason")
        {
            self.additional_context.push(reason);
        }
        let asks = event == HookEvent::PreToolUse
            && specific.get("permissionDecision").and_then(Value::as_str) == Some("ask");
        if matches!(event, HookEvent::PreToolUse | HookEvent::Stop)
            && (value.get("decision").and_then(Value::as_str) == Some("block")
                || specific.get("permissionDecision").and_then(Value::as_str) == Some("deny"))
        {
            self.blocked.get_or_insert_with(|| {
                let reason = if asks && self.approval.is_none() {
                    specific
                        .get("permissionDecisionReason")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                } else {
                    take_string(&mut specific, "permissionDecisionReason")
                };
                reason
                    .or_else(|| take_string(&mut value, "reason"))
                    .unwrap_or_else(|| "Blocked by hook".into())
            });
        }
        if asks {
            self.approval.get_or_insert_with(|| {
                take_string(&mut specific, "permissionDecisionReason")
                    .unwrap_or_else(|| "Approval requested by hook".into())
            });
        }
        if let Some(context) = take_string(&mut specific, "additionalContext") {
            self.additional_context.push(context);
        }
        if event == HookEvent::PreToolUse
            && let Some(input) = specific
                .as_object_mut()
                .and_then(|fields| fields.remove("updatedInput"))
        {
            self.updated_input = Some(input);
            return Ok(true);
        }
        Ok(false)
    }
}

fn exit_error(code: Option<i32>, stderr: &[u8]) -> Error {
    Error::Process {
        kind: "exit",
        exit_code: code,
        message: format!("exit {code:?}: {}", String::from_utf8_lossy(stderr)),
    }
}

fn validate_output(value: &Value, event: HookEvent) -> Result<()> {
    if !value.is_object() {
        return Err(Error::Invalid(
            "hook structured output must be an object".into(),
        ));
    }
    validate_strings(value, &["stopReason", "decision", "reason"])?;
    if value
        .get("continue")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(Error::Invalid("hook continue must be a boolean".into()));
    }
    if value
        .get("decision")
        .and_then(Value::as_str)
        .is_some_and(|decision| !matches!(decision, "approve" | "block"))
    {
        return Err(Error::Invalid("unknown hook decision".into()));
    }
    if let Some(specific) = value.get("hookSpecificOutput") {
        if !specific.is_object() {
            return Err(Error::Invalid(
                "hookSpecificOutput must be an object".into(),
            ));
        }
        validate_strings(
            specific,
            &[
                "hookEventName",
                "permissionDecision",
                "permissionDecisionReason",
                "additionalContext",
            ],
        )?;
        if specific
            .get("hookEventName")
            .and_then(Value::as_str)
            .is_some_and(|name| name != event.name())
        {
            return Err(Error::Invalid("hook output names another event".into()));
        }
        if specific
            .get("permissionDecision")
            .and_then(Value::as_str)
            .is_some_and(|decision| !matches!(decision, "allow" | "ask" | "deny"))
        {
            return Err(Error::Invalid("unknown hook permissionDecision".into()));
        }
        if specific
            .get("updatedInput")
            .is_some_and(|input| !input.is_object())
        {
            return Err(Error::Invalid("hook updatedInput must be an object".into()));
        }
    }
    Ok(())
}

fn validate_strings(value: &Value, names: &[&str]) -> Result<()> {
    for name in names {
        if value.get(name).is_some_and(|value| !value.is_string()) {
            return Err(Error::Invalid(format!("hook {name} must be a string")));
        }
    }
    Ok(())
}

fn take_string(value: &mut Value, name: &str) -> Option<String> {
    let Value::String(text) = value.as_object_mut()?.remove(name)? else {
        return None;
    };
    Some(text)
}

#[cfg(test)]
#[path = "output/properties.rs"]
mod properties;
