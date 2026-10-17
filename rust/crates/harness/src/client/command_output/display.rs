//! The one mapping from a completed command's `ok` value to transcript text.
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;

/// Display fields of a completed command's `ok` value, borrowed from the
/// canonical outcome file. Every field is raw, so an unexpected type hides
/// only that field and never the selected task.
#[derive(Default, Deserialize)]
pub(super) struct DisplayMetadata<'a> {
    #[serde(borrow)]
    text: Option<&'a RawValue>,
    pub(super) task: Option<crate::TaskId>,
    #[serde(borrow)]
    stdout: Option<&'a RawValue>,
    #[serde(borrow)]
    stderr: Option<&'a RawValue>,
    #[serde(borrow)]
    exit_code: Option<&'a RawValue>,
    #[serde(borrow)]
    success: Option<&'a RawValue>,
}

fn string(value: Option<&RawValue>) -> bool {
    value.is_some_and(|value| value.get().starts_with('"'))
}

impl<'a> DisplayMetadata<'a> {
    /// Whether [`Self::display`] shows text, without decoding it.
    pub(super) fn has_text(&self) -> bool {
        string(self.text) || string(self.stdout) || string(self.stderr)
    }

    /// Text a completed command shows in the transcript: an explicit `text`,
    /// or a process envelope's stdout then stderr, with a nonzero exit status
    /// line. Other structured results (acknowledgements, selections) show
    /// nothing.
    pub(super) fn display(&self) -> Option<Cow<'a, str>> {
        if let Some(text) = self.text.and_then(super::text) {
            return Some(text);
        }
        let stdout = self.stdout.and_then(super::text);
        let stderr = self.stderr.and_then(super::text);
        if stdout.is_none() && stderr.is_none() {
            return None;
        }
        let mut text = String::new();
        for stream in [stdout, stderr].into_iter().flatten() {
            if stream.is_empty() {
                continue;
            }
            text.push_str(&stream);
            if !stream.ends_with('\n') {
                text.push('\n');
            }
        }
        let code = self
            .exit_code
            .and_then(|code| serde_json::from_str::<serde_json::Number>(code.get()).ok());
        match code {
            Some(code) if code.as_i64() != Some(0) => text.push_str(&format!("exit {code}\n")),
            _ if self.success.is_some_and(|success| success.get() == "false") => {
                text.push_str("exit failed\n");
            }
            _ => {}
        }
        Some(Cow::Owned(text))
    }
}

#[cfg(test)]
mod tests {
    use super::DisplayMetadata;

    fn display(ok: &str) -> Option<String> {
        let metadata: DisplayMetadata<'_> = serde_json::from_str(ok).unwrap();
        let text = metadata.display().map(|text| text.into_owned());
        assert_eq!(metadata.has_text(), text.is_some(), "{ok}");
        text
    }

    #[test]
    fn text_and_process_envelopes_display_and_acknowledgements_do_not() {
        assert_eq!(display(r#"{"text":"done"}"#).as_deref(), Some("done"));
        assert_eq!(
            display(r#"{"success":true,"exit_code":0,"stdout":"LPSHELL\n","stderr":""}"#)
                .as_deref(),
            Some("LPSHELL\n")
        );
        assert_eq!(
            display(r#"{"success":false,"exit_code":2,"stdout":"out","stderr":"err"}"#).as_deref(),
            Some("out\nerr\nexit 2\n")
        );
        assert_eq!(
            display(
                r#"{"success":false,"exit_code":null,"stdout":"","stderr":"","stop":"timeout"}"#
            )
            .as_deref(),
            Some("exit failed\n")
        );
        assert_eq!(display(r#"{"task":null}"#), None);
        assert_eq!(display(r#"{"stdout":7,"text":["no"]}"#), None);
    }

    /// A non-string field hides only itself, never the selected task.
    #[test]
    fn mistyped_display_fields_keep_the_task() {
        let task = crate::TaskId::from_bytes([3; 16]);
        let ok = format!(r#"{{"task":"{task}","stdout":{{"bytes":1}},"exit_code":"x"}}"#);
        let metadata: DisplayMetadata<'_> = serde_json::from_str(&ok).unwrap();
        assert_eq!(metadata.task, Some(task));
        assert!(metadata.display().is_none());
    }
}
