use super::*;

impl CommandResolution {
    /// Moves a resolved prompt into the stock model command's input. Mentioned
    /// files retain their pinned revisions and use verified bounded text projection.
    /// Repeated file identities attach once, in first-mention order.
    pub fn into_model_input(self) -> Result<crate::model::ModelContent> {
        use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
        let Self::Prompt { text, mentions, .. } = self else {
            return Err(Error::Invalid(
                "command selects a view or typed operation".into(),
            ));
        };
        if mentions.is_empty() {
            let content = ModelContent::Text(text);
            content.validate_user_input(&mut None)?;
            return Ok(content);
        }
        // Borrow identities while selecting indices, then move refs into the input.
        // No file ref, path, version or prompt text is cloned for attachment.
        let mut seen = BTreeMap::new();
        let mut selected = Vec::with_capacity(mentions.len());
        for (index, mention) in mentions.iter().enumerate() {
            let file = &mention.file;
            let identity = (file.volume(), file.path(), file.version());
            if let Some(previous) = seen.insert(identity, file) {
                if previous != file {
                    return Err(Error::Conflict("mentioned file identity changed".into()));
                }
            } else {
                selected.push(index);
            }
        }
        drop(seen);
        let mut parts = Vec::with_capacity(selected.len() + 1);
        parts.push(ModelContentPart::Text { text });
        let mut selected = selected.into_iter().peekable();
        for (index, mention) in mentions.into_iter().enumerate() {
            if selected.peek() == Some(&index) {
                selected.next();
                parts.push(ModelContentPart::File {
                    policy: if mention.file.descriptor().media_type().starts_with("image/") {
                        FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
                            intent: crate::model::NativeMediaIntent::Image {
                                detail: crate::model::ImageDetail::Auto,
                            },
                            maximum_bytes: mention.file.descriptor().byte_length(),
                            maximum_work: 16_384,
                            configuration: None,
                        }))
                    } else {
                        FileProjectionPolicy::BoundedFull
                    },
                    file: mention.file,
                });
            }
        }
        let content = ModelContent::Parts(parts);
        content.validate_user_input(&mut None)?;
        Ok(content)
    }
}
