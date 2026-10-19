//! Capture only declared command directories through held, no-follow roots.
use super::*;
use acyclic_fs::{
    demand::{DemandError, DemandSource, SourceNodeKind, native::NativeDemandSource},
    kernel::NamespacePath,
};

pub(super) struct Bounds<'a> {
    pub directories: &'a mut u32,
    pub entries: &'a mut u32,
    pub remaining: &'a mut u64,
    pub limits: ContextDiscoveryLimits,
}

pub(super) async fn capture(
    settings: &crate::native_settings::SettingsConfig,
    prefix: &str,
    publisher: &dyn ContentPublisher,
    work: &acyclic_fs::NativeMaterializationWork,
    cancellation: &acyclic_fs::CancellationToken,
    bounds: &mut Bounds<'_>,
) -> Result<()> {
    let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
    let mut accounting =
        acyclic_fs::kernel::KernelAccounting::NativeOriginal { work, cancellation };
    // Establish an empty discoverable root even when no command directory exists.
    publish(prefix, ".root", b"", publisher).await?;
    for (directory, destination) in settings.command_roots() {
        let source = match NativeDemandSource::open(
            std::borrow::Cow::Borrowed(&directory),
            config.profile,
            config.limits,
            None,
            None,
            work,
            cancellation,
        )
        .await
        {
            Ok(source) => source,
            Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(storage(error)),
        };
        let reference = source.reference();
        let mut pending = vec![String::new()];
        while let Some(relative) = pending.pop() {
            *bounds.directories += 1;
            if *bounds.directories > bounds.limits.directories {
                return Err(Error::Invalid("command directory bound exceeded".into()));
            }
            let (path, allocation) = NamespacePath::parse_in_profile(
                &format!("/{relative}"),
                config.profile,
                config.limits,
                &mut accounting,
                cancellation,
            )?;
            let path = Held {
                value: path,
                _allocation: allocation,
            };
            let mut cursor = None;
            loop {
                let mut page = source
                    .list_page(reference, &path.value, cursor, 128, work, cancellation)
                    .await
                    .map_err(storage)?;
                for entry in &page.entries {
                    *bounds.entries += 1;
                    if *bounds.entries > bounds.limits.entries {
                        return Err(Error::Invalid("command entry bound exceeded".into()));
                    }
                    let (name, allocation) = entry
                        .name
                        .unicode_text(work, cancellation)
                        .map_err(storage)?;
                    let name = Held {
                        value: name,
                        _allocation: allocation,
                    };
                    let child = if relative.is_empty() {
                        name.value.clone()
                    } else {
                        format!("{relative}/{}", name.value)
                    };
                    crate::conversation::validate_content_path(&child, &mut None)?;
                    match entry.node.kind {
                        SourceNodeKind::Directory => pending.push(child),
                        SourceNodeKind::RegularFile if name.value.ends_with(".md") => {
                            let length = entry.node.logical_bytes.ok_or_else(|| {
                                Error::Invalid("command file has no length".into())
                            })?;
                            if length > *bounds.remaining {
                                return Err(Error::Invalid("command byte bound exceeded".into()));
                            }
                            let (file, allocation) = NamespacePath::parse_in_profile(
                                &format!("/{child}"),
                                config.profile,
                                config.limits,
                                &mut accounting,
                                cancellation,
                            )?;
                            let file = Held {
                                value: file,
                                _allocation: allocation,
                            };
                            let chunk = source
                                .read_range(
                                    reference,
                                    &file.value,
                                    entry.node.version,
                                    0,
                                    length,
                                    work,
                                    cancellation,
                                )
                                .await
                                .map_err(storage)?;
                            if chunk.bytes.len() as u64 != length {
                                return Err(Error::Conflict(
                                    "command file changed during capture".into(),
                                ));
                            }
                            publish(
                                prefix,
                                &format!("{destination}/{child}"),
                                chunk.bytes.as_ref(),
                                publisher,
                            )
                            .await?;
                            *bounds.remaining -= length;
                        }
                        _ if name.value.ends_with(".md") => {
                            return Err(Error::Invalid(
                                "command entry is not a regular file".into(),
                            ));
                        }
                        _ => {}
                    }
                }
                if let Some(publication) = page.publication.as_mut() {
                    publication.accept(&mut accounting).map_err(storage)?;
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn publish(
    prefix: &str,
    path: &str,
    bytes: &[u8],
    publisher: &dyn ContentPublisher,
) -> Result<()> {
    let destination = format!("{prefix}/{path}");
    publisher
        .stage_once(
            context::publication_operation(
                b"acyclic.local.composer-file/v1\0",
                destination.as_bytes(),
            ),
            &destination,
            bytes,
            "text/plain",
            path,
            None,
        )?
        .await?;
    Ok(())
}
