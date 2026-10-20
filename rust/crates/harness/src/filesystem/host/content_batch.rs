use super::*;

/// One file in an atomic, idempotent content publication.
pub struct ContentUpload<'a> {
    /// Volume-relative public content path.
    pub path: &'a str,
    /// Complete payload.
    pub bytes: &'a [u8],
    /// Authenticated content media type.
    pub media_type: &'a str,
    /// Authenticated display name.
    pub display_name: &'a str,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Publishes related files in one generation and retains that generation once.
    pub async fn put_content_batch(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        files: &[ContentUpload<'_>],
        maximum_bytes: u64,
        key: &IdempotencyKey,
        io: &PreparationIo,
    ) -> Result<Vec<FileRef>>
    where
        A: 'static,
        O: 'static,
    {
        grant.require(volume, VolumeOperation::Write, Some(io))?;
        let invalid = || {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "content batch has a foreign provider, invalid path, duplicate path or size",
            )
        };
        if volume.provider() != &self.provider || files.is_empty() {
            return Err(invalid());
        }
        let operation = fork_preparer_io::operation_key(key, Some(io))?;
        let provisional_version = fork_preparer_io::hex_key(&operation.into_bytes(), Some(io))?;
        let mut references = crate::model::vec_with_capacity(files.len(), Some(io))?;
        let capacity = files
            .len()
            .checked_mul(3)
            .and_then(|n| n.checked_add(3))
            .ok_or_else(invalid)?;
        let mut mutations = crate::model::vec_with_capacity(capacity, Some(io))?;
        for path in [
            "/.system/harness-upload-batches",
            "/.system/harness-file-metadata",
        ] {
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: stock_string_copy(path, Some(io))?,
            });
        }
        for (index, file) in files.iter().enumerate() {
            io.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            crate::executor::with_history_io(Some(io), |work| {
                crate::conversation::validate_content_path(file.path, work)
            })?;
            if file.bytes.len() as u64 > maximum_bytes
                || file.path == ".system"
                || file.path.starts_with(".system/")
                || file.path == "turns/.canonical"
                || file.path.starts_with("turns/.canonical/")
            {
                return Err(invalid());
            }
            for earlier in &files[..index] {
                io.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if earlier.path == file.path {
                    return Err(invalid());
                }
            }
            let reference = crate::executor::with_history_io(Some(io), |work| {
                FileRef::new(
                    volume.copy(work)?,
                    stock_string_copy(file.path, Some(io))?,
                    stock_string_copy(&provisional_version, Some(io))?,
                    FileDescriptor::from_bytes(file.bytes, file.media_type, work)?,
                    stock_string_copy(file.display_name, Some(io))?,
                    work,
                )
            })?;
            let absolute = crate::executor::with_history_io(Some(io), |work| {
                crate::executor::preparation_format(format_args!("/{}", file.path), work)
            })?;
            if let Some((parent, _)) = absolute.rsplit_once('/')
                && !parent.is_empty()
            {
                mutations.push(WorkspaceMutation::CreateDirectory {
                    path: stock_string_copy(parent, Some(io))?,
                });
            }
            mutations.push(WorkspaceMutation::PutFile {
                path: absolute,
                bytes: fork_preparer_io::clone_bytes(file.bytes, Some(io))?,
            });
            let metadata = content_metadata_path_io(file.path, false, Some(io))?;
            mutations.push(WorkspaceMutation::PutFile {
                path: crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::preparation_format(format_args!("/{metadata}"), work)
                })?,
                bytes: io.encode(&reference, io.remaining()?.bytes_encoded, false)?,
            });
            references.push(reference);
        }
        let receipt = content_identity_path_io(
            key.as_str(),
            false,
            ".system/harness-upload-batches/",
            Some(io),
        )?;
        let receipt_bytes = io.encode(&references, io.remaining()?.bytes_encoded, true)?;
        mutations.push(WorkspaceMutation::PutFile {
            path: crate::executor::with_history_io(Some(io), |work| {
                crate::executor::preparation_format(format_args!("/{receipt}"), work)
            })?,
            bytes: receipt_bytes,
        });
        if let Some(prior) = self
            .content_batch_for_operation(volume, grant, key, maximum_bytes, io)
            .await?
        {
            let mut differs = prior.len() != references.len();
            for (a, b) in prior.iter().zip(&references) {
                io.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                differs |= a.path() != b.path()
                    || a.descriptor() != b.descriptor()
                    || a.display_name() != b.display_name();
            }
            if differs {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "content batch retry differs from its original request",
                ));
            }
            return Ok(prior);
        }
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, Some(io))?;
        let generation = self
            .apply_generation(
                &workspace,
                None,
                &mutations,
                IdempotencyKey::new(stock_string_copy(key.as_str(), Some(io))?)?,
                Some(io),
                None,
            )?
            .await?;
        fork_preparer_io::retain(&generation, Some(io)).await?;
        self.content_batch_references(&generation, volume, key, references, maximum_bytes, io)
            .await
    }

    /// Recovers every file from the original atomic publication, independent of mutable head paths.
    pub async fn content_batch_for_operation(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        key: &IdempotencyKey,
        maximum_bytes: u64,
        io: &PreparationIo,
    ) -> Result<Option<Vec<FileRef>>> {
        grant.require(volume, VolumeOperation::Write, Some(io))?;
        if volume.provider() != &self.provider {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "content batch belongs to another provider",
            ));
        }
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, Some(io))?;
        let Some(generation) = self.operation_generation(&workspace, key, Some(io)).await? else {
            return Ok(None);
        };
        let path = content_identity_path_io(
            key.as_str(),
            true,
            ".system/harness-upload-batches/",
            Some(io),
        )?;
        let bytes = read_generation(
            &generation,
            &path,
            io.remaining()?.source_bytes_read,
            Some(io),
        )
        .await?;
        let references: Vec<FileRef> = io.decode_json(&bytes)?;
        let canonical = io.encode(&references, io.remaining()?.bytes_encoded, true)?;
        if canonical.as_ref() != bytes.as_ref() || references.is_empty() {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "content batch receipt is not canonical",
            ));
        }
        fork_preparer_io::retain(&generation, Some(io)).await?;
        self.content_batch_references(&generation, volume, key, references, maximum_bytes, io)
            .await
            .map(Some)
    }

    async fn content_batch_references(
        &self,
        generation: &Generation<A, O>,
        volume: &VolumeRef,
        key: &IdempotencyKey,
        references: Vec<FileRef>,
        maximum_bytes: u64,
        io: &PreparationIo,
    ) -> Result<Vec<FileRef>> {
        let operation = fork_preparer_io::operation_key(key, Some(io))?;
        let provisional = fork_preparer_io::hex_key(&operation.into_bytes(), Some(io))?;
        let generation_ref = self.generation_ref_id(generation.id(), Some(io))?;
        let version = fork_preparer_io::hex_key(generation_ref.as_resource().key(), Some(io))?;
        let mut output = crate::model::vec_with_capacity(references.len(), Some(io))?;
        for recorded in references {
            crate::executor::with_history_io(Some(io), |work| recorded.validate(work))?;
            let mut duplicate = false;
            for file in &output {
                io.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                duplicate |= FileRef::path(file) == recorded.path();
            }
            if recorded.volume() != volume
                || recorded.version() != provisional
                || recorded.path() == ".system"
                || recorded.path().starts_with(".system/")
                || recorded.path() == "turns/.canonical"
                || recorded.path().starts_with("turns/.canonical/")
                || duplicate
            {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "content batch receipt has a foreign or duplicate identity",
                ));
            }
            let metadata_path = content_metadata_path_io(recorded.path(), true, Some(io))?;
            let metadata = read_generation(
                generation,
                &metadata_path,
                io.remaining()?.source_bytes_read,
                Some(io),
            )
            .await?;
            let encoded = io.encode(&recorded, io.remaining()?.bytes_encoded, false)?;
            if metadata.as_ref() != encoded.as_ref() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "content batch metadata differs from its original receipt",
                ));
            }
            let reference = crate::executor::with_history_io(Some(io), |work| {
                FileRef::new(
                    volume.copy(work)?,
                    stock_string_copy(recorded.path(), Some(io))?,
                    stock_string_copy(&version, Some(io))?,
                    FileDescriptor::new(
                        *recorded.descriptor().sha256(),
                        recorded.descriptor().byte_length(),
                        stock_string_copy(recorded.descriptor().media_type(), Some(io))?,
                    )?,
                    stock_string_copy(recorded.display_name(), Some(io))?,
                    work,
                )
            })?;
            let path = crate::executor::with_history_io(Some(io), |work| {
                crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
            })?;
            let bytes = read_generation(generation, &path, maximum_bytes, Some(io)).await?;
            crate::executor::with_history_io(Some(io), |work| {
                reference.descriptor().verify(&bytes, work)
            })?;
            output.push(reference);
        }
        Ok(output)
    }
}

use crate::executor::{stock_boundary, stock_string_copy};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AggregateKind, AuthorityIssuer};

    #[tokio::test]
    async fn atomic_batch_retries_pin_original_files_and_reject_foreign_or_denied_input()
    -> Result<()> {
        let provider =
            ProviderRef::new("batch".into(), "filesystem".into(), "1".into(), &mut None)?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([31; 16]);
        let volume = VolumeRef::new(
            provider,
            "batch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "batch",
            [32; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "batch".into(),
            },
        );
        let scope = issuer.issue(
            "owner",
            &crate::Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]),
            Some(agent),
            None,
            &mut None,
        )?;
        let grant = ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &scope,
            &volume,
            VolumeOperation::Write,
            4096,
            &mut None,
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::for_unmeasured(&cancellation)?;
        let key = IdempotencyKey::new("pair")?;
        let files = [
            ContentUpload {
                path: "inputs/a",
                bytes: b"first",
                media_type: "text/plain",
                display_name: "a",
            },
            ContentUpload {
                path: "inputs/b",
                bytes: b"second",
                media_type: "text/plain",
                display_name: "b",
            },
        ];
        let denied = PreparationIo::from_budget(acyclic_fs::WorkBudget::default(), &cancellation)?;
        assert!(
            host.put_content_batch(&volume, &grant, &files, 4096, &key, &denied)
                .await
                .is_err()
        );
        assert!(
            host.content_batch_for_operation(&volume, &grant, &key, 4096, &io)
                .await?
                .is_none()
        );
        let duplicate = [
            ContentUpload {
                path: "inputs/a",
                bytes: b"first",
                media_type: "text/plain",
                display_name: "a",
            },
            ContentUpload {
                path: "inputs/a",
                bytes: b"second",
                media_type: "text/plain",
                display_name: "b",
            },
        ];
        assert!(
            host.put_content_batch(&volume, &grant, &duplicate, 4096, &key, &io)
                .await
                .is_err()
        );
        let first = host
            .put_content_batch(&volume, &grant, &files, 4096, &key, &io)
            .await?;
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].version(), first[1].version());
        let retried = host
            .put_content_batch(&volume, &grant, &files, 4096, &key, &io)
            .await?;
        assert_eq!(first, retried);
        host.put_content(
            &volume,
            &grant,
            "inputs/a",
            b"replaced",
            "text/plain",
            "a",
            4096,
            &IdempotencyKey::new("replacement")?,
            None,
            Some(&io),
        )
        .await?;
        let recovered = host
            .content_batch_for_operation(&volume, &grant, &key, 4096, &io)
            .await?
            .unwrap();
        assert_eq!(first, recovered);
        assert_eq!(
            host.read_pinned_io(&recovered[0], 4096, Some(&io))
                .await?
                .as_ref(),
            b"first"
        );
        let changed = [ContentUpload {
            path: "inputs/a",
            bytes: b"changed",
            media_type: "text/plain",
            display_name: "a",
        }];
        assert!(
            host.put_content_batch(&volume, &grant, &changed, 4096, &key, &io)
                .await
                .is_err()
        );
        let foreign = VolumeRef::new(
            volume.provider().clone(),
            "foreign".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        assert!(
            host.put_content_batch(&foreign, &grant, &files, 4096, &key, &io)
                .await
                .is_err()
        );
        let cancelled = acyclic_fs::CancellationToken::new();
        let stopped = PreparationIo::for_unmeasured(&cancelled)?;
        cancelled.cancel();
        let another = IdempotencyKey::new("cancelled")?;
        assert!(
            host.put_content_batch(&volume, &grant, &files, 4096, &another, &stopped)
                .await
                .is_err()
        );
        assert!(
            host.content_batch_for_operation(&volume, &grant, &another, 4096, &io)
                .await?
                .is_none()
        );
        Ok(())
    }
}
