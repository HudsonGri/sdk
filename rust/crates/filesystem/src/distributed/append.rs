use super::*;

#[cfg(test)]
thread_local! {
    pub(super) static OMIT_REQUEST_BINDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static OMIT_OPERATION_LOCATOR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn bind_attempt_key(
    request: &mut CommitRequest,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    let hashing = codec_work(0, 0, acyclic_stream::request::commit_hash_bytes(request));
    work.admit(hashing)?;
    let digest = acyclic_stream::request::commit_digest(request).map_err(map_stream_error)?;
    work.charge(hashing)?;
    // Streams retains rejected decisions too. Its key names this exact request,
    // while the authority record retains the logical operation across cuts.
    #[cfg(test)]
    if OMIT_REQUEST_BINDING.get() {
        return Ok(());
    }
    request.idempotency_key = work.key(b"operation-attempt\0", &[&digest])?;
    Ok(())
}

#[cfg(test)]
mod tests;

pub(super) async fn seal<G: AuthorityGuard>(
    work: &mut AuthorityWork<'_>,
    guard: Option<&mut G>,
) -> Result<(), AuthorityError<G::Error>> {
    work.check()?;
    let mut child = work.operation()?;
    let result = crate::async_storage::seal(guard, &mut child).await;
    work.absorb(&child)?;
    result?;
    work.check()?;
    Ok(())
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn append<G: AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        let crate::GuardedAppend {
            authority_id,
            epoch,
            expected,
            commit,
            permit,
        } = request;
        if let Some(existing) = self
            .authority_find_operation(authority_id, commit.operation_id, work)
            .await?
        {
            seal(work, guard).await?;
            return Ok(if existing.fingerprint == commit.fingerprint {
                FsAppendOutcome::AlreadyCommitted(existing)
            } else {
                FsAppendOutcome::IdempotencyConflict {
                    committed_fingerprint: existing.fingerprint,
                }
            });
        }
        let snapshot = self.fork_snapshot(authority_id, work).await?;
        if snapshot.head.epoch != epoch {
            return Ok(FsAppendOutcome::Fenced {
                actual_epoch: snapshot.head.epoch,
            });
        }
        if snapshot.head != expected {
            return Ok(FsAppendOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let (gate, gate_tail, active) = self.reservation_gate(authority_id, work).await?;
        let admitted = match permit {
            PublicationPermit::Unrestricted => active.is_none(),
            PublicationPermit::Reservation {
                operation_id,
                gate_tail: tail,
                expected: head,
            } => {
                tail == gate_tail
                    && active == Some(OperationId::from_bytes(operation_id))
                    && head == expected
            }
            PublicationPermit::Lease {
                authority_id: authority,
                workspace_id,
                ..
            } => {
                active.is_none()
                    && AuthorityId::from_bytes(authority) == authority_id
                    && volume_authority_id(crate::WorkspaceId::from_bytes(workspace_id).volume_id())
                        == authority_id
            }
        };
        if !admitted {
            return Ok(FsAppendOutcome::Fenced {
                actual_epoch: snapshot.head.epoch,
            });
        }
        let sequence = expected
            .sequence
            .checked_next()
            .map_err(|_| AuthorityStoreError::SequenceExhausted)?;
        let mut durable = crate::streams_record::BorrowedStreamsDurableRecord {
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: expected.digest,
            digest: Digest::ZERO,
            payload: &commit.payload,
        };
        durable.digest = work.durable_digest(authority_id, &durable)?;
        let encoded = StreamsDurableRecord::encode_borrowed_with(
            &durable,
            STREAM_RECORD_LIMIT,
            |domain, fields| work.stream_bytes(domain, fields),
        )
        .map_err(AuthorityStoreError::DurableCodec)??;
        let records = work.path(authority_id, "/records", None)?;
        let epochs = work.path(authority_id, "/epochs", None)?;
        let retirement = work.path(authority_id, "/retirement", None)?;
        let operation_path = work.operation_path(authority_id, commit.operation_id)?;
        let mut conditions = work.vec(8)?;
        conditions.extend([
            CommitCondition::Absent {
                path: operation_path.clone(),
            },
            CommitCondition::Absent { path: retirement },
            CommitCondition::Tail {
                path: records.clone(),
                expected: snapshot.head.sequence.get() + 1,
            },
            CommitCondition::Tail {
                path: epochs,
                expected: snapshot.epoch_tail,
            },
            CommitCondition::Tail {
                path: gate.clone(),
                expected: gate_tail,
            },
        ]);
        let (lease_path, deadline) = if let PublicationPermit::Lease {
            workspace_id,
            lease_id,
            expires_at_millis,
            ..
        } = permit
        {
            let path = work.lease_path(
                crate::WorkspaceId::from_bytes(workspace_id),
                crate::OperationLeaseId::from_bytes(lease_id),
                expires_at_millis,
            )?;
            conditions.push(CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            });
            (Some(path), Some(expires_at_millis))
        } else {
            (None, None)
        };
        let mut mutations = work.vec(4)?;
        work.append_record(&mut mutations, records.clone(), [encoded])?;
        let location =
            work.stream_bytes(OPERATION_SEQUENCE_DOMAIN, &[&sequence.get().to_le_bytes()])?;
        work.append_record(&mut mutations, operation_path, [location])?;
        #[cfg(test)]
        if OMIT_OPERATION_LOCATOR.get() {
            // The append stays real; remove only its durable operation index.
            mutations.pop();
        }
        if let Some(generation) =
            generation_from_payload(authority_id, sequence, durable.payload, work)?
        {
            let lineage = work.path(authority_id, "/lineage", None)?;
            let locator = work.path(authority_id, "/generations/", Some(generation))?;
            match self.tail(locator.clone(), work).await {
                Ok(1) => {
                    let location = self.record(locator, 0, work).await?;
                    let tail = work
                        .decode_fixed::<8>(
                            &location.0.value,
                            LINEAGE_TAIL_DOMAIN,
                            "invalid Stream generation locator",
                        )
                        .map(u64::from_le_bytes)?;
                    let from = tail.checked_sub(1).ok_or_else(|| {
                        authority_corrupt("generation locator selects an empty lineage", work)
                    })?;
                    let entry = self.record(lineage, from, work).await?;
                    if work.decode_fixed::<32>(
                        &entry.0.value,
                        LINEAGE_DOMAIN,
                        "invalid Stream generation lineage record",
                    )? != *generation.digest().as_bytes()
                    {
                        return Err(authority_corrupt(
                            "generation locator selected another generation",
                            work,
                        )
                        .into());
                    }
                }
                Err(error) if authority_missing(&error) => {
                    let tail = match self.tail(lineage.clone(), work).await {
                        Ok(tail) => tail,
                        Err(error) if authority_missing(&error) => 0,
                        Err(error) => return Err(error.into()),
                    };
                    conditions.push(if tail == 0 {
                        CommitCondition::Absent {
                            path: lineage.clone(),
                        }
                    } else {
                        CommitCondition::Tail {
                            path: lineage.clone(),
                            expected: tail,
                        }
                    });
                    conditions.push(CommitCondition::Absent {
                        path: locator.clone(),
                    });
                    let entry =
                        work.stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
                    work.append_record(&mut mutations, lineage, [entry])?;
                    let location = work.stream_bytes(
                        LINEAGE_TAIL_DOMAIN,
                        &[&tail.saturating_add(1).to_le_bytes()],
                    )?;
                    work.append_record(&mut mutations, locator, [location])?;
                }
                Ok(_) => {
                    return Err(authority_corrupt(
                        "generation locator contains an invalid record count",
                        work,
                    )
                    .into());
                }
                Err(error) => return Err(error.into()),
            }
        }
        let mut request = CommitRequest {
            conditions,
            mutations,
            // The canonical digest excludes its key; binding replaces this
            // static placeholder before the request reaches a provider.
            idempotency_key: acyclic_stream::IdempotencyKey::new(
                acyclic_stream::OwnedBytes::from_static(b"pending-attempt"),
            )
            .map_err(map_stream_error)?,
        };
        bind_attempt_key(&mut request, work)?;
        let result = self
            .commit(request, deadline, work, guard.as_deref_mut())
            .await;
        if work.is_native() && result.is_ok() {
            work.check()?;
        }
        if !work.is_native() {
            let retained = gate.as_str().len() as u64
                + records.as_str().len() as u64
                + lease_path
                    .as_ref()
                    .map_or(0, |path| path.as_str().len() as u64);
            let retired = work
                .allocations
                .live_bytes()
                .checked_sub(retained)
                .ok_or_else(|| {
                    authority_corrupt("append path custody exceeds live request claims", work)
                })?;
            work.release(retired)?;
        }
        match result {
            Ok(output) => match &output.0 {
                CommitOutcome::Committed(envelope) => {
                    let record = envelope
                        .mutations
                        .iter()
                        .find_map(|mutation| match mutation {
                            acyclic_stream::CommittedMutation::Append(append)
                                if append.path == records =>
                            {
                                match append.records.as_slice() {
                                    [record]
                                        if record.sequence == sequence.get()
                                            && record.commit_id == envelope.commit_id =>
                                    {
                                        Some(record)
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        })
                        .ok_or_else(|| {
                            authority_corrupt("commit envelope omits its authority record", work)
                        })?;
                    Ok(FsAppendOutcome::Committed(durable_commit(
                        durable,
                        record.committed_at_micros,
                        commit.payload.clone(),
                    )))
                }
                CommitOutcome::Conflict(conflicts) => {
                    if let Some(existing) = self
                        .authority_find_operation(authority_id, commit.operation_id, work)
                        .await?
                    {
                        seal(work, guard).await?;
                        return Ok(if existing.fingerprint == commit.fingerprint {
                            FsAppendOutcome::AlreadyCommitted(existing)
                        } else {
                            FsAppendOutcome::IdempotencyConflict {
                                committed_fingerprint: existing.fingerprint,
                            }
                        });
                    }
                    let actual = self.fork_snapshot(authority_id, work).await?.head;
                    let rejected = conflicts.iter().any(|conflict| {
                        let path = match conflict {
                            acyclic_stream::CommitConflict::Tail { path, .. }
                            | acyclic_stream::CommitConflict::Head { path, .. }
                            | acyclic_stream::CommitConflict::Exists { path } => path,
                        };
                        path == &gate || lease_path.as_ref().is_some_and(|lease| path == lease)
                    });
                    Ok(if rejected || actual.epoch != epoch {
                        FsAppendOutcome::Fenced {
                            actual_epoch: actual.epoch,
                        }
                    } else {
                        FsAppendOutcome::Conflict { actual }
                    })
                }
            },
            Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            ))) => {
                let existing = self
                    .authority_find_operation(authority_id, commit.operation_id, work)
                    .await?
                    .ok_or_else(|| {
                        authority_corrupt(
                            "Stream idempotency mismatch has no operation record",
                            work,
                        )
                    })?;
                seal(work, guard).await?;
                Ok(FsAppendOutcome::IdempotencyConflict {
                    committed_fingerprint: existing.fingerprint,
                })
            }
            Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::DeadlineElapsed,
            ))) => Ok(FsAppendOutcome::Fenced {
                actual_epoch: epoch,
            }),
            Err(error) => Err(error),
        }
    }
}
