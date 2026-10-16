use super::*;
mod heads;
pub(super) use heads::Heads;

impl<P: StreamProvider> Session<P> {
    pub(super) fn record(
        &mut self,
        identity: wire::SessionChannel,
        record: Result<Event, StreamError>,
    ) -> Result<(), StreamError> {
        let Some(channel) = self.channels.get(&identity.channel_id).filter(|channel| {
            channel.generation == identity.generation && channel.ending.is_none()
        }) else {
            return Ok(());
        };
        let (after, item, key) = match record {
            Ok(Event::Opened(preceding)) => {
                self.controls.push_back(frame(
                    identity.channel_id,
                    identity.generation,
                    Body::Opened(wire::SessionOpened {
                        resume: Some(channel.applied.clone()),
                        preceding_commit_id: preceding
                            .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(id.as_bytes())),
                    }),
                ));
                return Ok(());
            }
            Ok(Event::Child(child, after)) => (
                after,
                Item::Child(wire::Child {
                    path: child.path.to_string(),
                }),
                None,
            ),
            Ok(Event::Record(record)) => {
                let next = record
                    .sequence
                    .checked_add(1)
                    .ok_or(StreamError::OutOfRange)?;
                let (item, key) = if channel.latest {
                    let value = match wire::SessionLatestValue::decode(record.value.as_ref()) {
                        Ok(value) if !value.key.is_empty() => value,
                        _ => return self.record(identity, Err(StreamError::InvalidArgument)),
                    };
                    let key = (!value.attention).then(|| value.key.clone());
                    (Item::LatestValue(value), key)
                } else {
                    (Item::Record(wire_codec::record_wire(record)), None)
                };
                (cursor(next), item, key)
            }
            Err(error) => {
                let channel = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?;
                channel.ending = Some(error);
                channel.cancel.take();
                self.finish_if_drained(identity.channel_id)?;
                return Ok(());
            }
        };
        let next = after.next;
        let frame = frame(
            identity.channel_id,
            identity.generation,
            Body::Data(wire::SessionData {
                after: Some(after),
                accepted: next,
                durable: 0,
                item: Some(item),
            }),
        );
        // Channel-local candidates preserve global insertion/cursor order while
        // avoiding a scan of every other follower for an ordinary-key update.
        let previous = key
            .as_ref()
            .map(|key| {
                find_key(
                    &self.pending,
                    channel.pending.iter(),
                    identity.channel_id,
                    key,
                )
            })
            .transpose()?
            .flatten();
        let token = self.next_pending;
        let next = token.checked_add(1).ok_or(StreamError::Capacity)?;
        let replaced_bytes = previous
            .map(|token| self.pending[&token].frame.encoded_len())
            .unwrap_or(0);
        let bytes = self
            .pending_bytes
            .checked_sub(replaced_bytes)
            .and_then(|bytes| bytes.checked_add(frame.encoded_len()))
            .filter(|bytes| *bytes <= self.limits.pending_bytes)
            .ok_or(StreamError::Capacity)?;
        if self.pending.len() - usize::from(previous.is_some()) >= self.limits.pending_frames {
            return Err(StreamError::Capacity);
        }
        if let Some(previous) = previous {
            self.take_pending(previous)?;
            self.channels
                .get_mut(&identity.channel_id)
                .ok_or(StreamError::Unavailable)?
                .pending
                .retain(|token| *token != previous);
        }
        self.next_pending = next;
        self.pending_bytes = bytes;
        self.pending.insert(token, Pending { frame, key });
        self.channels
            .get_mut(&identity.channel_id)
            .ok_or(StreamError::Unavailable)?
            .pending
            .push_back(token);
        self.index_channel(identity.channel_id)?;
        self.refresh(Some(identity.channel_id))?;
        Ok(())
    }

    fn finish_if_drained(&mut self, id: u64) -> Result<(), StreamError> {
        if self
            .channels
            .get(&id)
            .is_some_and(|channel| !channel.pending.is_empty())
        {
            return Ok(());
        }
        if let Some(channel) = self.channels.get(&id)
            && let Some(error) = &channel.ending
        {
            self.controls.push_back(frame(
                id,
                channel.generation,
                Body::Closed(wire::SessionClosed {
                    code: error.code().into(),
                    message: String::new(),
                }),
            ));
            self.remove(id)?;
        }
        Ok(())
    }

    pub(super) fn ready(&mut self) -> Result<Option<wire::SessionFrame>, StreamError> {
        if self.credit.frames == 0 || self.credit.bytes == 0 {
            return Ok(None);
        }
        if let Some(id) = self.ready.next(self.credit.bytes) {
            let token = self
                .channels
                .get(&id)
                .and_then(|channel| channel.pending.front())
                .copied()
                .ok_or(StreamError::Unavailable)?;
            let pending = self.pending.get(&token).ok_or(StreamError::Unavailable)?;
            let mut local = self
                .channels
                .get(&id)
                .ok_or(StreamError::Unavailable)?
                .credit;
            wire_codec::session::consume_credit(&mut local, &pending.frame)?;
            wire_codec::session::consume_credit(&mut self.credit, &pending.frame)?;
            let pending = self.take_pending(token)?;
            let channel = self.channels.get_mut(&id).ok_or(StreamError::Unavailable)?;
            if channel.pending.pop_front() != Some(token) {
                return Err(StreamError::Unavailable);
            }
            channel.credit = local;
            if let Some(Body::Data(data)) = &pending.frame.body {
                channel.sent = data.after.clone().ok_or(StreamError::Unavailable)?;
            }
            self.finish_if_drained(id)?;
            self.index_channel(id)?;
            self.refresh(Some(id))?;
            return Ok(Some(pending.frame));
        }
        Ok(None)
    }

    pub(super) fn take_pending(&mut self, token: u64) -> Result<Pending, StreamError> {
        let pending = self
            .pending
            .remove(&token)
            .ok_or(StreamError::Unavailable)?;
        let bytes = pending.frame.encoded_len();
        self.pending_bytes = self
            .pending_bytes
            .checked_sub(bytes)
            .ok_or(StreamError::Unavailable)?;
        Ok(pending)
    }

    // One leaf per channel contains its head size, or is disabled by local credit.
    // Aggregate credit queries the byte minimum without waking/scanning followers.
    pub(super) fn index_channel(&mut self, id: u64) -> Result<(), StreamError> {
        let Some(channel) = self.channels.get_mut(&id) else {
            return Ok(());
        };
        let Some(token) = channel.pending.front().copied() else {
            if let Some(slot) = channel.slot {
                self.ready.set(slot, None)?;
            }
            return Ok(());
        };
        let pending = self.pending.get(&token).ok_or(StreamError::Unavailable)?;
        let bytes = pending.frame.encoded_len();
        let cost =
            (channel.credit.frames > 0 && channel.credit.bytes >= bytes as u64).then_some(bytes);
        let slot = match channel.slot {
            Some(slot) => slot,
            None => {
                let slot = self.ready.insert(id)?;
                channel.slot = Some(slot);
                slot
            }
        };
        self.ready.set(slot, cost)
    }
}

fn find_key<'a>(
    pending: &BTreeMap<u64, Pending>,
    candidates: impl DoubleEndedIterator<Item = &'a u64>,
    channel: u64,
    key: &Bytes,
) -> Result<Option<u64>, StreamError> {
    for token in candidates.rev() {
        examined();
        let candidate = pending.get(token).ok_or(StreamError::Unavailable)?;
        if candidate.frame.channel_id == channel && candidate.key.as_ref() == Some(key) {
            return Ok(Some(*token));
        }
    }
    Ok(None)
}

fn examined() {
    #[cfg(all(test, not(acyclic_stream_loom)))]
    WORK.with(|work| work.set(work.get() + 1));
}
#[cfg(all(test, not(acyclic_stream_loom)))]
std::thread_local! { static WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(any(test, acyclic_stream_loom))]
pub(super) mod fixture;
#[cfg(acyclic_stream_loom)]
mod model;
#[cfg(all(test, not(acyclic_stream_loom)))]
mod tests;
#[cfg(acyclic_stream_loom)]
pub use model::verify_queued_admission;
