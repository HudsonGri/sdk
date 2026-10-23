use super::*;

#[derive(Clone, Copy)]
pub(in crate::filesystem::execution_journal) enum Node<'a> {
    Leaf { key: [u8; 32], value: &'a [u8] },
    Branch { bit: u16, children: [NodeRef; 2] },
}

impl<'a> Node<'a> {
    pub(in crate::filesystem::execution_journal) fn decode(
        bytes: &'a [u8],
        sequence: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let malformed = || invalid(io, "execution index node is malformed");
        if bytes.get(..4) != Some(b"EXI1") {
            return Err(malformed());
        }
        match bytes.get(4) {
            Some(0) => {
                let key = bytes
                    .get(5..37)
                    .and_then(|key| key.try_into().ok())
                    .ok_or_else(malformed)?;
                let length = u32::from_le_bytes(
                    bytes
                        .get(37..41)
                        .and_then(|length| length.try_into().ok())
                        .ok_or_else(malformed)?,
                );
                let value = bytes
                    .get(41..)
                    .filter(|value| value.len() as u64 == u64::from(length))
                    .ok_or_else(malformed)?;
                Ok(Self::Leaf { key, value })
            }
            Some(1) if bytes.len() == 87 => {
                let bit = u16::from_le_bytes(
                    bytes
                        .get(5..7)
                        .and_then(|bit| bit.try_into().ok())
                        .ok_or_else(malformed)?,
                );
                let link = |offset| -> Result<NodeRef> {
                    let child = NodeRef {
                        sequence: u64::from_le_bytes(
                            bytes
                                .get(offset..offset + 8)
                                .and_then(|v| v.try_into().ok())
                                .ok_or_else(malformed)?,
                        ),
                        digest: bytes
                            .get(offset + 8..offset + 40)
                            .and_then(|v| v.try_into().ok())
                            .ok_or_else(malformed)?,
                    };
                    if child.sequence >= sequence {
                        return Err(malformed());
                    }
                    Ok(child)
                };
                if bit >= 256 {
                    return Err(malformed());
                }
                Ok(Self::Branch {
                    bit,
                    children: [link(7)?, link(47)?],
                })
            }
            _ => Err(malformed()),
        }
    }

    pub(in crate::filesystem::execution_journal) fn encode(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<(OwnedBytes, u64)> {
        let length = match self {
            Self::Leaf { value, .. } => value.len().checked_add(41),
            Self::Branch { .. } => Some(87),
        }
        .filter(|length| *length <= acyclic_stream::MAX_RECORD_BYTES)
        .ok_or_else(|| invalid(io, "execution index node exceeds its record bound"))?;
        if let Some(io) = io {
            let charge = acyclic_fs::WorkCounters {
                bytes_encoded: length as u64,
                bytes_copied: length as u64,
                items_examined: 1,
                ..Default::default()
            };
            io.preflight(charge)?;
            io.charge(charge)?;
        }
        crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut bytes = crate::core::reserve_copy_vector(length, &mut quota)?;
            bytes.extend_from_slice(b"EXI1");
            match self {
                Self::Leaf { key, value } => {
                    bytes.push(0);
                    bytes.extend_from_slice(key);
                    bytes.extend_from_slice(
                        &u32::try_from(value.len())
                            .map_err(|_| {
                                invalid(io, "execution index value exceeds its record bound")
                            })?
                            .to_le_bytes(),
                    );
                    bytes.extend_from_slice(value);
                }
                Self::Branch { bit, children } => {
                    bytes.push(1);
                    bytes.extend_from_slice(&bit.to_le_bytes());
                    for child in children {
                        bytes.extend_from_slice(&child.sequence.to_le_bytes());
                        bytes.extend_from_slice(&child.digest);
                    }
                }
            }
            quota.transfer();
            crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())
        })
    }
}
