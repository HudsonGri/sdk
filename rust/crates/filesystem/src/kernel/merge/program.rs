//! Canonical restart program for the retained merge frontier. It contains the
//! accepted mutations and exact alternatives, never instructions to diff again.
use super::*;
use crate::foundation::{Digest, VolumeId, usize_to_u64};
use crate::kernel::codec::{Decoder, EncodeWorkStep, Encoder, ReadAllocationStep};

const DOMAIN: &[u8] = b"ACYFSMRG";
type Steps<'a> = dyn FnMut(ReadAllocationStep) -> Result<(), FsError> + 'a;

fn invalid() -> FsError {
    FsError::InvalidDiff
}
fn add(a: usize, b: usize) -> Result<usize, FsError> {
    a.checked_add(b).ok_or_else(|| WorkError::Overflow.into())
}

/// Fields compose the existing canonical scalar/record/name codecs; no Rust
/// memory representation or platform-sized integer is written to storage.
trait Field: Sized {
    fn length(&self) -> Result<usize, FsError>;
    fn write(&self, out: &mut Encoder) -> Result<(), FsError>;
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError>;
    fn nested(&self) -> Result<usize, FsError> {
        Ok(0)
    }
}

macro_rules! scalar {
    ($ty:ty, $bytes:expr, $method:ident) => {
        impl Field for $ty {
            fn length(&self) -> Result<usize, FsError> {
                Ok($bytes)
            }
            fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
                out.$method(*self);
                Ok(())
            }
            fn read(
                input: &mut Decoder<'_>,
                _: DecodeLimits,
                steps: &mut Steps<'_>,
            ) -> Result<Self, FsError> {
                input.$method(&mut |s| steps(s))
            }
        }
    };
}
scalar!(u32, 4, u32);
scalar!(u64, 8, u64);
impl Field for usize {
    fn length(&self) -> Result<usize, FsError> {
        Ok(8)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        out.u64(usize_to_u64(*self));
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        _: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        usize::try_from(input.u64(&mut |s| steps(s))?).map_err(|_| invalid())
    }
}
macro_rules! fixed_id {
    ($ty:ty, $bytes:expr) => {
        impl Field for $ty {
            fn length(&self) -> Result<usize, FsError> {
                Ok($bytes)
            }
            fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
                out.fixed(&self.into_bytes());
                Ok(())
            }
            fn read(
                input: &mut Decoder<'_>,
                _: DecodeLimits,
                steps: &mut Steps<'_>,
            ) -> Result<Self, FsError> {
                Ok(Self::from_bytes(input.fixed(&mut |s| steps(s))?))
            }
        }
    };
}
fixed_id!(FileId, 16);
fixed_id!(VolumeId, 16);
impl Field for ObjectId {
    fn length(&self) -> Result<usize, FsError> {
        Ok(33)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        out.u8(self.kind.canonical_tag());
        out.fixed(self.digest.as_bytes());
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        _: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        Ok(Self {
            kind: ObjectKind::from_canonical_tag(input.u8(&mut |s| steps(s))?)
                .map_err(|_| invalid())?,
            digest: Digest::from_bytes(input.fixed(&mut |s| steps(s))?),
        })
    }
}
impl Field for bool {
    fn length(&self) -> Result<usize, FsError> {
        Ok(1)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        out.u8(u8::from(*self));
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        _: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        match input.u8(&mut |s| steps(s))? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid()),
        }
    }
}
impl<T: Field> Field for Option<T> {
    fn length(&self) -> Result<usize, FsError> {
        add(1, self.as_ref().map_or(Ok(0), Field::length)?)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        self.is_some().write(out)?;
        if let Some(value) = self {
            value.write(out)?;
        }
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        if bool::read(input, limits, steps)? {
            Ok(Some(T::read(input, limits, steps)?))
        } else {
            Ok(None)
        }
    }
    fn nested(&self) -> Result<usize, FsError> {
        self.as_ref().map_or(Ok(0), Field::nested)
    }
}
impl<A: Field, B: Field> Field for (A, B) {
    fn length(&self) -> Result<usize, FsError> {
        add(self.0.length()?, self.1.length()?)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        self.0.write(out)?;
        self.1.write(out)
    }
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        Ok((
            A::read(input, limits, steps)?,
            B::read(input, limits, steps)?,
        ))
    }
    fn nested(&self) -> Result<usize, FsError> {
        add(self.0.nested()?, self.1.nested()?)
    }
}
impl<T: Field> Field for [T; 3] {
    fn length(&self) -> Result<usize, FsError> {
        self.iter().try_fold(0, |n, v| add(n, v.length()?))
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        for v in self {
            v.write(out)?;
        }
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        Ok([
            T::read(input, limits, steps)?,
            T::read(input, limits, steps)?,
            T::read(input, limits, steps)?,
        ])
    }
    fn nested(&self) -> Result<usize, FsError> {
        self.iter().try_fold(0, |n, v| add(n, v.nested()?))
    }
}
impl Field for crate::kernel::FileKind {
    fn length(&self) -> Result<usize, FsError> {
        Ok(1)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        out.u8(self.tag());
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        _: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        Self::from_tag(input.u8(&mut |s| steps(s))?).map_err(|_| invalid())
    }
}
impl Field for LogicalName {
    fn length(&self) -> Result<usize, FsError> {
        add(5, self.as_bytes().len())
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        out.u8(self.encoding().tag());
        out.bounded_bytes(self.as_bytes())?;
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        let encoding = crate::kernel::NameEncoding::from_tag(input.u8(&mut |s| steps(s))?)
            .map_err(|_| invalid())?;
        let bytes = input.bounded_bytes(limits.maximum_name_bytes, &mut |s| steps(s))?;
        Self::new(encoding, bytes, limits.maximum_name_bytes).map_err(|_| invalid())
    }
    fn nested(&self) -> Result<usize, FsError> {
        Ok(self.retained_bytes())
    }
}
impl Field for FileRecord {
    fn length(&self) -> Result<usize, FsError> {
        add(
            8,
            super::super::file_table::file_record_encoded_length(*self)?,
        )
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        // Intermediate link counts can be zero. The published-record codec
        // keeps its invariant; the extra scalar restores the intermediate count.
        let canonical = Self {
            link_count: self.link_count.max(1),
            ..*self
        };
        canonical.validate().map_err(|_| invalid())?;
        out.u64(self.link_count);
        super::super::file_table::encode_record(out, canonical);
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        _: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        let actual = input.u64(&mut |s| steps(s))?;
        let mut record =
            super::super::file_table::decode_record(input, &mut |s| steps(s), |record| record)?;
        if record.link_count != actual.max(1) {
            return Err(invalid());
        }
        record.link_count = actual;
        Ok(record)
    }
}
impl Field for MergeConflict {
    fn length(&self) -> Result<usize, FsError> {
        add(
            1,
            match self {
                Self::File(id) => id.length()?,
                Self::Binding { directory_id, name } => {
                    add(directory_id.length()?, name.length()?)?
                }
            },
        )
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        match self {
            Self::File(id) => {
                out.u8(0);
                id.write(out)
            }
            Self::Binding { directory_id, name } => {
                out.u8(1);
                directory_id.write(out)?;
                name.write(out)
            }
        }
    }
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        match input.u8(&mut |s| steps(s))? {
            0 => Ok(Self::File(FileId::read(input, limits, steps)?)),
            1 => Ok(Self::Binding {
                directory_id: FileId::read(input, limits, steps)?,
                name: LogicalName::read(input, limits, steps)?,
            }),
            _ => Err(invalid()),
        }
    }
    fn nested(&self) -> Result<usize, FsError> {
        match self {
            Self::File(_) => Ok(0),
            Self::Binding { name, .. } => name.nested(),
        }
    }
}
impl Field for ContentMerge {
    fn length(&self) -> Result<usize, FsError> {
        Ok(1)
    }
    fn write(&self, out: &mut Encoder) -> Result<(), FsError> {
        out.u8(u8::from(*self == Self::Bytes));
        Ok(())
    }
    fn read(
        input: &mut Decoder<'_>,
        limits: DecodeLimits,
        steps: &mut Steps<'_>,
    ) -> Result<Self, FsError> {
        Ok(if bool::read(input, limits, steps)? {
            Self::Bytes
        } else {
            Self::Extents
        })
    }
}
macro_rules! fields {
    ($ty:ty { $($name:ident : $field:ty),+ $(,)? }) => {
        impl Field for $ty {
            fn length(&self) -> Result<usize, FsError> { let mut n = 0; $(n = add(n, self.$name.length()?)?;)+ Ok(n) }
            fn write(&self, out: &mut Encoder) -> Result<(), FsError> { $(self.$name.write(out)?;)+ Ok(()) }
            fn read(input: &mut Decoder<'_>, limits: DecodeLimits, steps: &mut Steps<'_>) -> Result<Self, FsError> {
                Ok(Self { $($name: <$field>::read(input, limits, steps)?),+ })
            }
            fn nested(&self) -> Result<usize, FsError> { let mut n = 0; $(n = add(n, self.$name.nested()?)?;)+ Ok(n) }
        }
    };
}
fields!(PendingFile { conflict: usize, values: [Option<FileRecord>; 3], directory_scalars: bool });
fields!(PendingBinding { conflict: usize, values: [Option<(FileId, crate::kernel::FileKind)>; 3] });
fields!(FileFold {
    directory_id: FileId,
    name: LogicalName,
    ours: FileId,
    theirs: FileId
});
fields!(MergeCheckpointContext {
    base_generation: ObjectId, ours_generation: Option<ObjectId>, ours_table: ObjectId,
    theirs_generation: ObjectId, retain_theirs_parent: bool, volume_id: VolumeId,
    root_file_id: FileId, required_features: u64,
});

fn values_length<T: Field>(
    values: &[T],
    observe: &mut dyn FnMut(EncodeWorkStep) -> Result<(), FsError>,
) -> Result<usize, FsError> {
    values.iter().try_fold(4, |length, value| {
        let work = WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        observe(EncodeWorkStep::Check)?;
        observe(EncodeWorkStep::Admit(work))?;
        let result = add(length, value.length()?);
        observe(EncodeWorkStep::Completed(work))?;
        result
    })
}
fn write_values<T: Field>(values: &[T], out: &mut Encoder) -> Result<(), FsError> {
    out.u32(u32::try_from(values.len()).map_err(|_| WorkError::Overflow)?);
    for value in values {
        value.write(out)?;
    }
    Ok(())
}
fn read_values<T: Field>(
    input: &mut Decoder<'_>,
    limits: DecodeLimits,
    maximum_count: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<T>>, FsError> {
    let ((values, bytes), allocation) =
        crate::kernel::file_read::decode_owned(accounting, |steps| {
            let count = usize::try_from(input.u32(&mut |s| steps(s))?).map_err(|_| invalid())?;
            if count > maximum_count {
                return Err(invalid());
            }
            let mut values = Vec::new();
            crate::kernel::file_read::reserve_decoded_items(&mut values, count, &mut |s| steps(s))?;
            for _ in 0..count {
                steps(ReadAllocationStep::BeforeItem)?;
                let value = T::read(input, limits, steps)?;
                let copied = usize_to_u64(std::mem::size_of::<T>());
                steps(ReadAllocationStep::BeforeCopy(copied))?;
                values.push(value);
                steps(ReadAllocationStep::Copied(copied))?;
                steps(ReadAllocationStep::Item)?;
            }
            let bytes = values.iter().try_fold(
                values
                    .capacity()
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or(WorkError::Overflow)?,
                |n, v| add(n, v.nested()?),
            )?;
            Ok((values, usize_to_u64(bytes)))
        })?;
    Ok(Owned::new(values, allocation, bytes))
}

macro_rules! state_vectors {
    ($action:ident, $state:expr, $($args:expr),+) => {
        $action!($state, [$($args),+], records, conflicts, folds, file_folds, replacements,
            pending_files, pending_bindings, pending_rebindings)
    };
}
macro_rules! measure_vectors {
    ($state:expr, [$length:expr, $observe:expr], $($field:ident),+) => {
        $( $length = add($length, values_length(&$state.$field.value, $observe)?)?; )+
    };
}
macro_rules! write_vectors {
    ($state:expr, [$out:expr], $($field:ident),+) => { $(write_values(&$state.$field.value, $out)?;)+ };
}
macro_rules! read_vectors {
    ($state:expr, [$input:expr, $limits:expr, $maximum:expr, $accounting:expr], $($field:ident),+) => {
        $( $state.$field = read_values($input, $limits, $maximum, $accounting)?; )+
    };
}
impl GenerationMergePreparation {
    pub(crate) fn encode_program(
        &self,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
        crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
            let state = &self.tables.state;
            let mut length = add(DOMAIN.len() + 2, self.checkpoint.length()?)?;
            length = add(length, self.tables.original.length()? + 4 + 4 + 1 + 1)?;
            state_vectors!(measure_vectors, state, length, observe);
            if usize_to_u64(length) > limits.maximum_object_bytes {
                return Err(invalid());
            }
            let mut out = Encoder::with_exact_capacity(
                DOMAIN,
                1,
                length,
                |step| observe(EncodeWorkStep::Allocation(step)),
                allocation_error,
            )?;
            let body = WorkCounters {
                bytes_encoded: usize_to_u64(length - DOMAIN.len() - 2),
                bytes_copied: usize_to_u64(length - DOMAIN.len() - 2),
                ..WorkCounters::UNCHARGED
            };
            observe(EncodeWorkStep::Admit(body))?;
            self.checkpoint.write(&mut out)?;
            self.tables.original.write(&mut out)?;
            self.tables.maximum_changes.write(&mut out)?;
            state.remaining_changes.write(&mut out)?;
            state.truncated.write(&mut out)?;
            state.content.write(&mut out)?;
            state_vectors!(write_vectors, state, &mut out);
            observe(EncodeWorkStep::Completed(body))?;
            Ok(out.finish())
        })
    }

    pub(crate) fn decode_program(
        bytes: &[u8],
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let (
            (
                mut input,
                checkpoint,
                original,
                maximum_changes,
                remaining_changes,
                truncated,
                content,
            ),
            allocation,
        ) = crate::kernel::file_read::decode_owned(accounting, |steps| {
            let mut input =
                Decoder::new(bytes, DOMAIN, 1, limits.maximum_object_bytes, &mut |s| {
                    steps(s)
                })?;
            let checkpoint = MergeCheckpointContext::read(&mut input, limits, steps)?;
            let original = ObjectId::read(&mut input, limits, steps)?;
            let maximum_changes = u32::read(&mut input, limits, steps)?;
            let remaining_changes = u32::read(&mut input, limits, steps)?;
            let truncated = bool::read(&mut input, limits, steps)?;
            let content = ContentMerge::read(&mut input, limits, steps)?;
            if maximum_changes == 0
                || remaining_changes > maximum_changes
                || original != checkpoint.ours_table
            {
                return Err(invalid());
            }
            Ok((
                input,
                checkpoint,
                original,
                maximum_changes,
                remaining_changes,
                truncated,
                content,
            ))
        })?;
        retire_allocation(allocation, 0, accounting)?;
        let mut state = MergeState::new(maximum_changes, accounting)?;
        let result = (|| {
            state_vectors!(
                read_vectors,
                state,
                &mut input,
                limits,
                bytes.len(),
                accounting
            );
            input.finish()?;
            if state
                .pending_files
                .value
                .iter()
                .any(|v| v.conflict >= state.conflicts.value.len())
                || state
                    .pending_bindings
                    .value
                    .iter()
                    .any(|v| v.conflict >= state.conflicts.value.len())
            {
                return Err(invalid());
            }
            Ok(())
        })();
        if let Err(error) = result {
            state.retire(accounting)?;
            return Err(error);
        }
        state.remaining_changes = remaining_changes;
        state.truncated = truncated;
        state.content = content;
        Ok(Self {
            tables: FileTablePreparation {
                original,
                maximum_changes,
                state,
            },
            checkpoint,
        })
    }
}
