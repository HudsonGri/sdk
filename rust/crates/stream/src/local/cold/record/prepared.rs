//! Decode the admitted page with controls funded before the physical read.
use super::*;
use crate::record_headers::PreparedHeaderControls;

pub(in crate::local::cold) struct PreparedRecord {
    headers: PreparedHeaderControls,
}

impl PreparedRecord {
    pub(in crate::local::cold) fn new(
        work: &Arc<crate::memory::Work>,
    ) -> Result<Self, LocalStreamError> {
        let headers = page::observe(
            Some(work),
            PreparedHeaderControls::new(&work.meter, crate::MAX_RECORD_BYTES),
        )?;
        Ok(Self { headers })
    }

    pub(in crate::local::cold) fn read(
        self,
        plan: &mut page::PreparedRead,
        file: &acyclic_native_runtime::PreparedFileAccess<'_>,
        reference: PageRef,
    ) -> Result<Record, LocalStreamError> {
        let page = plan.read_prepared(file, reference)?;
        if page.kind != PageKind::Fact {
            return Err(LocalStreamError::Corrupt);
        }
        let bytes = page.into_body()?;
        let layout = Layout::read(&bytes)?;
        let mut keys: [std::ops::Range<usize>; crate::MAX_RECORD_KEYS] =
            std::array::from_fn(|_| 0..0);
        let mut attributes: [(std::ops::Range<usize>, std::ops::Range<usize>);
            crate::MAX_RECORD_ATTRIBUTES] = std::array::from_fn(|_| (0..0, 0..0));
        let key_count = layout.keys()?.len();
        let attribute_count = layout.attributes()?.len();
        for (range, span) in keys.iter_mut().zip(layout.keys()?) {
            *range = span.range();
        }
        for ((name, value), (name_span, value_span)) in
            attributes.iter_mut().zip(layout.attributes()?)
        {
            *name = name_span.range();
            *value = value_span.range();
        }
        let headers = self.headers.finish(
            bytes.clone(),
            keys.get(..key_count).ok_or(LocalStreamError::Corrupt)?,
            attributes
                .get(..attribute_count)
                .ok_or(LocalStreamError::Corrupt)?,
        )?;
        let record = Record {
            sequence: layout.sequence,
            committed_at_micros: layout.time,
            commit_id: layout.commit,
            value: layout.value.slice(&bytes),
            headers,
        };
        crate::changes::validate(&record.value, &record.headers)
            .map_err(|_| LocalStreamError::Corrupt)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests;
