use super::*;
use acyclic_native_runtime::ReadAllocation;

struct Controls {
    keys: Vec<OwnedBytes>,
    attributes: Vec<Attribute>,
    allocation: Option<ReadAllocation>,
}

fn vector<T>(
    count: usize,
    work: Option<&Arc<crate::memory::Work>>,
    allocation: &mut Option<ReadAllocation>,
) -> Result<Vec<T>, LocalStreamError> {
    if let (Some(work), Some(allocation)) = (work, allocation) {
        return page::observe(
            Some(work),
            crate::native::vector(count, &work.meter, allocation),
        );
    }
    let mut value = Vec::new();
    value
        .try_reserve_exact(count)
        .map_err(|_| LocalStreamError::InvalidLimits)?;
    Ok(value)
}

pub(super) fn headers(
    bytes: &OwnedBytes,
    layout: &Layout,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<RecordHeaders, LocalStreamError> {
    let mut controls = Controls {
        keys: Vec::new(),
        attributes: Vec::new(),
        allocation: work.map(|work| ReadAllocation::empty(&work.meter)),
    };
    controls.keys = vector(layout.keys()?.len(), work, &mut controls.allocation)?;
    controls.attributes = vector(layout.attributes()?.len(), work, &mut controls.allocation)?;
    for span in layout.keys()? {
        controls.keys.push(span.slice(bytes));
    }
    for (name, value) in layout.attributes()? {
        let name = name.text(bytes)?;
        let mut copied = vector(name.len(), work, &mut controls.allocation)?;
        page::codec(work, name.len() as u64, 0, true)?;
        copied.extend_from_slice(name.as_bytes());
        page::codec(work, name.len() as u64, 0, false)?;
        controls.attributes.push(Attribute {
            name: String::from_utf8(copied).map_err(|_| LocalStreamError::Corrupt)?,
            value: value.slice(bytes),
        });
    }
    match (work, controls.allocation.take()) {
        (Some(work), Some(allocation)) => page::observe(
            Some(work),
            RecordHeaders::from_native_parts(
                controls.keys,
                controls.attributes,
                allocation,
                &work.meter,
            ),
        )?
        .map_err(|_| LocalStreamError::Corrupt),
        (None, None) => RecordHeaders::new(controls.keys, controls.attributes)
            .map_err(|_| LocalStreamError::Corrupt),
        _ => Err(LocalStreamError::Corrupt),
    }
}
