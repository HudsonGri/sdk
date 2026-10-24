use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

pub(super) fn sample(keys: usize, attributes: usize) -> Result<Record, crate::StreamError> {
    Ok(Record {
        sequence: 17,
        committed_at_micros: 73_000,
        commit_id: crate::CommitId::from_bytes([37; 32]),
        value: OwnedBytes::from_static(b"body"),
        headers: RecordHeaders::new(
            (0..keys)
                .map(|key| key.to_be_bytes().to_vec().into())
                .collect(),
            (0..attributes)
                .map(|number| Attribute {
                    name: format!("attribute-{number:03}"),
                    value: OwnedBytes::from_static(b"value"),
                })
                .collect(),
        )?,
    })
}

#[test]
fn record_facts_roundtrip_all_header_bounds_and_retain_the_original_page_owner()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        for (keys, attributes) in [
            (0, 0),
            (1, 1),
            (crate::MAX_RECORD_KEYS, crate::MAX_RECORD_ATTRIBUTES),
        ] {
            let meter = Arc::new(Meter::local());
            let work = native
                .then(|| crate::memory::Work::new(meter.original()))
                .transpose()?;
            let mut disk = Disk::new(work)?;
            let expected = sample(keys, attributes)?;
            let reference = append(&mut disk, &expected)?;
            disk.reads = 0;
            let actual = read(&mut disk, reference)?;
            assert_eq!(actual, expected);
            assert_eq!(disk.reads, 1);
            drop(disk);
            if native {
                assert!(meter.held.load(Ordering::SeqCst) >= u64::from(reference.bytes));
            }
            drop(actual);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
        }
    }
    Ok(())
}

#[test]
fn record_facts_reject_reauthenticated_malformed_layouts_and_noncanonical_headers()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        let meter = Arc::new(Meter::local());
        let work = native
            .then(|| crate::memory::Work::new(meter.original()))
            .transpose()?;
        let mut disk = Disk::new(work)?;
        let record = sample(2, 1)?;
        let reference = append(&mut disk, &record)?;
        let original = disk.read(reference)?.body()?.to_vec();
        let retained = meter.held.load(Ordering::SeqCst);
        for mutation in 0..6 {
            let mut bytes = original.clone();
            match mutation {
                0 => *bytes.first_mut().ok_or("missing tag")? = 255,
                1 => {
                    bytes.pop();
                }
                2 => bytes.push(0),
                3 => bytes.get_mut(1..9).ok_or("missing sequence")?.fill(255),
                4 => {
                    let keys = 1 + 8 + 8 + 32 + 4 + record.value.len();
                    bytes
                        .get_mut(keys..keys + 2)
                        .ok_or("missing count")?
                        .fill(255);
                }
                _ => {
                    // Repeated equal keys have valid lengths and checksums.
                    let keys = 1 + 8 + 8 + 32 + 4 + record.value.len() + 2;
                    let key_width = std::mem::size_of::<usize>();
                    bytes
                        .get_mut(keys + 4 + key_width + 4..keys + 4 + key_width + 4 + key_width)
                        .ok_or("missing second key")?
                        .fill(0);
                }
            }
            let forged = disk.append(PageKind::Fact, &bytes)?;
            assert!(read(&mut disk, forged).is_err(), "mutation {mutation}");
            assert_eq!(meter.held.load(Ordering::SeqCst), retained);
        }
        drop(disk);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}

mod properties {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn arbitrary_values_and_header_shapes_have_one_canonical_encoding(
            value in prop::collection::vec(any::<u8>(), 0..512),
            keys in 0..=crate::MAX_RECORD_KEYS,
            attributes in 0..=crate::MAX_RECORD_ATTRIBUTES,
        ) {
            let mut disk = Disk::new(None)?;
            let mut expected = sample(keys, attributes)?;
            expected.value = value.into();
            let reference = append(&mut disk, &expected)?;
            let actual = read(&mut disk, reference)?;
            prop_assert_eq!(&actual, &expected);
            let encoded = append(&mut disk, &actual)?;
            let original = disk.read(reference)?;
            let repeated = disk.read(encoded)?;
            prop_assert_eq!(original.body()?, repeated.body()?);
        }
    }
}
