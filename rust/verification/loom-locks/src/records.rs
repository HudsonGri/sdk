//! Counted, byte-length-delimited UTF-8 records. Source bytes have no sentinel.
use std::io::{Error, ErrorKind, Result};

pub fn decode(mut input: &[u8]) -> Result<Vec<&str>> {
    fn size(input: &mut &[u8]) -> Result<usize> {
        let bytes = input.get(..4).ok_or_else(invalid)?;
        let value = u32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
        *input = &input[4..];
        usize::try_from(value).map_err(|_| invalid())
    }
    let count = size(&mut input)?;
    if count > input.len() / 4 {
        return Err(invalid());
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let length = size(&mut input)?;
        let bytes = input.get(..length).ok_or_else(invalid)?;
        records.push(std::str::from_utf8(bytes).map_err(|_| invalid())?);
        input = &input[length..];
    }
    if !input.is_empty() {
        return Err(invalid());
    }
    Ok(records)
}

fn invalid() -> Error {
    Error::new(ErrorKind::InvalidData, "invalid scanner source records")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn encode(records: &[String]) -> Vec<u8> {
        let mut bytes = (records.len() as u32).to_be_bytes().to_vec();
        for record in records {
            bytes.extend((record.len() as u32).to_be_bytes());
            bytes.extend(record.as_bytes());
        }
        bytes
    }

    proptest! {
        #[test]
        fn arbitrary_source_bytes_round_trip(records in prop::collection::vec(any::<String>(), 0..16)) {
            let bytes = encode(&records);
            prop_assert_eq!(decode(&bytes).ok(), Some(records.iter().map(String::as_str).collect()));
            for end in 0..bytes.len() {
                prop_assert!(decode(&bytes[..end]).is_err());
            }
            let mut extra = bytes;
            extra.push(0);
            prop_assert!(decode(&extra).is_err());
        }
    }

    #[test]
    fn nul_is_source_and_invalid_utf8_is_rejected() {
        assert_eq!(
            decode(&encode(&["/*\0*/ fn f() {}".into()])).ok(),
            Some(vec!["/*\0*/ fn f() {}"])
        );
        assert!(decode(&[0, 0, 0, 1, 0, 0, 0, 1, 255]).is_err());
        assert!(decode(&u32::MAX.to_be_bytes()).is_err());
    }
}
