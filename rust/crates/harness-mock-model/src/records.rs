//! One buffered append is flushed before the request notification.
use serde_json::Value;
use std::io::{BufWriter, Write};

pub(super) fn append(writer: &mut BufWriter<impl Write>, value: &Value) -> std::io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct Sink {
        bytes: Vec<u8>,
        writes: usize,
        flushed: usize,
    }
    impl Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushed = self.bytes.len();
            Ok(())
        }
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn recorded_json_is_complete_at_flush_without_fragment_writes(text in ".{0,128}", count in 32usize..96) {
            let value = json!({"tools":(0..count).map(|i| json!({"name":format!("tool-{i}"),"description":text,"schema":{"type":"object","properties":{"path":{"type":"string"}}}})).collect::<Vec<_>>()});
            let mut buffered = BufWriter::new(Sink::default());
            append(&mut buffered, &value).unwrap();
            let sink = buffered.get_ref();
            proptest::prop_assert_eq!(sink.flushed, sink.bytes.len());
            proptest::prop_assert_eq!(serde_json::from_slice::<Value>(&sink.bytes).unwrap(), value.clone());
            proptest::prop_assert_eq!(sink.bytes.last(), Some(&b'\n'));
            let bound = 2 + sink.bytes.len() / 4096;
            proptest::prop_assert!(sink.writes <= bound);
            // Negative control is the former production path: formatting a
            // structured request straight into File emits tiny writes.
            let mut raw = Sink::default();
            writeln!(raw, "{value}").unwrap();
            raw.flush().unwrap();
            proptest::prop_assert_eq!(&raw.bytes, &sink.bytes);
            proptest::prop_assert!(raw.writes > bound);
        }
    }
}
