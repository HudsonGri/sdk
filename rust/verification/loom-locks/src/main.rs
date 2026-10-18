use loom_lock_scan::{bounds, entries, records, scan};
use std::{
    io::{Read, Write},
    process::ExitCode,
};

fn main() -> ExitCode {
    let mut input = Vec::new();
    if std::io::stdin().read_to_end(&mut input).is_err() {
        return ExitCode::FAILURE;
    }
    let Ok(sources) = records::decode(&input) else {
        return ExitCode::FAILURE;
    };
    let mut output = String::new();
    for (index, source) in sources.into_iter().enumerate() {
        let verdict = match scan(source) {
            Ok(found) => u8::from(found).to_string(),
            Err(error) => format!("!{}", error.to_string().replace(['\n', '\r', '\t'], " ")),
        };
        let bound = match bounds::bounded(source) {
            Ok(value) => u8::from(value).to_string(),
            Err(_) => "!invalid Rust tokens".to_string(),
        };
        let entry = match entries::present(source) {
            Ok(value) => u8::from(value).to_string(),
            Err(_) => "!invalid Rust tokens".to_string(),
        };
        output.push_str(&format!("{index}\t{verdict}\t{bound}\t{entry}\n"));
    }
    match std::io::stdout().write_all(output.as_bytes()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
