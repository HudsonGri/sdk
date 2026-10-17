use super::*;
use serde::Serialize;

/// Count encoded bytes without allocating an encoded copy or rescanning prior
/// hooks. Each optional field changes monotonically except replacement input.
pub(super) struct Budget {
    maximum: u64,
    used: u64,
    replacement: u64,
}

impl Budget {
    pub(super) fn new(maximum: u64) -> Result<Self> {
        Ok(Self {
            maximum,
            used: encoded_size(&HookOutput::default(), maximum)?,
            replacement: 4,
        })
    }

    pub(super) fn apply(
        &mut self,
        output: &mut HookOutput,
        event: HookEvent,
        policy: HookFailure,
        result: Result<(Option<i32>, Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let blocked = output.blocked.is_some();
        let approval = output.approval.is_some();
        let contexts = output.additional_context.len();
        let diagnostics = output.diagnostics.len();
        let updated = output.apply(event, policy, result);
        if updated {
            self.used -= self.replacement;
        }
        if !blocked && let Some(value) = &output.blocked {
            self.replace(4, value)?;
        }
        if !approval && let Some(value) = &output.approval {
            self.replace(4, value)?;
        }
        for (index, value) in output.additional_context.iter().enumerate().skip(contexts) {
            self.add(encoded_size(value, self.maximum)? + u64::from(index > 0))?;
        }
        for (index, value) in output.diagnostics.iter().enumerate().skip(diagnostics) {
            self.add(encoded_size(value, self.maximum)? + u64::from(index > 0))?;
        }
        if updated {
            let next = encoded_size(&output.updated_input, self.maximum)?;
            self.add(next)?;
            self.replacement = next;
        }
        Ok(())
    }

    fn replace(&mut self, previous: u64, value: &impl Serialize) -> Result<()> {
        let next = encoded_size(value, self.maximum)?;
        self.used -= previous;
        self.add(next)
    }

    fn add(&mut self, bytes: u64) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|used| *used <= self.maximum)
            .ok_or_else(exceeded)?;
        Ok(())
    }
}

pub(super) fn encoded_size(value: &impl Serialize, maximum: u64) -> Result<u64> {
    struct Count {
        used: u64,
        maximum: u64,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.used = self
                .used
                .checked_add(bytes.len() as u64)
                .filter(|used| *used <= self.maximum)
                .ok_or_else(|| std::io::Error::other("hook output budget exhausted"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { used: 0, maximum };
    serde_json::to_writer(&mut count, value).map_err(|_| exceeded())?;
    Ok(count.used)
}

fn exceeded() -> Error {
    Error::Process {
        kind: "output_limit",
        exit_code: None,
        message: "aggregate hook output limit exceeded".into(),
    }
}

#[cfg(test)]
mod tests;
