//! The admitted prefix is immutable. Callers can change only the new suffix;
//! its tool exchanges and instruction budget are checked before publication.
use super::*;

pub(crate) struct RequestContinuation {
    request: ModelRequest,
    limits: Limits,
    instruction_remaining: u64,
    suffix: Vec<ModelMessage>,
}

impl PreparedModelRequest {
    pub(crate) fn into_continuation(self) -> RequestContinuation {
        let limits = self.admitted_limits;
        let instruction_remaining = self.instruction_remaining;
        RequestContinuation {
            request: self.into_request(),
            limits,
            instruction_remaining,
            suffix: Vec::new(),
        }
    }
}

impl RequestContinuation {
    pub(crate) fn messages(&mut self) -> &mut Vec<ModelMessage> {
        &mut self.suffix
    }

    pub(crate) fn tools(&self) -> &[crate::tool::ToolDefinition] {
        &self.request.tools
    }

    fn validate(
        mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<(ModelRequest, u64)> {
        let count = self
            .request
            .messages
            .len()
            .checked_add(self.suffix.len())
            .ok_or_else(|| Error::Invalid("model context count overflows".into()))?;
        if count == 0 || count > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        let remaining = if limits == self.limits {
            with_history_io(io, |work| limits.validate(work))?;
            let mut remaining = self.instruction_remaining;
            reserve_instruction_bodies(&self.suffix, &mut remaining, io)?;
            ModelRequest::validate_messages(&mut self.suffix, &self.request.tools, limits, io)?;
            remaining
        } else {
            // Changed bounds cannot reuse an admission under different limits.
            for message in self.suffix {
                crate::executor::stock_vec_push(&mut self.request.messages, message, io)?;
            }
            let remaining = self.request.validate_with_instruction_budget(limits, io)?;
            return Ok((self.request, remaining));
        };
        for message in self.suffix {
            crate::executor::stock_vec_push(&mut self.request.messages, message, io)?;
        }
        Ok((self.request, remaining))
    }

    pub(crate) fn finish(
        self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<PreparedModelRequest> {
        PreparedModelRequest::begin_preparation(io)?;
        let (request, remaining) = self.validate(limits, io)?;
        PreparedModelRequest::encode_validated(request, limits, remaining, io)
    }
}

#[cfg(test)]
mod tests;
