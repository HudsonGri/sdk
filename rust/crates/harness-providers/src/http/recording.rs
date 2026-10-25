//! Opt-in diagnostic capture at the sole native model transport.
use super::*;
use acyclic_harness_core::{
    Error,
    http::recording::{BodyHash, Identity, Provenance, ResponseEnd, sha256, sha256_reader},
};
use std::{
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

pub(super) struct Recorder {
    directory: PathBuf,
    binary_sha256: String,
    source_commit: &'static str,
}
impl Recorder {
    pub(super) fn new(directory: impl Into<PathBuf>) -> Result<Self> {
        let source_commit = env!("ACYCLIC_PROVIDER_SOURCE_COMMIT");
        if source_commit.is_empty() {
            return Err(crate::invalid(
                "STEP configure provider recording failed: binary has no source commit; recovery: ACYCLIC_PROVIDER_SOURCE_COMMIT=$(git rev-parse HEAD) cargo build --locked -p graphcoder",
            ));
        }
        let directory = directory.into();
        std::fs::create_dir_all(&directory)
            .map_err(|e| failure("create capture root", &directory, e))?;
        static BINARY: std::sync::OnceLock<std::io::Result<String>> = std::sync::OnceLock::new();
        let binary_sha256 = BINARY
            .get_or_init(|| {
                std::env::current_exe()
                    .and_then(File::open)
                    .and_then(sha256_reader)
            })
            .as_ref()
            .map_err(|e| failure("hash running binary", &directory, e))?
            .clone();
        Ok(Self {
            directory,
            binary_sha256,
            source_commit,
        })
    }
    // Header *values* are deliberately absent from this API.
    pub(super) fn begin<'a>(
        &self,
        identity: &Identity,
        url: &str,
        header_names: impl Iterator<Item = &'a String> + Clone,
        body: &[u8],
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Capture> {
        let metadata = self.directory.as_os_str().len()
            + url.len()
            + identity.adapter.len()
            + identity.provider.len()
            + identity.model.len()
            + header_names.clone().map(String::len).sum::<usize>();
        let custody = work
            .map(|work| {
                crate::native::reserve(
                    work,
                    metadata as u64,
                    header_names.clone().count() as u64 + 16,
                    0,
                )
            })
            .transpose()?;
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                bytes_hashed: body.len() as u64,
                output_bytes: body.len() as u64,
                backend_write_operations: 3,
                materializations: 3,
                ..Default::default()
            })?;
        }
        let directory = tempfile::Builder::new()
            .prefix("exchange-")
            .tempdir_in(&self.directory)
            .map_err(|e| failure("allocate exclusive exchange", &self.directory, e))?
            .keep();
        let path = url::Url::parse(url)
            .map_err(|_| {
                crate::invalid(
                    "STEP parse recording path failed; recovery: configure a valid provider URL",
                )
            })?
            .path()
            .to_owned();
        let mut redacted_headers: Vec<_> = header_names.map(|n| n.to_ascii_lowercase()).collect();
        redacted_headers.push("content-type".into());
        redacted_headers.sort();
        redacted_headers.dedup();
        let provenance = Provenance {
            version: 1,
            timestamp_unix_nanos: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| failure("read capture clock", &directory, e))?
                .as_nanos(),
            identity: identity.clone(),
            binary_sha256: self.binary_sha256.clone(),
            source_commit: self.source_commit.into(),
            method: "POST".into(),
            path,
            redacted_headers,
            request_sha256: sha256(body),
            status: None,
            content_type: None,
            response_sha256: sha256(&[]),
            chunks: Vec::new(),
            end: None,
        };
        std::fs::write(directory.join("request.body"), body)
            .map_err(|e| failure("write request body", &directory, e))?;
        let response = File::create_new(directory.join("response.body"))
            .map_err(|e| failure("create response body", &directory, e))?;
        let capture = Capture {
            directory,
            provenance,
            response,
            digest: BodyHash::default(),
            settled: false,
            custody,
        };
        capture.publish()?;
        Ok(capture)
    }
}
fn failure(step: &str, directory: &Path, error: impl std::fmt::Display) -> Error {
    Error::Storage(format!(
        "STEP provider recorder {step} failed at {}: {error}; recovery: choose a writable ACYCLIC_PROVIDER_RECORD directory and recapture the exchange",
        directory.display()
    ))
}

pub(super) struct Capture {
    directory: PathBuf,
    provenance: Provenance,
    response: File,
    digest: BodyHash,
    settled: bool,
    custody: Option<acyclic_fs::NativeAllocation>,
}
impl Capture {
    fn publish(&self) -> Result<()> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
            .map_err(|e| failure("create provenance", &self.directory, e))?;
        if let Some(custody) = &self.custody {
            let io =
                acyclic_harness_core::conversation::ContentIo::from_native_work(custody.work())?;
            let bytes = io.encode(&self.provenance, io.remaining()?.bytes_encoded, false)?;
            custody.work().charge(acyclic_fs::WorkCounters {
                output_bytes: bytes.len() as u64,
                backend_write_operations: 1,
                ..Default::default()
            })?;
            temporary
                .write_all(&bytes)
                .map_err(|e| failure("write provenance", &self.directory, e))?;
        } else {
            serde_json::to_writer(&mut temporary, &self.provenance)
                .map_err(|e| failure("encode provenance", &self.directory, e))?;
        }
        temporary
            .flush()
            .map_err(|e| failure("flush provenance", &self.directory, e))?;
        temporary
            .persist(self.directory.join("provenance.json"))
            .map_err(|e| failure("publish provenance", &self.directory, e))?;
        Ok(())
    }
    pub(super) fn headers<'a, I: Iterator<Item = &'a str>>(
        &mut self,
        status: u16,
        content_type: Option<&str>,
        names: impl Fn() -> I,
    ) -> Result<()> {
        if let Some(custody) = self.custody.as_mut() {
            let mut extra = crate::native::reserve(
                custody.work(),
                (content_type.map_or(0, str::len) + names().map(str::len).sum::<usize>()) as u64,
                names().count() as u64 + 1,
                0,
            )?;
            custody.absorb(&mut extra)?;
        }
        self.provenance
            .redacted_headers
            .extend(names().map(str::to_owned));
        self.provenance.redacted_headers.sort();
        self.provenance.redacted_headers.dedup();
        self.provenance.status = Some(status);
        self.provenance.content_type = content_type.map(str::to_owned);
        self.publish()
    }
    fn chunk(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(custody) = self.custody.as_mut() {
            custody.work().charge(acyclic_fs::WorkCounters {
                bytes_hashed: bytes.len() as u64,
                output_bytes: bytes.len() as u64,
                backend_write_operations: 1,
                items_examined: 1,
                ..Default::default()
            })?;
            if self.provenance.chunks.len() == self.provenance.chunks.capacity() {
                let capacity = self
                    .provenance
                    .chunks
                    .capacity()
                    .checked_mul(2)
                    .unwrap_or(usize::MAX)
                    .max(4);
                let size = capacity.checked_mul(std::mem::size_of::<u64>()).ok_or_else(|| crate::invalid("STEP grow recording chunks failed; recovery: use a smaller provider exchange"))?;
                let mut extra =
                    custody
                        .work()
                        .claim(size as u64, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
                self.provenance
                    .chunks
                    .reserve_exact(capacity - self.provenance.chunks.len());
                custody.absorb(&mut extra)?;
            }
        }
        self.response
            .write_all(bytes)
            .map_err(|e| failure("append response chunk", &self.directory, e))?;
        self.digest.update(bytes);
        self.provenance.chunks.push(bytes.len() as u64);
        Ok(())
    }
    fn finish(&mut self, end: ResponseEnd) -> Result<()> {
        self.settled = true;
        self.provenance.end = Some(end);
        self.provenance.response_sha256 = self.digest.hex();
        self.response
            .flush()
            .map_err(|e| failure("flush response body", &self.directory, e))?;
        self.publish()
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        if !self.settled
            && let Err(error) = self.finish(ResponseEnd::Cancelled)
        {
            eprintln!("{error}");
        }
    }
}
struct RecordedBody {
    source: BoxProviderStream<'static, Result<Bytes>>,
    capture: Capture,
    terminal: bool,
}
impl futures::Stream for RecordedBody {
    type Item = Result<Bytes>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminal {
            return Poll::Ready(None);
        }
        match this.source.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(bytes))) => match this.capture.chunk(&bytes) {
                Ok(()) => Poll::Ready(Some(Ok(bytes))),
                Err(error) => {
                    this.terminal = true;
                    Poll::Ready(Some(Err(error)))
                }
            },
            Poll::Ready(Some(Err(error))) => {
                this.terminal = true;
                let result = this.capture.finish(ResponseEnd::Interrupted);
                Poll::Ready(Some(Err(result.err().unwrap_or(error))))
            }
            Poll::Ready(None) => {
                this.terminal = true;
                Poll::Ready(this.capture.finish(ResponseEnd::Eof).err().map(Err))
            }
        }
    }
}
pub(super) fn body(
    source: BoxProviderStream<'static, Result<Bytes>>,
    capture: Option<Capture>,
) -> Result<BoxProviderStream<'static, Result<Bytes>>> {
    match capture {
        Some(mut capture) => {
            if let Some(custody) = capture.custody.as_mut() {
                let mut frame = custody.work().claim(
                    std::mem::size_of::<RecordedBody>() as u64,
                    1,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                custody.absorb(&mut frame)?;
            }
            Ok(Box::pin(RecordedBody {
                source,
                capture,
                terminal: false,
            }))
        }
        None => Ok(source),
    }
}
#[cfg(test)]
mod tests;
