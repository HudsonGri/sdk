use super::stdin::{Stdin, from_file};
use acyclic_native_runtime::terminal::pipe_bytes_available;
use futures::StreamExt;
use std::fs::File;
use std::io::{self, Write};
use std::os::windows::fs::OpenOptionsExt as _;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio_util::codec::{FramedRead, LinesCodec};

/// FILE_FLAG_OVERLAPPED.
const OVERLAPPED: u32 = 0x4000_0000;

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

fn pipe() -> io::Result<(File, File)> {
    let (read, write) = io::pipe()?;
    Ok((
        File::from(std::os::windows::io::OwnedHandle::from(read)),
        File::from(std::os::windows::io::OwnedHandle::from(write)),
    ))
}

async fn lines(input: Stdin) -> io::Result<Vec<String>> {
    FramedRead::new(input, LinesCodec::new())
        .map(|line| line.map_err(io::Error::other))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
}

#[test]
fn anonymous_pipes_are_read_by_the_dedicated_reader_until_the_writer_closes() -> io::Result<()> {
    let (read, mut write) = pipe()?;
    runtime()?.block_on(async {
        let input = from_file(read)?;
        assert!(matches!(input, Stdin::Reader(_)));
        write.write_all(b"one\ntwo\n")?;
        drop(write);
        assert_eq!(lines(input).await?, ["one", "two"]);
        Ok(())
    })
}

#[test]
fn disk_files_are_read_directly() -> io::Result<()> {
    let mut file = tempfile::tempfile()?;
    file.write_all(b"alpha\nbeta\n")?;
    io::Seek::rewind(&mut file)?;
    runtime()?.block_on(async {
        let input = from_file(file)?;
        assert!(matches!(input, Stdin::Ready(_)));
        assert_eq!(lines(input).await?, ["alpha", "beta"]);
        Ok(())
    })
}

#[test]
fn overlapped_pipes_complete_on_the_runtime_without_a_thread() -> io::Result<()> {
    let name = format!(r"\\.\pipe\acyclic-harness-stdin-{}", std::process::id());
    runtime()?.block_on(async {
        let server = tokio::net::windows::named_pipe::ServerOptions::new()
            .access_inbound(false)
            .create(&name)?;
        let client = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(OVERLAPPED)
            .open(&name)?;
        server.connect().await?;
        let input = from_file(client)?;
        assert!(matches!(input, Stdin::Overlapped(_)));
        let mut server = server;
        tokio::io::AsyncWriteExt::write_all(&mut server, b"a\nb\n").await?;
        drop(server);
        assert_eq!(lines(input).await?, ["a", "b"]);
        Ok(())
    })
}

/// Waits, bounded, until the pipe's queued byte count reaches `expected`.
fn settles_at(pipe: &File, expected: u32) -> io::Result<bool> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if pipe_bytes_available(pipe)? == expected {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::yield_now();
    }
}

#[test]
fn dropping_a_waiting_reader_leaves_no_read_to_consume_later_input() -> io::Result<()> {
    let (read, mut write) = pipe()?;
    let observer = read.try_clone()?;
    runtime()?.block_on(async {
        let mut input = from_file(read)?;
        // The reader thread is now blocked in its read.
        let waited = tokio::time::timeout(Duration::from_millis(5), input.read(&mut [0; 1])).await;
        assert!(waited.is_err());
        drop(input);
        io::Result::Ok(())
    })?;
    write.write_all(b"x")?;
    // Nothing read the byte: it stays queued for the next owner of stdin.
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(pipe_bytes_available(&observer)?, 1);
    Ok(())
}

#[test]
fn negative_control_detects_a_blocking_pool_read_left_behind() -> io::Result<()> {
    // tokio's stdin reads on the blocking pool, as tokio's File does: a
    // dropped read stays in flight and takes the next input.
    let (read, mut write) = pipe()?;
    let observer = read.try_clone()?;
    let runtime = runtime()?;
    runtime.block_on(async {
        let mut input = tokio::fs::File::from_std(read);
        let waited = tokio::time::timeout(Duration::from_millis(5), input.read(&mut [0; 1])).await;
        assert!(waited.is_err());
        drop(input);
    });
    write.write_all(b"x")?;
    assert!(
        settles_at(&observer, 0)?,
        "the orphaned read never took the byte"
    );
    runtime.shutdown_background();
    Ok(())
}
