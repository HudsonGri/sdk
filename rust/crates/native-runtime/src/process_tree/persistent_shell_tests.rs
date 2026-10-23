//! Measure whether disposing a fresh runspace fences work in a retained pwsh.
use super::*;
use std::io::{BufRead as _, Write as _};
use std::process::Stdio;

const RUNSPACE_PROBE: &str = r#"
$ErrorActionPreference = 'Stop'
$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
$listener.Start()
$producer = [Net.Sockets.TcpClient]::new('127.0.0.1', $listener.LocalEndpoint.Port)
$consumer = $listener.AcceptTcpClient()
$listener.Stop()
$source = $producer.GetStream()
$inputPipe = $consumer.GetStream()
$sink = [IO.File]::Create($env:ACYCLIC_RUNSPACE_PROBE_FILE)
$runspace = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspace()
$runspace.Open()
$runspace.SessionStateProxy.SetVariable('source', $inputPipe)
$runspace.SessionStateProxy.SetVariable('sink', $sink)
$pipeline = [PowerShell]::Create()
$pipeline.Runspace = $runspace
[void]$pipeline.AddScript('$source.CopyToAsync($sink)')
$work = $pipeline.Invoke()[0]
if ($pipeline.HadErrors) { throw $pipeline.Streams.Error[0] }
$pipeline.Dispose()
$runspace.Dispose()
[Console]::WriteLine('runspace-disposed')
[Console]::Out.Flush()
$null = [Console]::ReadLine()
$bytes = [byte[]]::new(131072)
$source.Write($bytes, 0, $bytes.Length)
$source.Dispose()
$null = $work.GetAwaiter().GetResult()
$sink.Flush()
$sink.Dispose()
[Console]::WriteLine('background-write-completed')
[Console]::Out.Flush()
$null = [Console]::ReadLine()
"#;

#[test]
fn fresh_runspace_disposal_does_not_fence_background_dotnet_io() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("after-command.bin");
    let mut command = Command::new("pwsh.exe");
    command
        .args(["-NoProfile", "-NonInteractive", "-Command", RUNSPACE_PROBE])
        .env("ACYCLIC_RUNSPACE_PROBE_FILE", &file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let mut input = tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
    let output = tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?;
    let (sent, received) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines() {
            if sent.send(line).is_err() {
                break;
            }
        }
    });
    let first = received
        .recv_timeout(Duration::from_secs(3))
        .map_err(io::Error::other)??;
    assert_eq!(first, "runspace-disposed");
    assert_eq!(std::fs::metadata(&file)?.len(), 0);
    input.write_all(b"continue\n")?;
    input.flush()?;
    let second = received
        .recv_timeout(Duration::from_secs(3))
        .map_err(io::Error::other)??;
    assert_eq!(second, "background-write-completed");
    let bytes = std::fs::metadata(&file)?.len();
    drop(input);
    tree.terminate(None)?;
    reader
        .join()
        .map_err(|_| io::Error::other("probe output reader panicked"))?;
    eprintln!("PWSH_RUNSPACE_BACKGROUND bytes_after_disposal={bytes}");
    assert_eq!(bytes, 131072);
    Ok(())
}
