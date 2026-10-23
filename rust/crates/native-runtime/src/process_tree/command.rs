//! Owned process-tree configuration. Keep streams and the environment's
//! inheritance policy explicit, so a warden can receive the target's state
//! without running its loader or startup under the target's environment.

use super::ProcessTree;
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};

/// One spawn's program, arguments, environment, directory and owned streams.
/// Configuration never installs executable callbacks in a forked host.
pub struct ProcessTreeCommand {
    command: Command,
    #[cfg(unix)]
    clear_environment: bool,
    streams: [Option<Stdio>; 3],
}

impl ProcessTreeCommand {
    /// Starts configuring `program`.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            command: Command::new(program),
            #[cfg(unix)]
            clear_environment: false,
            streams: [None, None, None],
        }
    }

    /// Read-only command metadata, also used by seeded process fixtures.
    pub fn as_command(&self) -> &Command {
        &self.command
    }

    /// Adds one program argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.command.arg(arg);
        self
    }

    /// Adds program arguments.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command.args(args);
        self
    }

    /// Sets the program's working directory.
    pub fn current_dir(&mut self, directory: impl AsRef<Path>) -> &mut Self {
        self.command.current_dir(directory);
        self
    }

    /// Sets a program environment variable.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.command.env(key, value);
        self
    }

    /// Sets program environment variables.
    pub fn envs<I, K, V>(&mut self, values: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.command.envs(values);
        self
    }

    /// Removes a program environment variable.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.command.env_remove(key);
        self
    }

    /// Starts with an empty program environment.
    pub fn env_clear(&mut self) -> &mut Self {
        self.command.env_clear();
        #[cfg(unix)]
        {
            self.clear_environment = true;
        }
        self
    }

    /// Supplies owned standard input.
    pub fn stdin(&mut self, stream: impl Into<Stdio>) -> &mut Self {
        self.streams[0] = Some(stream.into());
        self
    }

    /// Supplies owned standard output.
    pub fn stdout(&mut self, stream: impl Into<Stdio>) -> &mut Self {
        self.streams[1] = Some(stream.into());
        self
    }

    /// Supplies owned standard error.
    pub fn stderr(&mut self, stream: impl Into<Stdio>) -> &mut Self {
        self.streams[2] = Some(stream.into());
        self
    }

    /// Starts this configuration in an owned tree without durable custody.
    pub fn spawn_tree(mut self) -> io::Result<ProcessTree> {
        self.prepare_streams();
        ProcessTree::spawn(&mut self.command)
    }

    pub(super) fn prepare_streams(&mut self) {
        Self::apply_streams(&mut self.command, &mut self.streams);
    }

    #[cfg(windows)]
    pub(super) fn command_mut(&mut self) -> &mut Command {
        &mut self.command
    }

    fn apply_streams(command: &mut Command, streams: &mut [Option<Stdio>; 3]) {
        if let Some(stream) = streams[0].take() {
            command.stdin(stream);
        }
        if let Some(stream) = streams[1].take() {
            command.stdout(stream);
        }
        if let Some(stream) = streams[2].take() {
            command.stderr(stream);
        }
    }

    #[cfg(unix)]
    pub(super) fn configure_warden(&mut self, warden: &mut Command) {
        warden.args(self.command.get_args());
        Self::apply_streams(warden, &mut self.streams);
    }

    #[cfg(unix)]
    pub(super) fn environment(&self) -> io::Result<Vec<u8>> {
        use std::collections::BTreeMap;
        use std::os::unix::ffi::OsStrExt as _;
        let mut environment = if self.clear_environment {
            BTreeMap::new()
        } else {
            std::env::vars_os().collect::<BTreeMap<_, _>>()
        };
        for (key, value) in self.command.get_envs() {
            match value {
                Some(value) => {
                    environment.insert(key.to_owned(), value.to_owned());
                }
                None => {
                    environment.remove(key);
                }
            }
        }
        let mut bytes = Vec::new();
        for (key, value) in environment {
            if key.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            bytes.extend_from_slice(key.as_bytes());
            bytes.push(b'=');
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0);
        }
        Ok(bytes)
    }
}
