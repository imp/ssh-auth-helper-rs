//! CLI parsing, explicit output and foreground serving.

use clap::Args;
use clap::Parser;
use clap::Subcommand;

use super::*;

#[derive(Debug, Parser)]
#[command(version, about = "Forwarded SSH agent helper (under development)")]
pub struct Cli {
    #[command(flatten)]
    pub flags: Flags,
    #[command(subcommand)]
    pub command: Command,
}

/// Shared command options, captured by Clap or supplied directly by callers.
#[derive(Debug, Default, Args)]
pub struct Flags {
    /// Absolute runtime directory shared by helper commands.
    #[arg(long, global = true)]
    pub runtime_dir: Option<Utf8PathBuf>,
    /// XDG base directory; UTF-8 conversion is deferred until it is selected.
    #[arg(long, global = true, env = "XDG_RUNTIME_DIR", hide = true)]
    pub xdg_runtime_dir: Option<ffi::OsString>,
}

impl Flags {
    /// Resolve runtime paths without reading environment variables or changing files.
    ///
    /// # Errors
    /// Rejects invalid selected paths and socket paths exceeding platform limits.
    pub fn runtime_paths(&self) -> Result<RuntimePaths, RuntimeError> {
        RuntimePaths::discover(self.runtime_dir.as_deref(), self.xdg_runtime_dir.as_deref())
    }
}

impl Cli {
    /// Execute a supported command.
    ///
    /// # Errors
    /// Reports invalid paths, daemon lifecycle failures or failed stdout writes.
    pub fn run(self) -> Result<(), CliError> {
        use io::Write;

        match self.command {
            Command::Serve => {
                let paths = self.flags.runtime_paths()?;
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(CliError::RuntimeBuild)?
                    .block_on(crate::daemon::serve(paths))?;
            }
            Command::SocketPath => {
                let paths = self.flags.runtime_paths()?;
                writeln!(io::stdout().lock(), "{}", paths.agent())?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run in the foreground until SIGINT/SIGTERM (upstream forwarding is pending).
    Serve,
    /// Print the planned agent socket path without creating files or starting a daemon.
    SocketPath,
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Daemon(#[from] crate::daemon::DaemonError),
    #[error("cannot create async runtime: {0}")]
    RuntimeBuild(io::Error),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("cannot write command output: {0}")]
    Output(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::unix::ffi::OsStrExt;

    impl Flags {
        fn runtime(self, runtime: &str) -> Self {
            let runtime_dir = Utf8PathBuf::from(runtime).into();
            Self {
                runtime_dir,
                ..self
            }
        }

        fn xdg(self, xdg: &[u8]) -> Self {
            let xdg_runtime_dir = ffi::OsStr::from_bytes(xdg).to_os_string().into();
            Self {
                xdg_runtime_dir,
                ..self
            }
        }
    }

    #[test]
    fn explicit_directory_overrides_non_utf8_xdg() {
        let flags = Flags::default().runtime("/tmp/helper").xdg(b"/tmp/\xff");
        assert_eq!(
            flags.runtime_paths().unwrap().agent(),
            "/tmp/helper/agent.sock"
        );
    }

    #[test]
    fn xdg_directory_contains_helper_endpoints() {
        let flags = Flags::default().xdg("/run/user/42".as_bytes());
        let paths = flags.runtime_paths().unwrap();
        assert_eq!(paths.directory(), "/run/user/42/ssh-auth-helper");
        assert_eq!(paths.agent(), "/run/user/42/ssh-auth-helper/agent.sock");
        assert_eq!(paths.control(), "/run/user/42/ssh-auth-helper/control.sock");
        assert_eq!(paths.lock(), "/run/user/42/ssh-auth-helper/daemon.lock");
    }

    #[test]
    fn absent_directories_use_current_uid_fallback() {
        let paths = Flags::default().runtime_paths().unwrap();
        let uid = rustix::process::getuid();
        assert_eq!(
            paths.agent(),
            format!("/tmp/ssh-auth-helper-{uid}/agent.sock")
        );
    }

    #[test]
    fn selected_non_utf8_xdg_is_rejected() {
        let flags = Flags::default().xdg(b"/tmp/\xff");
        assert!(matches!(
            flags.runtime_paths(),
            Err(RuntimeError::NonUtf8Environment)
        ));
    }

    #[test]
    fn invalid_xdg_directories_are_rejected() {
        for base in [
            "",
            "relative",
            "/tmp/../helper",
            "/tmp/./helper",
            "/tmp/line\nbreak",
            "/tmp/nul\0byte",
        ] {
            let flags = Flags::default().xdg(base.as_bytes());
            assert!(
                matches!(flags.runtime_paths(), Err(RuntimeError::InvalidPath(_))),
                "{base:?}"
            );
        }
    }
}
