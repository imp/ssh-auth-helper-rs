//! CLI parsing and output; daemon operations will be added as implemented.

use std::ffi::OsString;
use std::io;

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
    pub xdg_runtime_dir: Option<OsString>,
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
    /// Returns an error for invalid runtime path input or failed stdout writes.
    pub fn run(self) -> Result<(), CliError> {
        use io::Write;

        match self.command {
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
    /// Print the planned agent socket path without creating files or starting a daemon.
    SocketPath,
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("cannot write command output: {0}")]
    Output(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn explicit_directory_overrides_non_utf8_xdg() {
        let flags = Flags {
            runtime_dir: Some("/tmp/helper".into()),
            xdg_runtime_dir: Some(OsString::from_vec(b"/tmp/\xff".to_vec())),
        };
        assert_eq!(
            flags.runtime_paths().unwrap().agent(),
            "/tmp/helper/agent.sock"
        );
    }

    #[test]
    fn xdg_directory_contains_helper_endpoints() {
        let flags = Flags {
            xdg_runtime_dir: Some("/run/user/42".into()),
            ..Flags::default()
        };
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
        let flags = Flags {
            xdg_runtime_dir: Some(OsString::from_vec(b"/tmp/\xff".to_vec())),
            ..Flags::default()
        };
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
            let flags = Flags {
                xdg_runtime_dir: Some(base.into()),
                ..Flags::default()
            };
            assert!(
                matches!(flags.runtime_paths(), Err(RuntimeError::InvalidPath(_))),
                "{base:?}"
            );
        }
    }
}
