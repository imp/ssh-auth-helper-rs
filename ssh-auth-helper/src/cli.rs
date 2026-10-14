//! CLI parsing, explicit output and foreground serving.

use clap::Args;
use clap::Subcommand;
use serde_json as json;
use ssh_auth_helper_lib::control;

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
    pub async fn run(self) -> Result<(), CliError> {
        use io::Write;

        let paths = self.flags.runtime_paths()?;
        match self.command {
            Command::Serve => daemon::serve(paths).await?,
            Command::Register(args) => {
                let machine_output = args.json;
                let request = args.into_request()?;
                let response = control::register(&paths, request).await?;
                if machine_output {
                    write_json(json::to_value(response)?)?;
                }
            }
            Command::Unregister { id, json } => {
                let response =
                    control::unregister(&paths, control::UnregisterRequest { id }).await?;
                if json {
                    write_json(json::to_value(response)?)?;
                }
            }
            Command::Status { json } => {
                let response = control::status(&paths).await?;
                if json {
                    write_json(json::to_value(response)?)?;
                } else {
                    let mut output = io::stdout().lock();
                    writeln!(output, "Instance: {}", response.instance)?;
                    writeln!(
                        output,
                        "Registered agents: {}",
                        response.registrations.len()
                    )?;
                    writeln!(output, "Active agent: none")?;
                    for registration in response.registrations {
                        let label = registration
                            .label
                            .map_or_else(|| "-".to_owned(), |label| format!("{label:?}"));
                        writeln!(
                            output,
                            "{}  {}  unobserved  {}",
                            registration.id, registration.socket, label
                        )?;
                    }
                    writeln!(
                        output,
                        "Recent retirements: {}",
                        response.recent_retirements.len()
                    )?;
                }
            }
            Command::SocketPath => {
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
    /// Register an original forwarded socket with an already running daemon.
    Register(RegisterArgs),
    /// Remove a registration; repeated cleanup succeeds.
    Unregister {
        id: control::RegistrationId,
        #[arg(long)]
        json: bool,
    },
    /// Inspect registered sockets without probing agent availability.
    Status {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Default, Args)]
pub struct RegisterArgs {
    /// Original forwarded agent socket; defaults to SSH_AUTH_SOCK.
    #[arg(long, env = "SSH_AUTH_SOCK")]
    pub socket: Option<ffi::OsString>,
    /// Optional display label, at most 128 UTF-8 bytes.
    #[arg(long)]
    pub label: Option<String>,
    /// Print the registration result as JSON; default output is silent.
    #[arg(long)]
    pub json: bool,
}

impl RegisterArgs {
    fn into_request(self) -> Result<control::RegisterRequest, CliError> {
        let socket = self.socket.ok_or(CliError::MissingSocket)?;
        let socket = camino::Utf8Path::from_os_str(&socket)
            .ok_or(CliError::NonUtf8Socket)?
            .to_owned();
        Ok(control::RegisterRequest {
            socket,
            label: self.label,
        })
    }
}

fn write_json(value: json::Value) -> Result<(), CliError> {
    use io::Write;

    let text = json::to_string(&value)?;
    writeln!(io::stdout().lock(), "{text}")?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Control(#[from] control::ControlError),
    #[error("no forwarded agent socket; pass --socket or set SSH_AUTH_SOCK")]
    MissingSocket,
    #[error("forwarded agent socket must be UTF-8")]
    NonUtf8Socket,
    #[error("cannot serialize command output: {0}")]
    Json(#[from] json::Error),
    #[error(transparent)]
    Daemon(#[from] daemon::DaemonError),
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
        let flags = Flags::default().xdg(b"/run/user/42");
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

#[cfg(test)]
mod registration_tests {
    use std::os::unix::ffi::OsStrExt;

    use super::*;

    #[test]
    fn register_options_are_constructible_without_environment_access() {
        let args = RegisterArgs {
            socket: Some("/tmp/original-agent".into()),
            label: Some("laptop".into()),
            json: false,
        };
        let request = args.into_request().unwrap();
        assert_eq!(request.socket, "/tmp/original-agent");
        assert_eq!(request.label.as_deref(), Some("laptop"));
        assert!(matches!(
            RegisterArgs::default().into_request(),
            Err(CliError::MissingSocket)
        ));
    }

    #[test]
    fn selected_non_utf8_socket_is_rejected_before_a_control_call() {
        let args = RegisterArgs {
            socket: Some(ffi::OsStr::from_bytes(b"/tmp/\xff").to_owned()),
            ..RegisterArgs::default()
        };
        assert!(matches!(args.into_request(), Err(CliError::NonUtf8Socket)));
    }
}
