//! Executable-owned detached startup. The library client remains connect-only.

use std::path::Path;
use std::process::ExitStatus;
use std::process::Stdio;
use std::time::Duration;

use ssh_auth_helper_lib::api;
use ssh_auth_helper_lib::control;
use ssh_auth_helper_lib::runtime::RuntimeDirectory;
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout_at;

use super::*;

pub(super) async fn register(
    paths: &RuntimePaths,
    request: api::RegisterRequest,
) -> Result<api::RegisterResponse, StartupError> {
    let deadline = Instant::now() + control::STARTUP_TIMEOUT;
    let mut startup = Startup::default();
    let result = timeout_at(deadline, async {
        let mut client = startup.client(paths).await?;
        client.register(request).await.map_err(StartupError::from)
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            startup.reap()?;
            Err(StartupError::Timeout(startup.exit))
        }
    }
}

#[derive(Debug, Default)]
struct Startup {
    child: Option<Child>,
    exit: Option<ExitStatus>,
}

impl Startup {
    async fn client(&mut self, paths: &RuntimePaths) -> Result<control::Client, StartupError> {
        let mut may_launch = true;
        loop {
            self.reap()?;
            match paths.client().await {
                Ok(client) => return Ok(client),
                Err(control::ControlError::Io(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    if may_launch {
                        // Preparation validates existing objects but never recovers endpoints.
                        drop(RuntimeDirectory::prepare(paths.clone())?);
                        let executable = std::env::current_exe().map_err(StartupError::Launch)?;
                        let mut command = detached_command(&executable, paths);
                        // Mark, don't close: the parent's Tokio descriptors remain usable.
                        // This executable-only sweep must never run in a unit-test process.
                        close_fds::set_fds_cloexec_threadsafe(3, &[]);
                        self.child = Some(command.spawn().map_err(StartupError::Launch)?);
                        may_launch = false;
                    }
                }
                // An ambiguous listener never authorizes launch, even if it later vanishes.
                Err(control::ControlError::Timeout) => may_launch = false,
                Err(error) => return Err(error.into()),
            }
            sleep(Duration::from_millis(25)).await;
        }
    }

    fn reap(&mut self) -> Result<(), StartupError> {
        if let Some(child) = self.child.as_mut()
            && let Some(exit) = child.try_wait().map_err(StartupError::Child)?
        {
            self.exit = Some(exit);
            self.child = None;
        }
        Ok(())
    }
}

fn detached_command(executable: &Path, paths: &RuntimePaths) -> Command {
    let mut command = Command::new(executable);
    command
        .arg("--runtime-dir")
        .arg(paths.directory())
        .args(["serve", "--detached"])
        .current_dir("/")
        .env_remove("SSH_AUTH_SOCK")
        .env_remove("SSH_AGENT_PID")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false);
    command
}

#[derive(Debug, thiserror::Error)]
pub(super) enum StartupError {
    #[error(transparent)]
    Control(#[from] control::ControlError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("cannot launch daemon: {0}; run foreground serve for startup diagnostics")]
    Launch(#[source] io::Error),
    #[error("cannot observe daemon child: {0}")]
    Child(#[source] io::Error),
    #[error(
        "startup/registration timed out (child exit: {0:?}); registration may have succeeded; run foreground serve for startup diagnostics"
    )]
    Timeout(Option<ExitStatus>),
}
