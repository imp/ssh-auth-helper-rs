//! CLI parsing and output; daemon operations will be added as implemented.

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};

use crate::runtime::{RuntimeError, RuntimePaths};

#[derive(Debug, Parser)]
#[command(version, about = "Forwarded SSH agent helper (under development)")]
pub struct Cli {
    /// Absolute runtime directory shared by helper commands.
    #[arg(long, global = true)]
    pub runtime_dir: Option<Utf8PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print the planned agent socket path without creating files or starting a daemon.
    SocketPath,
}

/// Execute a supported command.
///
/// # Errors
/// Returns an error for invalid runtime path input or failed stdout writes.
pub fn run(cli: Cli) -> Result<(), CliError> {
    use std::io::Write;

    match cli.command {
        Command::SocketPath => {
            let paths = RuntimePaths::discover(cli.runtime_dir)?;
            writeln!(std::io::stdout().lock(), "{}", paths.agent())?;
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("cannot write command output: {0}")]
    Output(#[from] std::io::Error),
}
