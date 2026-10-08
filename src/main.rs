use std::process::ExitCode;

use clap::Parser;
use ssh_auth_helper::cli;

fn main() -> ExitCode {
    cli::Cli::parse()
        .run()
        .inspect_err(|error| eprintln!("ssh-auth-helper: {error}"))
        .map_or(ExitCode::FAILURE, |()| ExitCode::SUCCESS)
}
