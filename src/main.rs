use clap::Parser;
use ssh_auth_helper::cli::Cli;
use std::process::ExitCode;

fn main() -> ExitCode {
    match ssh_auth_helper::cli::run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ssh-auth-helper: {error}");
            ExitCode::FAILURE
        }
    }
}
