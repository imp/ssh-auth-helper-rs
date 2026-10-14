use std::ffi;
use std::io;
use std::process::ExitCode;

use camino::Utf8PathBuf;
use clap::Parser;
use ssh_auth_helper_lib::daemon;
use ssh_auth_helper_lib::runtime::RuntimeError;
use ssh_auth_helper_lib::runtime::RuntimePaths;

mod cli;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    cli::Cli::parse()
        .run()
        .await
        .inspect_err(|error| eprintln!("ssh-auth-helper: {error}"))
        .map_or(ExitCode::FAILURE, |()| ExitCode::SUCCESS)
}
