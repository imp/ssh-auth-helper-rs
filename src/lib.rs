//! Foundation for a per-user forwarded SSH agent proxy.
//!
//! The current binary provides socket-path discovery. Daemon serving,
//! registration, health checks, and relaying are not implemented yet.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("ssh-auth-helper currently targets Linux and macOS");

pub mod cli;
pub mod runtime;
