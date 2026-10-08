//! Foundation for a per-user forwarded SSH agent proxy.
//!
//! The current binary provides socket-path discovery. The library also offers
//! private-directory preparation and same-user peer authorization. Daemon serving,
//! registration, health checks, and relaying are not implemented yet.

use camino::Utf8Path;
use camino::Utf8PathBuf;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("ssh-auth-helper currently targets Linux and macOS");

use runtime::RuntimeError;
use runtime::RuntimePaths;

pub mod cli;
pub mod runtime;
