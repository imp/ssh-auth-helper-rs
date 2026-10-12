//! Foundation for a per-user forwarded SSH agent proxy.
//!
//! The binary provides foreground serving and socket-path discovery. The daemon
//! offers bounded readiness and safe runtime ownership. Upstream registration,
//! health checks and relaying are not implemented yet.

use std::ffi;
use std::io;

use camino::Utf8Path;
use camino::Utf8PathBuf;
use serde_json as json;
use tokio::net;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("ssh-auth-helper currently targets Linux and macOS");

use runtime::RuntimeDirectory;
use runtime::RuntimeError;
use runtime::RuntimeLock;
use runtime::RuntimePaths;
use runtime::RuntimeSockets;

pub mod cli;
pub mod control;
pub mod daemon;
pub mod runtime;
