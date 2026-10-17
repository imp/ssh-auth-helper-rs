//! Foundation for a per-user forwarded SSH agent proxy.
//!
//! The library provides the daemon, bounded readiness and safe runtime ownership.
//! The separate CLI crate provides foreground serving, management commands and path discovery.
//! Each agent client probes and retains one upstream connection for an opaque relay.

use std::ffi;
use std::fs;
use std::io;
use std::sync::Arc;

use camino::Utf8Path;
use camino::Utf8PathBuf;
use serde_json as json;
use tokio::net;
use tokio::task;

pub use ssh_auth_helper_api as api;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("ssh-auth-helper currently targets Linux and macOS");

use forwarding::ForwardingConfig;
use forwarding::MAX_CONTROL_CLIENTS;
use registry::ProbeTicket;
use registry::State;
use runtime::RuntimeDirectory;
use runtime::RuntimeError;
use runtime::RuntimeLock;
use runtime::RuntimePaths;
use runtime::RuntimeSockets;

pub mod control;
pub mod daemon;
pub mod forwarding;
mod registry;
pub mod runtime;
