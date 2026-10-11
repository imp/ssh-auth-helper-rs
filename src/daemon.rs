//! Foreground daemon supervision. Upstream registration and relays are pending.

use std::sync::Arc;

use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::*;

const MAX_CONTROL_CLIENTS: usize = 32;

/// Serve until SIGINT/SIGTERM, then clean up this instance's socket entries.
///
/// # Errors
/// Reports startup, listener, task-supervision and cleanup failures.
pub async fn serve(paths: RuntimePaths) -> Result<(), DaemonError> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    serve_until(paths, async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
    })
    .await
}

/// Foreground serving with caller-supplied shutdown, useful for service embedding.
///
/// # Errors
/// Returns the same startup/supervision/cleanup errors as `serve`.
pub async fn serve_until(
    paths: RuntimePaths,
    shutdown: impl Future<Output = ()>,
) -> Result<(), DaemonError> {
    let deadline = Instant::now() + control::STARTUP_TIMEOUT;
    let directory = RuntimeDirectory::prepare(paths)?;
    let lock = RuntimeLock::acquire(directory)?;
    let sockets = RuntimeSockets::bind(lock, deadline).await?;
    let hello = Arc::new(control::hello_response(sockets.directory().paths()));
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            accepted = sockets.control_listener().accept() => {
                let (stream, _) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(DaemonError::Io(error)) };
                if sockets.directory().authorize_peer(&stream).is_err() { continue }
                if tasks.len() >= MAX_CONTROL_CLIENTS { continue }
                let hello = Arc::clone(&hello);
                tasks.spawn(async move {
                    // Malformed or disconnected peers affect their connection only.
                    let _ = control::handle(stream, &hello).await;
                });
            }
            accepted = sockets.agent_listener().accept() => {
                let (stream, _) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(DaemonError::Io(error)) };
                // This increment has no upstreams. Authorize before handling traffic
                // and close immediately; never fabricate agent protocol responses.
                if sockets.directory().authorize_peer(&stream).is_err() { continue }
                drop(stream);
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = joined { break Err(DaemonError::Task(error)) }
            }
        }
    };
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined
            && error.is_panic()
        {
            if let Err(cleanup) = sockets.cleanup() {
                eprintln!("ssh-auth-helper: {cleanup}");
            }
            return Err(DaemonError::Task(error));
        }
    }
    let cleanup = sockets.cleanup();
    if let Err(error) = result {
        if let Err(cleanup) = cleanup {
            eprintln!("ssh-auth-helper: {cleanup}");
        }
        return Err(error);
    }
    cleanup?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("daemon I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("daemon task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}
