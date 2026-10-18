//! Foreground daemon supervision, shared registry ownership and pinned agent relays.

use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;

use super::*;

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
    serve_until_with_config(paths, ForwardingConfig::default(), shutdown).await
}

/// Serve with validated forwarding limits and caller-supplied shutdown.
///
/// # Errors
/// Rejects invalid limits and reports startup, task or cleanup failures.
pub async fn serve_until_with_config(
    paths: RuntimePaths,
    config: ForwardingConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), DaemonError> {
    config.validate()?;
    let max_agents = config.agent_clients;
    let deadline = Instant::now() + control::STARTUP_TIMEOUT;
    let directory = RuntimeDirectory::prepare(paths)?;
    let lock = RuntimeLock::acquire(directory)?;
    let sockets = RuntimeSockets::bind(lock, deadline).await?;
    let state = match State::new(control::hello_response(sockets.directory().paths()), config) {
        Ok(state) => Arc::new(state),
        Err(error) => {
            sockets.cleanup()?;
            return Err(error.into());
        }
    };
    let mut tasks = task::JoinSet::new();
    let mut agents = task::JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            accepted = sockets.control_listener().accept() => {
                let (stream, _) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(DaemonError::Io(error)) };
                if sockets.directory().authorize_peer(&stream).is_err() { continue }
                if tasks.len() >= MAX_CONTROL_CLIENTS { continue }
                let state = Arc::clone(&state);
                tasks.spawn(async move {
                    // Malformed or disconnected peers affect their connection only.
                    let _ = control::handle(stream, &state).await;
                });
            }
            accepted = sockets.agent_listener().accept() => {
                let (stream, _) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(DaemonError::Io(error)) };
                if sockets.directory().authorize_peer(&stream).is_err() { continue }
                if agents.len() >= max_agents { continue }
                let state = Arc::clone(&state);
                agents.spawn(async move { forwarding::handle(stream, state).await; });
            }
            Some(joined) = agents.join_next(), if !agents.is_empty() => {
                if let Err(error) = joined { break Err(DaemonError::Task(error)) }
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = joined { break Err(DaemonError::Task(error)) }
            }
        }
    };
    agents.abort_all();
    let mut agent_panic = None;
    while let Some(joined) = agents.join_next().await {
        if let Err(error) = joined
            && error.is_panic()
        {
            agent_panic = Some(error);
        }
    }
    let result = agent_panic.map_or(result, |error| Err(DaemonError::Task(error)));
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
    Config(#[from] forwarding::InvalidConfig),
    #[error(transparent)]
    Registry(#[from] registry::RegistryError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("daemon I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("daemon task failed: {0}")]
    Task(#[from] task::JoinError),
}
