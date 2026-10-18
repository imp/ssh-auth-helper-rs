//! Bounded setup followed by a pinned opaque byte relay.

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use super::*;

pub(crate) const MAX_CONTROL_CLIENTS: usize = 32;

/// Validated limits for the foreground proxy. Defaults match the control status bounds.
#[derive(Clone, Debug)]
pub struct ForwardingConfig {
    pub probe_timeout: Duration,
    pub setup_timeout: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub max_probe_bytes: usize,
    pub agent_clients: usize,
    pub relay_buffer_bytes: usize,
}

impl Default for ForwardingConfig {
    fn default() -> Self {
        Self {
            probe_timeout: Duration::from_millis(750),
            setup_timeout: Duration::from_secs(3),
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(30),
            max_probe_bytes: 1024 * 1024,
            agent_clients: 128,
            relay_buffer_bytes: 64 * 1024,
        }
    }
}

impl ForwardingConfig {
    /// Validate nonzero limits without changing process configuration.
    ///
    /// # Errors
    /// Rejects zero, inverted, or over-maximum limits.
    pub fn validate(&self) -> Result<(), InvalidConfig> {
        if self.probe_timeout.is_zero()
            || self.probe_timeout > self.setup_timeout
            || self.setup_timeout > Duration::from_secs(3)
            || self.retry_initial.is_zero()
            || self.retry_initial > self.retry_max
            || self.retry_max > Duration::from_secs(30)
            || !(5..=1024 * 1024).contains(&self.max_probe_bytes)
            || !(1..=128).contains(&self.agent_clients)
            || !(1..=64 * 1024).contains(&self.relay_buffer_bytes)
        {
            return Err(InvalidConfig);
        }
        Ok(())
    }

    pub(crate) fn bounds(&self) -> api::ForwardingBounds {
        api::ForwardingBounds {
            probe_timeout_ms: millis(self.probe_timeout),
            setup_timeout_ms: millis(self.setup_timeout),
            retry_initial_ms: millis(self.retry_initial),
            retry_max_ms: millis(self.retry_max),
            max_probe_bytes: self.max_probe_bytes,
            agent_clients: self.agent_clients,
            control_clients: MAX_CONTROL_CLIENTS,
            relay_buffer_bytes: self.relay_buffer_bytes,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid forwarding limits")]
pub struct InvalidConfig;

pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The successful probe owns the stream that must be used by the relay.
struct ProbedAgent {
    stream: net::UnixStream,
}

impl ProbedAgent {
    async fn connect(
        ticket: &ProbeTicket,
        config: &ForwardingConfig,
        deadline: Instant,
    ) -> Result<Self, api::ProbeFailure> {
        let deadline = deadline.min(Instant::now() + config.probe_timeout);
        timeout_at(deadline, async {
            ticket.validate_identity()?;
            let mut stream = net::UnixStream::connect(&ticket.socket)
                .await
                .map_err(|_| api::ProbeFailure::Transport)?;
            stream
                .write_all(&[0, 0, 0, 1, 11])
                .await
                .map_err(|_| api::ProbeFailure::Transport)?;
            let length = usize::try_from(
                stream
                    .read_u32()
                    .await
                    .map_err(|_| api::ProbeFailure::Transport)?,
            )
            .map_err(|_| api::ProbeFailure::MalformedReply)?;
            if !(5..=config.max_probe_bytes).contains(&length) {
                return Err(api::ProbeFailure::MalformedReply);
            }
            let mut body = vec![0; length];
            stream
                .read_exact(&mut body)
                .await
                .map_err(|_| api::ProbeFailure::Transport)?;
            IdentitiesAnswer::validate(&body)?;
            ticket.validate_identity()?;
            if Instant::now() >= deadline {
                return Err(api::ProbeFailure::Timeout);
            }
            Ok(Self { stream })
        })
        .await
        .map_err(|_| api::ProbeFailure::Timeout)?
    }
}

struct IdentitiesAnswer;

impl IdentitiesAnswer {
    fn validate(body: &[u8]) -> Result<Self, api::ProbeFailure> {
        let mut rest = body;
        if rest.first() != Some(&12) {
            return Err(api::ProbeFailure::MalformedReply);
        }
        rest = &rest[1..];
        let count = take_u32(&mut rest).ok_or(api::ProbeFailure::MalformedReply)?;
        if u64::from(count) > u64::try_from(rest.len() / 8).unwrap_or(u64::MAX) {
            return Err(api::ProbeFailure::MalformedReply);
        }
        for _ in 0..count {
            for _ in 0..2 {
                let length = take_u32(&mut rest).ok_or(api::ProbeFailure::MalformedReply)?;
                let length =
                    usize::try_from(length).map_err(|_| api::ProbeFailure::MalformedReply)?;
                rest = rest
                    .get(length..)
                    .ok_or(api::ProbeFailure::MalformedReply)?;
            }
        }
        if !rest.is_empty() {
            return Err(api::ProbeFailure::MalformedReply);
        }
        Ok(Self)
    }
}

fn take_u32(bytes: &mut &[u8]) -> Option<u32> {
    let value = u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?);
    *bytes = &bytes[4..];
    Some(value)
}

pub(crate) async fn handle(mut client: net::UnixStream, state: Arc<State>) {
    let deadline = Instant::now() + state.config.setup_timeout;
    loop {
        if Instant::now() >= deadline {
            state.setup_failed(api::SetupFailure::Deadline);
            return;
        }
        let ticket = match state.next_probe() {
            Ok(ticket) => ticket,
            Err(reason) => {
                state.setup_failed(reason);
                return;
            }
        };
        let Ok(Ok(permit)) = timeout_at(deadline, ticket.gate.acquire()).await else {
            state.setup_failed(api::SetupFailure::Deadline);
            return;
        };
        if !state.ticket_current(&ticket) {
            continue;
        }
        match ProbedAgent::connect(&ticket, &state.config, deadline).await {
            Err(failure) => state.probe_failed(&ticket, failure),
            Ok(mut upstream) => {
                if Instant::now() >= deadline {
                    state.setup_failed(api::SetupFailure::Deadline);
                    return;
                }
                let Some(lease) = state.pin(&ticket) else {
                    continue;
                };
                // No permit or registry lock is retained during a signing operation.
                drop(permit);
                let result = tokio::io::copy_bidirectional_with_sizes(
                    &mut client,
                    &mut upstream.stream,
                    state.config.relay_buffer_bytes,
                    state.config.relay_buffer_bytes,
                )
                .await;
                if result.is_err() {
                    state.relay_failed(&ticket.id);
                }
                drop(lease);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests;
