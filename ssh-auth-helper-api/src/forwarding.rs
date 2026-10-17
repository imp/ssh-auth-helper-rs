//! Bounded health and forwarding diagnostics. Never contains agent payloads.

use jiff::Timestamp;

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeFailure {
    Timeout,
    Transport,
    MalformedReply,
    MissingSocket,
    ReplacedSocket,
    UnsafeSocket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupFailure {
    NoCandidates,
    Backoff,
    Deadline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayFailure {
    pub registration: RegistrationId,
    pub observed_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForwardingBounds {
    pub probe_timeout_ms: u64,
    pub setup_timeout_ms: u64,
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    pub max_probe_bytes: usize,
    pub agent_clients: usize,
    pub control_clients: usize,
    pub relay_buffer_bytes: usize,
}
