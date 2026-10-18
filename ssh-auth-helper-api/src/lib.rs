//! Shared management API contract for ssh-auth-helper. No runtime discovery or I/O.

use std::fmt;
use std::str;

use camino::Utf8PathBuf;
use jiff::Timestamp;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub use forwarding::ForwardingBounds;
pub use forwarding::ProbeFailure;
pub use forwarding::RelayFailure;
pub use forwarding::SetupFailure;
pub use hello::HelloRequest;
pub use registration::Health;
pub use registration::MAX_LABEL_BYTES;
pub use registration::MAX_REGISTRATIONS;
pub use registration::MAX_RETIREMENTS;
pub use registration::REGISTER_METHOD;
pub use registration::RegisterRequest;
pub use registration::RegisterResponse;
pub use registration::Registration;
pub use registration::RegistrationId;
pub use registration::Retirement;
pub use registration::RetirementReason;
pub use registration::STATUS_METHOD;
pub use registration::StatusRequest;
pub use registration::StatusResponse;
pub use registration::UNREGISTER_METHOD;
pub use registration::UnregisterRequest;
pub use registration::UnregisterResponse;

mod forwarding;
mod hello;
mod registration;

/// Helper capability version, independent of the JSON-RPC envelope version.
pub const PROTOCOL_VERSION: u32 = 1;
/// Method used to query initialized listeners.
pub const HELLO_METHOD: &str = hello::HelloRequest::METHOD;

/// A management request owns its wire method name and corresponding response type.
pub trait Request: Serialize + DeserializeOwned {
    const METHOD: &'static str;

    type Response: Serialize + DeserializeOwned;
}

/// Public endpoint paths; constructing these values does not validate filesystem safety.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoints {
    pub agent: Utf8PathBuf,
    pub control: Utf8PathBuf,
}

/// Initialized listeners. Readiness does not imply an available upstream agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloResponse {
    pub instance: Uuid,
    pub supported_versions: Vec<u32>,
    pub ready: bool,
    pub endpoints: Endpoints,
}
