//! Shared management API contract for ssh-auth-helper. No runtime discovery or I/O.

use camino::Utf8PathBuf;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Helper capability version, independent of the JSON-RPC envelope version.
pub const PROTOCOL_VERSION: u32 = 1;
/// Method used to query initialized listeners.
pub const HELLO_METHOD: &str = "hello";

/// Hello has no application parameters.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HelloRequest {}

/// Public endpoint paths; constructing these values does not validate filesystem safety.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Endpoints {
    pub agent: Utf8PathBuf,
    pub control: Utf8PathBuf,
}

/// Initialized listeners. Readiness does not imply an available upstream agent.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HelloResponse {
    pub instance: Uuid,
    pub supported_versions: Vec<u32>,
    pub ready: bool,
    pub endpoints: Endpoints,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_schema_round_trips_without_runtime_discovery() {
        let response = HelloResponse {
            instance: Uuid::from_u128(1),
            supported_versions: vec![PROTOCOL_VERSION],
            ready: true,
            endpoints: Endpoints {
                agent: "/example/agent.sock".into(),
                control: "/example/control.sock".into(),
            },
        };
        let encoded = serde_json::to_string(&response).unwrap();
        assert_eq!(
            serde_json::from_str::<HelloResponse>(&encoded).unwrap(),
            response
        );
        assert!(serde_json::from_str::<HelloRequest>(r#"{"unexpected":true}"#).is_err());
    }
}
