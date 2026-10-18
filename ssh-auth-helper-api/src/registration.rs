//! Registration IDs and management schemas; filesystem validation belongs to the daemon.

use super::*;

pub const REGISTER_METHOD: &str = RegisterRequest::METHOD;
pub const UNREGISTER_METHOD: &str = UnregisterRequest::METHOD;
pub const STATUS_METHOD: &str = StatusRequest::METHOD;
pub const MAX_REGISTRATIONS: usize = 64;
pub const MAX_RETIREMENTS: usize = 64;
pub const MAX_LABEL_BYTES: usize = 128;

/// Opaque ID qualified by the daemon instance that issued it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RegistrationId {
    instance: Uuid,
    registration: Uuid,
}

impl RegistrationId {
    /// Construct an ID from non-nil UUIDs.
    ///
    /// # Errors
    /// Rejects nil instance or registration UUIDs.
    pub fn new(instance: Uuid, registration: Uuid) -> Result<Self, &'static str> {
        if instance.is_nil() || registration.is_nil() {
            return Err("registration ID requires non-nil UUIDs");
        }
        Ok(Self {
            instance,
            registration,
        })
    }

    pub fn instance(&self) -> Uuid {
        self.instance
    }
}

impl fmt::Display for RegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.instance, self.registration)
    }
}

impl str::FromStr for RegistrationId {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (instance, registration) = value.split_once('/').ok_or("invalid registration ID")?;
        let instance =
            Uuid::parse_str(instance).map_err(|_| "invalid registration instance UUID")?;
        let registration =
            Uuid::parse_str(registration).map_err(|_| "invalid registration UUID")?;
        Self::new(instance, registration)
    }
}

impl TryFrom<String> for RegistrationId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<RegistrationId> for String {
    fn from(value: RegistrationId) -> Self {
        value.to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterRequest {
    pub socket: Utf8PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl Request for RegisterRequest {
    const METHOD: &'static str = "register";

    type Response = RegisterResponse;
}

/// Last observed connectivity, not a promise of signing capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    Unobserved,
    Available,
    TemporarilyUnavailable,
}

impl fmt::Display for Health {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unobserved => "unobserved",
            Self::Available => "available",
            Self::TemporarilyUnavailable => "temporarily unavailable",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub id: RegistrationId,
    pub socket: Utf8PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub registered_at: Timestamp,
    pub health: Health,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<ProbeFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub active_relays: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterResponse {
    pub registration: Registration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnregisterRequest {
    pub id: RegistrationId,
}

impl Request for UnregisterRequest {
    const METHOD: &'static str = "unregister";

    type Response = UnregisterResponse;
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnregisterResponse {}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusRequest {}

impl Request for StatusRequest {
    const METHOD: &'static str = "status";

    type Response = StatusResponse;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetirementReason {
    Replaced,
    Unregistered,
    Missing,
    Unsafe,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retirement {
    pub registration: Registration,
    pub retired_at: Timestamp,
    pub reason: RetirementReason,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusResponse {
    pub instance: Uuid,
    pub endpoints: Endpoints,
    pub registrations: Vec<Registration>,
    pub recent_retirements: Vec<Retirement>,
    pub capacity: usize,
    pub retirement_capacity: usize,
    pub active: Option<RegistrationId>,
    pub active_relays: usize,
    pub bounds: ForwardingBounds,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_setup_failure: Option<SetupFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_relay_error: Option<RelayFailure>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_ids_round_trip_and_reject_malformed_or_nil_components() {
        let id = RegistrationId::new(Uuid::from_u128(1), Uuid::from_u128(2)).unwrap();
        let text = id.to_string();
        assert_eq!(text.parse::<RegistrationId>().unwrap(), id);
        assert_eq!(
            serde_json::from_value::<RegistrationId>(serde_json::json!(text)).unwrap(),
            id
        );
        assert_eq!(serde_json::to_value(&id).unwrap(), serde_json::json!(text));
        for text in [
            "",
            "wrong",
            "a/b",
            "00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000000",
        ] {
            assert!(text.parse::<RegistrationId>().is_err());
            assert!(
                serde_json::from_value::<UnregisterRequest>(serde_json::json!({"id":text}))
                    .is_err()
            );
        }
        assert!(
            serde_json::from_value::<RegisterRequest>(
                serde_json::json!({"socket":"/tmp/a","extra":true})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<StatusRequest>(serde_json::json!({"extra":true})).is_err()
        );
    }
}
