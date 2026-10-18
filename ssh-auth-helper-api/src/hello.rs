use super::*;

/// Hello has no application parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloRequest {}

impl Request for HelloRequest {
    const METHOD: &'static str = "hello";

    type Response = HelloResponse;
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
