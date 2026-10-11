//! Strict envelope checks supplementing jsonrpsee's deliberately permissive response parsing.

use jsonrpsee_types::ErrorObjectOwned;
use jsonrpsee_types::Id;
use jsonrpsee_types::Response;
use jsonrpsee_types::ResponsePayload;
use jsonrpsee_types::TwoPointZero;
use serde::Deserialize;
use serde::Deserializer;
use serde_json as json;
use serde_json::value::RawValue;

/// Maximum string request-ID size in UTF-8 bytes.
pub const MAX_ID_BYTES: usize = 128;

#[derive(Debug, Default)]
enum Member<T> {
    #[default]
    Absent,
    Present(T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Member<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self::Present)
    }
}

// RawValue preserves explicit null and duplicate fields in nested method params.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    jsonrpc: TwoPointZero,
    #[serde(default)]
    id: Member<Box<RawValue>>,
    #[serde(default)]
    method: Member<String>,
    #[serde(default)]
    params: Member<Box<RawValue>>,
    #[serde(default)]
    result: Member<Box<RawValue>>,
    #[serde(default)]
    error: Member<Box<RawValue>>,
}

/// A validated request or notification. Absent ID differs from explicit null.
#[derive(Debug)]
pub struct RpcRequest {
    id: Option<Id<'static>>,
    method: String,
    params: Option<Box<RawValue>>,
}

impl RpcRequest {
    /// Method name, without an application-specific interpretation.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// None denotes a notification; Some(Id::Null) is an identified call.
    pub fn id(&self) -> Option<&Id<'static>> {
        self.id.as_ref()
    }

    /// Decode typed method params without losing duplicate-field checks.
    ///
    /// # Errors
    /// Returns Invalid params when the method schema rejects the supplied value.
    pub fn params<T: serde::de::DeserializeOwned>(&self) -> Result<T, ErrorObjectOwned> {
        json::from_str(self.params.as_ref().map_or("{}", |raw| raw.get()))
            .map_err(|_| rpc_error(-32602, "Invalid params"))
    }
}

/// A single or batch wire response using the upstream response representation.
pub type RpcResponse = Response<'static, json::Value>;

/// Construct a standard structured error without including untrusted payloads.
pub fn rpc_error(code: i32, message: &'static str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(code, message, None::<()>)
}

/// Construct an error response with the supplied correlation ID.
pub fn error_response(id: Id<'static>, error: ErrorObjectOwned) -> RpcResponse {
    Response::new(ResponsePayload::error(error), id)
}

fn parse_id(raw: &RawValue) -> Result<Id<'static>, ErrorObjectOwned> {
    let id: Id<'_> = json::from_str(raw.get()).map_err(|_| rpc_error(-32600, "Invalid Request"))?;
    if let Id::Str(value) = &id
        && (value.is_empty() || value.len() > MAX_ID_BYTES)
    {
        return Err(rpc_error(-32600, "Invalid Request"));
    }
    Ok(id.into_owned())
}

fn structured(raw: &RawValue) -> bool {
    matches!(raw.get().as_bytes().first(), Some(b'{' | b'['))
}

/// Parse a single call/notification under the documented ID and strict-field profile.
///
/// # Errors
/// Returns Invalid Request for malformed envelopes; frame syntax is checked separately.
pub fn parse_request(raw: &RawValue) -> Result<RpcRequest, ErrorObjectOwned> {
    let envelope: Envelope =
        json::from_str(raw.get()).map_err(|_| rpc_error(-32600, "Invalid Request"))?;
    let _version = envelope.jsonrpc;
    let Member::Present(method) = envelope.method else {
        return Err(rpc_error(-32600, "Invalid Request"));
    };
    if !matches!(envelope.result, Member::Absent) || !matches!(envelope.error, Member::Absent) {
        return Err(rpc_error(-32600, "Invalid Request"));
    }
    let id = match envelope.id {
        Member::Absent => None,
        Member::Present(raw) => Some(parse_id(&raw)?),
    };
    let params = match envelope.params {
        Member::Absent => None,
        Member::Present(raw) if structured(&raw) => Some(raw),
        Member::Present(_) => return Err(rpc_error(-32600, "Invalid Request")),
    };
    Ok(RpcRequest { id, method, params })
}

/// Require exact-version unambiguous responses or structured server notifications.
///
/// # Errors
/// Returns Invalid Request for invalid envelope shape or fields.
pub fn validate_server_frame(
    frame: &str,
    max_batch_members: usize,
) -> Result<(), ErrorObjectOwned> {
    let raw: Box<RawValue> = json::from_str(frame).map_err(|_| rpc_error(-32700, "Parse error"))?;
    if raw.get().starts_with('[') {
        let entries: Vec<Box<RawValue>> =
            json::from_str(raw.get()).map_err(|_| rpc_error(-32600, "Invalid Request"))?;
        if entries.is_empty() || entries.len() > max_batch_members {
            return Err(rpc_error(-32600, "Invalid Request"));
        }
        for entry in entries {
            validate_server_object(&entry)?;
        }
        Ok(())
    } else {
        validate_server_object(&raw)
    }
}

fn validate_server_object(raw: &RawValue) -> Result<(), ErrorObjectOwned> {
    let envelope: Envelope =
        json::from_str(raw.get()).map_err(|_| rpc_error(-32600, "Invalid Request"))?;
    let _version = envelope.jsonrpc;
    match envelope.method {
        Member::Present(_) => {
            if !matches!(envelope.id, Member::Absent)
                || !matches!(envelope.result, Member::Absent)
                || !matches!(envelope.error, Member::Absent)
            {
                return Err(rpc_error(-32600, "Invalid Request"));
            }
            if let Member::Present(params) = envelope.params
                && !structured(&params)
            {
                return Err(rpc_error(-32600, "Invalid Request"));
            }
        }
        Member::Absent => {
            let Member::Present(id) = envelope.id else {
                return Err(rpc_error(-32600, "Invalid Request"));
            };
            parse_id(&id)?;
            if !matches!(envelope.params, Member::Absent) {
                return Err(rpc_error(-32600, "Invalid Request"));
            }
            match (envelope.result, envelope.error) {
                (Member::Present(_), Member::Absent) => {}
                (Member::Absent, Member::Present(error)) => {
                    json::from_str::<ErrorObjectOwned>(error.get())
                        .map_err(|_| rpc_error(-32600, "Invalid Request"))?;
                }
                _ => return Err(rpc_error(-32600, "Invalid Request")),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_id_is_a_notification_and_null_is_a_call() {
        let notification =
            json::from_str::<Box<RawValue>>(r#"{"jsonrpc":"2.0","method":"hello"}"#).unwrap();
        let call =
            json::from_str::<Box<RawValue>>(r#"{"jsonrpc":"2.0","method":"hello","id":null}"#)
                .unwrap();
        assert!(parse_request(&notification).unwrap().id().is_none());
        assert_eq!(parse_request(&call).unwrap().id(), Some(&Id::Null));
    }

    #[test]
    fn strict_requests_reject_unsupported_ids_versions_and_duplicate_fields() {
        for frame in [
            r#"{"jsonrpc":"2.0","method":"hello","id":true}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":-1}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":1.5}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":""}"#,
            r#"{"jsonrpc":"3.0","method":"hello","id":"a"}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":"a","id":"b"}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":"a","params":null}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":"a","params":true}"#,
            r#"{"jsonrpc":"2.0","method":"hello","id":"a","extra":true}"#,
        ] {
            let raw = json::from_str::<Box<RawValue>>(frame).unwrap();
            assert!(parse_request(&raw).is_err(), "{frame}");
        }
    }

    #[test]
    fn strict_responses_reject_leniency_ambiguity_and_duplicate_fields() {
        for frame in [
            r#"{"id":"a","result":true}"#,
            r#"{"jsonrpc":null,"id":"a","result":true}"#,
            r#"{"jsonrpc":"3.0","id":"a","result":true}"#,
            r#"{"jsonrpc":"2.0","id":"a","result":true,"error":{"code":-32603,"message":"bad"}}"#,
            r#"{"jsonrpc":"2.0","id":"a","result":1,"result":2}"#,
            r#"{"jsonrpc":"2.0","id":"a"}"#,
            r#"{"jsonrpc":"2.0","id":"a","error":null}"#,
        ] {
            assert!(validate_server_frame(frame, 32).is_err(), "{frame}");
        }
        assert!(validate_server_frame(r#"{"jsonrpc":"2.0","id":"a","result":null}"#, 32).is_ok());
    }
}
