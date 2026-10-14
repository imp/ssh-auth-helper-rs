//! Owned JSON-RPC wire types. Method payloads use ordinary JSON values.

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::Error as _;

use super::*;

/// The only supported JSON-RPC version; missing/null/other versions are invalid.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum RpcVersion {
    #[serde(rename = "2.0")]
    V2,
}

/// The protocol's owned string, unsigned integer and explicit-null ID profile.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RpcId {
    String(String),
    Number(u64),
    Null,
}

impl RpcId {
    fn is_valid(&self) -> bool {
        !matches!(self, RpcId::String(s) if s.is_empty() || s.len() > MAX_ID_BYTES)
    }
}

impl From<u64> for RpcId {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}

impl From<String> for RpcId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for RpcId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

/// Standard numeric RPC errors. Data is optional ordinary JSON.
#[derive(Debug, Deserialize, Serialize, thiserror::Error)]
#[error("RPC error {code}: {message}")]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_value"
    )]
    pub data: Option<json::Value>,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

/// An identified call. Method payload validation belongs to its handler.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcRequest {
    pub jsonrpc: RpcVersion,
    #[serde(deserialize_with = "checked_id")]
    pub id: RpcId,
    pub method: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "params"
    )]
    pub params: Option<json::Value>,
}

impl RpcRequest {
    pub fn new(
        id: &RpcId,
        method: impl Into<String>,
        params: impl Into<Option<json::Value>>,
    ) -> Self {
        Self {
            jsonrpc: RpcVersion::V2,
            id: id.clone(),
            method: method.into(),
            params: params.into(),
        }
    }

    fn is_id_valid(&self) -> bool {
        self.id.is_valid()
    }

    fn is_params_valid(&self) -> bool {
        self.params.as_ref().is_none_or(is_structured)
    }

    fn is_valid(&self) -> bool {
        self.is_id_valid() && self.is_params_valid()
    }
}

/// A notification has no ID and receives no reply, including method errors.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcNotification {
    pub jsonrpc: RpcVersion,
    pub method: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "params"
    )]
    pub params: Option<json::Value>,
}

impl RpcNotification {
    fn is_valid(&self) -> bool {
        self.params.as_ref().is_none_or(is_structured)
    }
}

/// Responses contain exactly one result or error and always require version/ID.
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum RpcResponse {
    Success {
        jsonrpc: RpcVersion,
        #[serde(deserialize_with = "checked_id")]
        id: RpcId,
        result: json::Value,
    },
    Error {
        jsonrpc: RpcVersion,
        #[serde(deserialize_with = "checked_id")]
        id: RpcId,
        error: RpcError,
    },
}

impl RpcResponse {
    fn success(id: RpcId, result: json::Value) -> Self {
        Self::Success {
            jsonrpc: RpcVersion::V2,
            id,
            result,
        }
    }

    fn error(id: RpcId, error: RpcError) -> Self {
        Self::Error {
            jsonrpc: RpcVersion::V2,
            id,
            error,
        }
    }

    fn into_result(self) -> (RpcId, Result<json::Value, RpcError>) {
        match self {
            Self::Success { id, result, .. } => (id, Ok(result)),
            Self::Error { id, error, .. } => (id, Err(error)),
        }
    }

    pub fn id(&self) -> &RpcId {
        match self {
            Self::Success { id, .. } | Self::Error { id, .. } => id,
        }
    }

    fn is_valid(&self) -> bool {
        self.id().is_valid()
    }
}

/// One message or a batch. Rejected input is retained for standard error replies.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RpcFrame {
    Single(RpcMessage),
    Batch(Vec<RpcMessage>),
}

impl RpcFrame {
    pub fn request(
        id: &RpcId,
        method: impl Into<String>,
        params: impl Into<Option<json::Value>>,
    ) -> Self {
        Self::Single(RpcMessage::request(id, method, params))
    }

    pub fn is_valid(&self, max_batch_members: usize) -> bool {
        match self {
            Self::Single(message) => message.is_valid(),
            Self::Batch(messages) => {
                !messages.is_empty()
                    && messages.len() <= max_batch_members
                    && messages.iter().all(|message| message.is_valid())
            }
        }
    }

    pub fn into_single(self) -> Option<RpcMessage> {
        match self {
            RpcFrame::Single(message) => Some(message),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RpcMessage {
    Request(RpcRequest),
    Notification(RpcNotification),
    Response(RpcResponse),
    #[serde(skip)]
    Invalid(ProtocolError),
}

impl RpcMessage {
    pub fn request(
        id: &RpcId,
        method: impl Into<String>,
        params: impl Into<Option<json::Value>>,
    ) -> Self {
        Self::Request(RpcRequest::new(id, method, params))
    }

    pub fn success(id: impl Into<RpcId>, result: json::Value) -> Self {
        let id = id.into();
        Self::Response(RpcResponse::success(id, result))
    }

    pub fn error(id: impl Into<RpcId>, error: impl Into<RpcError>) -> Self {
        let id = id.into();
        let error = error.into();
        Self::Response(RpcResponse::error(id, error))
    }

    pub fn from_result(
        id: impl Into<RpcId>,
        result: Result<json::Value, impl Into<RpcError>>,
    ) -> Self {
        match result {
            Ok(result) => Self::success(id, result),
            Err(error) => Self::error(id, error),
        }
    }

    pub fn into_result(self) -> Option<(RpcId, Result<json::Value, RpcError>)> {
        self.into_response().map(|response| response.into_result())
    }

    fn into_response(self) -> Option<RpcResponse> {
        match self {
            Self::Response(response) => Some(response),
            _ => None,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        match self {
            Self::Request(request) => request.is_valid(),
            Self::Notification(notification) => notification.is_valid(),
            Self::Response(response) => response.is_valid(),
            Self::Invalid(_) => false,
        }
    }
}

/// Recoverable input rejection, distinct from fatal framing/I/O errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    #[error("Parse error")]
    Parse,
    #[error("Invalid Request")]
    InvalidRequest,
}

impl ProtocolError {
    pub fn into_rpc_error(self) -> RpcError {
        match self {
            Self::Parse => RpcError::new(-32700, "Parse error"),
            Self::InvalidRequest => RpcError::new(-32600, "Invalid Request"),
        }
    }
}

impl From<ProtocolError> for RpcError {
    fn from(value: ProtocolError) -> Self {
        value.into_rpc_error()
    }
}

fn checked_id<'de, D: Deserializer<'de>>(d: D) -> Result<RpcId, D::Error> {
    let id = RpcId::deserialize(d)?;
    if id.is_valid() {
        Ok(id)
    } else {
        Err(D::Error::custom("invalid RPC ID"))
    }
}

fn params<'de, D: Deserializer<'de>>(d: D) -> Result<Option<json::Value>, D::Error> {
    let value = json::Value::deserialize(d)?;
    if is_structured(&value) {
        Ok(Some(value))
    } else {
        Err(D::Error::custom("RPC params must be an object or array"))
    }
}

fn optional_value<'de, D: Deserializer<'de>>(d: D) -> Result<Option<json::Value>, D::Error> {
    json::Value::deserialize(d).map(Some)
}

// Untagged parsing retains duplicate envelope fields. Payload objects intentionally
// follow Value's last-key-wins behavior; invalid members remain individually reportable.
#[derive(Deserialize)]
#[serde(untagged)]
enum IncomingMessage {
    Request(RpcRequest),
    Notification(RpcNotification),
    Response(RpcResponse),
    Invalid(json::Value),
}

impl From<IncomingMessage> for RpcMessage {
    fn from(message: IncomingMessage) -> Self {
        match message {
            IncomingMessage::Request(request) => Self::Request(request),
            IncomingMessage::Notification(notification) => Self::Notification(notification),
            IncomingMessage::Response(response) => Self::Response(response),
            IncomingMessage::Invalid(value) => {
                drop(value);
                Self::Invalid(ProtocolError::InvalidRequest)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum IncomingFrame {
    Batch(Vec<IncomingMessage>),
    Single(IncomingMessage),
}

pub(crate) fn parse(text: &str, max_batch_members: usize) -> RpcFrame {
    match json::from_str::<IncomingFrame>(text) {
        Ok(IncomingFrame::Single(message)) => RpcFrame::Single(message.into()),
        Ok(IncomingFrame::Batch(messages))
            if !messages.is_empty() && messages.len() <= max_batch_members =>
        {
            RpcFrame::Batch(messages.into_iter().map(Into::into).collect())
        }
        Ok(IncomingFrame::Batch(_)) => {
            RpcFrame::Single(RpcMessage::Invalid(ProtocolError::InvalidRequest))
        }
        Err(_) => RpcFrame::Single(RpcMessage::Invalid(ProtocolError::Parse)),
    }
}

fn is_structured(value: &json::Value) -> bool {
    value.is_array() | value.is_object()
}
