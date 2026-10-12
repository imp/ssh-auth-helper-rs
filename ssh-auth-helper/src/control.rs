//! JSON-RPC readiness and helper method dispatch. Registry operations come next.

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio::time::timeout_at;
use tokio_util::codec::Framed;
use uuid::Uuid;

use jsonrpc_uds::JsonRpcCodec;
use jsonrpc_uds::Limits;
use jsonrpc_uds::ProtocolError;
use jsonrpc_uds::RpcError;
use jsonrpc_uds::RpcFrame;
use jsonrpc_uds::RpcId;
use jsonrpc_uds::RpcMessage;
use jsonrpc_uds::RpcResponse;
use jsonrpc_uds::TransportError;
use ssh_auth_protocol::HELLO_METHOD;
use ssh_auth_protocol::HelloRequest;
use ssh_auth_protocol::PROTOCOL_VERSION;

pub use jsonrpc_uds::MAX_FRAME_BYTES;
pub use ssh_auth_protocol::Endpoints;
pub use ssh_auth_protocol::HelloResponse;

use super::*;

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn hello_response(paths: &RuntimePaths) -> HelloResponse {
    HelloResponse {
        instance: Uuid::new_v4(),
        supported_versions: vec![PROTOCOL_VERSION],
        ready: true,
        endpoints: Endpoints {
            agent: paths.agent().to_owned(),
            control: paths.control().to_owned(),
        },
    }
}

/// Query a ready same-user daemon within one absolute startup deadline.
///
/// # Errors
/// Fails closed for incompatible/invalid replies, foreign credentials and timeout.
pub async fn wait_ready(
    paths: &RuntimePaths,
    deadline: Instant,
) -> Result<HelloResponse, ControlError> {
    loop {
        let attempt = deadline.min(Instant::now() + REQUEST_TIMEOUT);
        match hello_once(paths, attempt).await {
            Ok(hello) => return Ok(hello),
            Err(ControlError::Io(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) => {}
            Err(ControlError::Timeout) if Instant::now() < deadline => {}
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            return Err(ControlError::Timeout);
        }
        sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))).await;
    }
}

async fn hello_once(
    paths: &RuntimePaths,
    deadline: Instant,
) -> Result<HelloResponse, ControlError> {
    let stream = timeout_at(deadline, net::UnixStream::connect(paths.control())).await??;
    runtime::authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
    let hello = query_hello(stream, deadline).await?;
    if !hello.ready
        || hello.instance.is_nil()
        || !hello.supported_versions.contains(&PROTOCOL_VERSION)
        || hello.endpoints.agent != paths.agent()
        || hello.endpoints.control != paths.control()
    {
        return Err(ControlError::InvalidReply);
    }
    Ok(hello)
}

// Own the stream so every return, including timeout or invalid reply, closes it.
// Send and receive share the caller's absolute deadline. No replay/background task.
async fn query_hello(
    stream: net::UnixStream,
    deadline: Instant,
) -> Result<HelloResponse, ControlError> {
    let mut wire = Framed::new(stream, JsonRpcCodec::default());
    let id = RpcId::String(Uuid::new_v4().to_string());
    let frame = RpcFrame::request(&id, HELLO_METHOD, None);
    timeout_at(deadline, wire.send(frame)).await??;
    let reply = timeout_at(deadline, wire.next())
        .await?
        .ok_or(ControlError::InvalidReply)??;
    match reply {
        RpcFrame::Single(RpcMessage::Response(RpcResponse::Success {
            id: reply_id,
            result,
            ..
        })) if reply_id == id => json::from_value(result).map_err(|_| ControlError::InvalidReply),
        RpcFrame::Single(RpcMessage::Response(RpcResponse::Error {
            id: reply_id,
            error,
            ..
        })) if reply_id == id => Err(ControlError::Rpc(error)),
        _ => Err(ControlError::InvalidReply),
    }
}

fn execute(message: RpcMessage, hello: &HelloResponse) -> Result<Option<RpcMessage>, ControlError> {
    let (method, params, id) = match message {
        RpcMessage::Request(request) => (request.method, request.params, Some(request.id)),
        RpcMessage::Notification(notification) => (notification.method, notification.params, None),
        RpcMessage::Invalid(error) => {
            return Ok(Some(RpcMessage::error_response(error)));
        }
        RpcMessage::Response(_) => {
            return Ok(Some(RpcMessage::error_response(
                ProtocolError::InvalidRequest,
            )));
        }
    };
    let outcome = if method != HELLO_METHOD {
        Err(RpcError::new(-32601, "Method not found"))
    } else if json::from_value::<HelloRequest>(params.unwrap_or_else(|| json::json!({}))).is_err() {
        Err(RpcError::new(-32602, "Invalid params"))
    } else {
        json::to_value(hello).map_err(|_| RpcError::new(-32603, "Internal error"))
    };
    Ok(id.map(|id| {
        RpcMessage::Response(match outcome {
            Ok(result) => RpcResponse::success(id, result),
            Err(error) => RpcResponse::error(id, error),
        })
    }))
}

// This application-local function keeps batch policy out of the codec. The
// callback also lets fixtures verify execution counts without a server framework.
fn dispatch(
    frame: RpcFrame,
    codec: &JsonRpcCodec,
    deadline: Instant,
    limit: usize,
    mut execute: impl FnMut(RpcMessage) -> Result<Option<RpcMessage>, ControlError>,
) -> Result<Option<RpcFrame>, ControlError> {
    let (messages, batch) = match frame {
        RpcFrame::Single(message) => (vec![message], false),
        RpcFrame::Batch(messages) => (messages, true),
    };
    let mut replies = Vec::new();
    let mut bytes: usize = if batch { 2 } else { 0 }; // brackets
    for message in messages {
        if Instant::now() >= deadline {
            Err(ControlError::Timeout)?;
        }
        if let Some(reply) = execute(message)? {
            let length = codec.encoded_len(&reply)?;
            bytes = bytes
                .saturating_add(length)
                .saturating_add(usize::from(batch && !replies.is_empty()));
            if bytes > limit {
                Err(TransportError::TooLarge)?;
            }
            replies.push(reply);
        }
        if Instant::now() >= deadline {
            Err(ControlError::Timeout)?;
        }
    }
    if batch {
        Ok((!replies.is_empty()).then_some(RpcFrame::Batch(replies)))
    } else {
        Ok(replies.pop().map(RpcFrame::Single))
    }
}

pub(crate) async fn handle(
    stream: net::UnixStream,
    hello: &HelloResponse,
) -> Result<(), ControlError> {
    let limits = Limits::default();
    let mut wire = Framed::new(stream, JsonRpcCodec::new(limits));
    while let Some(frame) = timeout_at(Instant::now() + REQUEST_TIMEOUT, wire.next()).await? {
        let reply = dispatch(
            frame?,
            wire.codec(),
            Instant::now() + REQUEST_TIMEOUT,
            limits.max_frame_bytes,
            |message| execute(message, hello),
        )?;
        if let Some(reply) = reply {
            timeout_at(Instant::now() + REQUEST_TIMEOUT, wire.send(reply)).await??;
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Peer(#[from] RuntimeError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("control RPC failed: {0}")]
    Rpc(RpcError),
    #[error("control deadline exceeded")]
    Timeout,
    #[error("invalid or incompatible daemon readiness reply")]
    InvalidReply,
}

impl From<tokio::time::error::Elapsed> for ControlError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

#[cfg(test)]
mod readiness_tests;

#[cfg(test)]
mod tests;
