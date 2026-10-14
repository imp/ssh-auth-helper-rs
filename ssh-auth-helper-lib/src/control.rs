//! JSON-RPC readiness, basic management and bounded single-call clients.

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio::time::timeout_at;
use tokio_util::codec::Framed;
use uuid::Uuid;

use jsonrpc_uds::JsonRpcCodec;
use jsonrpc_uds::ProtocolError;
use jsonrpc_uds::RpcError;
use jsonrpc_uds::RpcId;
use jsonrpc_uds::RpcMessage;
use jsonrpc_uds::TransportError;
use ssh_auth_helper_api::HELLO_METHOD;
use ssh_auth_helper_api::HelloRequest;
use ssh_auth_helper_api::PROTOCOL_VERSION;
use ssh_auth_helper_api::REGISTER_METHOD;
use ssh_auth_helper_api::STATUS_METHOD;
use ssh_auth_helper_api::StatusRequest;
use ssh_auth_helper_api::UNREGISTER_METHOD;

pub use ssh_auth_helper_api::RegisterRequest;
pub use ssh_auth_helper_api::RegisterResponse;
pub use ssh_auth_helper_api::RegistrationId;
pub use ssh_auth_helper_api::StatusResponse;
pub use ssh_auth_helper_api::UnregisterRequest;
pub use ssh_auth_helper_api::UnregisterResponse;

pub use jsonrpc_uds::MAX_FRAME_BYTES;
pub use ssh_auth_helper_api::Endpoints;
pub use ssh_auth_helper_api::HelloResponse;

use super::*;
use registry::State;

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
    validate_hello(paths, &hello)?;
    Ok(hello)
}

fn validate_hello(paths: &RuntimePaths, hello: &HelloResponse) -> Result<(), ControlError> {
    if !hello.ready
        || hello.instance.is_nil()
        || !hello.supported_versions.contains(&PROTOCOL_VERSION)
        || hello.endpoints.agent != paths.agent()
        || hello.endpoints.control != paths.control()
    {
        return Err(ControlError::InvalidReply);
    }
    Ok(())
}

// Own the stream so every return, including timeout or invalid reply, closes it.
// Send and receive share the caller's absolute deadline. No replay/background task.
async fn query_hello(
    stream: net::UnixStream,
    deadline: Instant,
) -> Result<HelloResponse, ControlError> {
    let mut wire = Framed::new(stream, JsonRpcCodec::default());
    let result = query(&mut wire, HELLO_METHOD, None, deadline).await?;
    json::from_value(result).map_err(|_| ControlError::InvalidReply)
}

async fn query(
    wire: &mut Framed<net::UnixStream, JsonRpcCodec>,
    method: &str,
    params: Option<json::Value>,
    deadline: Instant,
) -> Result<json::Value, ControlError> {
    let id = RpcId::String(Uuid::new_v4().to_string());
    timeout_at(
        deadline,
        wire.send(RpcMessage::request(&id, method, params)),
    )
    .await??;
    timeout_at(deadline, wire.next())
        .await?
        .ok_or(ControlError::InvalidReply)??
        .into_result()
        .and_then(|(reply_id, result)| (reply_id == id).then_some(result))
        .ok_or(ControlError::InvalidReply)?
        .map_err(ControlError::Rpc)
}

// One authenticated connection and absolute deadline. Never retry a mutating call.
struct Client {
    wire: Framed<net::UnixStream, JsonRpcCodec>,
    hello: HelloResponse,
    deadline: Instant,
}

impl Client {
    async fn connect(paths: &RuntimePaths) -> Result<Self, ControlError> {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let stream = timeout_at(deadline, net::UnixStream::connect(paths.control())).await??;
        runtime::authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
        let mut wire = Framed::new(stream, JsonRpcCodec::default());
        let result = query(&mut wire, HELLO_METHOD, None, deadline).await?;
        let hello = json::from_value(result).map_err(|_| ControlError::InvalidReply)?;
        validate_hello(paths, &hello)?;
        Ok(Self {
            wire,
            hello,
            deadline,
        })
    }

    async fn call(
        &mut self,
        method: &str,
        params: json::Value,
    ) -> Result<json::Value, ControlError> {
        query(&mut self.wire, method, Some(params), self.deadline).await
    }
}

/// Register a socket with an already running same-user foreground daemon.
///
/// # Errors
/// Reports invalid readiness, RPC rejection, transport failure or deadline expiry.
pub async fn register(
    paths: &RuntimePaths,
    request: RegisterRequest,
) -> Result<RegisterResponse, ControlError> {
    let mut client = Client::connect(paths).await?;
    let result = client
        .call(REGISTER_METHOD, json::to_value(request)?)
        .await?;
    let response: RegisterResponse =
        json::from_value(result).map_err(|_| ControlError::InvalidReply)?;
    if response.registration.id.instance() != client.hello.instance {
        return Err(ControlError::InvalidReply);
    }
    Ok(response)
}

/// Idempotently unregister an ID; never automatically replay an uncertain call.
///
/// # Errors
/// Reports readiness, RPC, transport or deadline failures.
pub async fn unregister(
    paths: &RuntimePaths,
    request: UnregisterRequest,
) -> Result<UnregisterResponse, ControlError> {
    let mut client = Client::connect(paths).await?;
    let result = client
        .call(UNREGISTER_METHOD, json::to_value(request)?)
        .await?;
    json::from_value(result).map_err(|_| ControlError::InvalidReply)
}

/// Inspect bounded registry metadata without probing any upstream socket.
///
/// # Errors
/// Reports readiness, RPC, transport or deadline failures.
pub async fn status(paths: &RuntimePaths) -> Result<StatusResponse, ControlError> {
    let mut client = Client::connect(paths).await?;
    let result = client
        .call(STATUS_METHOD, json::to_value(StatusRequest {})?)
        .await?;
    let response: StatusResponse =
        json::from_value(result).map_err(|_| ControlError::InvalidReply)?;
    if response.instance != client.hello.instance || response.endpoints != client.hello.endpoints {
        return Err(ControlError::InvalidReply);
    }
    Ok(response)
}

fn execute(
    message: RpcMessage,
    state: &State,
    deadline: Instant,
) -> Result<Option<RpcMessage>, ControlError> {
    let (method, params, id) = match message {
        RpcMessage::Request(request) => (request.method, request.params, Some(request.id)),
        RpcMessage::Notification(notification) => (notification.method, notification.params, None),
        RpcMessage::Invalid(error) => {
            return Ok(Some(RpcMessage::error(RpcId::Null, error)));
        }
        RpcMessage::Response(_) => {
            return Ok(Some(RpcMessage::error(
                RpcId::Null,
                ProtocolError::InvalidRequest,
            )));
        }
    };
    let params = params.unwrap_or_else(|| json::json!({}));
    let result = match method.as_str() {
        HELLO_METHOD => json::from_value::<HelloRequest>(params)
            .map_err(|_| RpcError::new(-32602, "Invalid params"))
            .and_then(|_| json::to_value(&state.hello).map_err(internal_error)),
        REGISTER_METHOD => json::from_value::<RegisterRequest>(params)
            .map_err(|_| RpcError::new(-32602, "Invalid params"))
            .and_then(|request| state.register(request, deadline).map_err(Into::into))
            .and_then(|response| json::to_value(response).map_err(internal_error)),
        UNREGISTER_METHOD => json::from_value::<UnregisterRequest>(params)
            .map_err(|_| RpcError::new(-32602, "Invalid params"))
            .and_then(|request| state.unregister(&request.id, deadline).map_err(Into::into))
            .and_then(|response| json::to_value(response).map_err(internal_error)),
        STATUS_METHOD => json::from_value::<StatusRequest>(params)
            .map_err(|_| RpcError::new(-32602, "Invalid params"))
            .and_then(|_| json::to_value(state.status()).map_err(internal_error)),
        _ => Err(RpcError::new(-32601, "Method not found")),
    };
    Ok(id.map(|id| RpcMessage::from_result(id, result)))
}

fn dispatch(
    message: RpcMessage,
    state: &State,
    deadline: Instant,
) -> Result<Option<RpcMessage>, ControlError> {
    if Instant::now() >= deadline {
        return Err(ControlError::Timeout);
    }
    let reply = execute(message, state, deadline)?;
    if Instant::now() >= deadline {
        return Err(ControlError::Timeout);
    }
    Ok(reply)
}

pub(crate) async fn handle(stream: net::UnixStream, state: &State) -> Result<(), ControlError> {
    let mut wire = Framed::new(stream, JsonRpcCodec::default());
    while let Some(message) = timeout_at(Instant::now() + REQUEST_TIMEOUT, wire.next()).await? {
        let reply = dispatch(message?, state, Instant::now() + REQUEST_TIMEOUT)?;
        if let Some(reply) = reply {
            timeout_at(Instant::now() + REQUEST_TIMEOUT, wire.send(reply)).await??;
        }
    }
    Ok(())
}

fn internal_error(_error: json::Error) -> RpcError {
    RpcError::new(-32603, "Internal error")
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("cannot encode management request: {0}")]
    Json(#[from] json::Error),
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
    #[error("invalid or incompatible daemon reply")]
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
