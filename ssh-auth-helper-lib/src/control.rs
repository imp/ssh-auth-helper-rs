//! JSON-RPC readiness, basic management and a bounded reusable client.

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

pub use jsonrpc_uds::MAX_FRAME_BYTES;

use super::*;
use api::Request;

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn hello_response(paths: &RuntimePaths) -> api::HelloResponse {
    api::HelloResponse {
        instance: Uuid::new_v4(),
        supported_versions: vec![api::PROTOCOL_VERSION],
        ready: true,
        endpoints: api::Endpoints {
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
) -> Result<api::HelloResponse, ControlError> {
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
) -> Result<api::HelloResponse, ControlError> {
    let stream = timeout_at(deadline, net::UnixStream::connect(paths.control())).await??;
    runtime::authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
    let hello = query_hello(stream, deadline).await?;
    validate_hello(paths, &hello)?;
    Ok(hello)
}

fn validate_hello(paths: &RuntimePaths, hello: &api::HelloResponse) -> Result<(), ControlError> {
    if !hello.ready
        || hello.instance.is_nil()
        || !hello.supported_versions.contains(&api::PROTOCOL_VERSION)
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
) -> Result<api::HelloResponse, ControlError> {
    let mut wire = Framed::new(stream, JsonRpcCodec::default());
    query::<api::HelloRequest>(&mut wire, None, deadline).await
}

async fn query<R: api::Request>(
    wire: &mut Framed<net::UnixStream, JsonRpcCodec>,
    params: Option<json::Value>,
    deadline: Instant,
) -> Result<R::Response, ControlError> {
    let id = RpcId::String(Uuid::new_v4().to_string());
    timeout_at(
        deadline,
        wire.send(RpcMessage::request(&id, R::METHOD, params)),
    )
    .await??;
    let result = timeout_at(deadline, wire.next())
        .await?
        .ok_or(ControlError::InvalidReply)??
        .into_result()
        .and_then(|(reply_id, result)| (reply_id == id).then_some(result))
        .ok_or(ControlError::InvalidReply)?
        .map_err(ControlError::Rpc)?;
    json::from_value(result).map_err(|_| ControlError::InvalidReply)
}

/// An authenticated management connection with sequential typed operations.
///
/// Connect/hello and each operation have separate two-second budgets. Transport,
/// protocol or validation failure closes the connection; create a new client to
/// reconnect. Cancelling an in-flight operation also closes it. No request is replayed.
/// The daemon may close connections idle for two seconds.
#[derive(Debug)]
pub struct Client {
    wire: Option<Framed<net::UnixStream, JsonRpcCodec>>,
    hello: api::HelloResponse,
}

impl Client {
    /// Connect to an existing same-user daemon and validate its hello response.
    ///
    /// # Errors
    /// Reports transport, peer authorization, readiness or deadline failures.
    pub async fn connect(paths: &RuntimePaths) -> Result<Self, ControlError> {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let stream = timeout_at(deadline, net::UnixStream::connect(paths.control())).await??;
        runtime::authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
        let mut wire = Framed::new(stream, JsonRpcCodec::default());
        let hello = query::<api::HelloRequest>(&mut wire, None, deadline).await?;
        validate_hello(paths, &hello)?;
        Ok(Self {
            wire: Some(wire),
            hello,
        })
    }

    async fn call<R: api::Request>(&mut self, request: R) -> Result<R::Response, ControlError> {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let params = json::to_value(request)?;
        // The future owns the wire until a complete reply is validated. If it is
        // cancelled, the wire drops and the client stays disconnected.
        let mut wire = self.wire.take().ok_or(ControlError::Disconnected)?;
        let result = query::<R>(&mut wire, Some(params), deadline).await;
        if matches!(&result, Ok(_) | Err(ControlError::Rpc(_))) {
            self.wire = Some(wire);
        }
        result
    }

    /// Register an original upstream socket and verify the returned instance.
    ///
    /// # Errors
    /// Reports disconnection, RPC rejection, invalid response, transport or timeout.
    pub async fn register(
        &mut self,
        request: api::RegisterRequest,
    ) -> Result<api::RegisterResponse, ControlError> {
        let response = self.call(request).await?;
        if response.registration.id.instance() != self.hello.instance {
            self.wire = None;
            return Err(ControlError::InvalidReply);
        }
        Ok(response)
    }

    /// Idempotently unregister an ID without replaying an uncertain operation.
    ///
    /// # Errors
    /// Reports disconnection, RPC rejection, invalid response, transport or timeout.
    pub async fn unregister(
        &mut self,
        request: api::UnregisterRequest,
    ) -> Result<api::UnregisterResponse, ControlError> {
        self.call(request).await
    }

    /// Query metadata and verify the instance and endpoints against hello.
    ///
    /// # Errors
    /// Reports disconnection, RPC rejection, invalid response, transport or timeout.
    pub async fn status(&mut self) -> Result<api::StatusResponse, ControlError> {
        let response = self.call(api::StatusRequest {}).await?;
        if response.instance != self.hello.instance || response.endpoints != self.hello.endpoints {
            self.wire = None;
            return Err(ControlError::InvalidReply);
        }
        Ok(response)
    }
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
        api::HelloRequest::METHOD => {
            invoke::<api::HelloRequest>(params, |_| Ok(state.hello.clone()))
        }
        api::RegisterRequest::METHOD => invoke::<api::RegisterRequest>(params, |request| {
            state.register(request, deadline).map_err(Into::into)
        }),
        api::UnregisterRequest::METHOD => invoke::<api::UnregisterRequest>(params, |request| {
            state.unregister(&request.id, deadline).map_err(Into::into)
        }),
        api::StatusRequest::METHOD => invoke::<api::StatusRequest>(params, |_| Ok(state.status())),
        _ => Err(RpcError::new(-32601, "Method not found")),
    };
    Ok(id.map(|id| RpcMessage::from_result(id, result)))
}

fn invoke<R: api::Request>(
    params: json::Value,
    handler: impl FnOnce(R) -> Result<R::Response, RpcError>,
) -> Result<json::Value, RpcError> {
    let request = json::from_value(params).map_err(|_| RpcError::new(-32602, "Invalid params"))?;
    let response = handler(request)?;
    json::to_value(response).map_err(internal_error)
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
    #[error("control connection is closed; connect a new client")]
    Disconnected,
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

#[cfg(test)]
mod client_tests;
