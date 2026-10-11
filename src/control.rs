//! JSON-RPC readiness and helper method dispatch. Registry operations come next.

use std::num::NonZeroUsize;

use jsonrpc_uds::Limits;
use jsonrpc_uds::client;
use jsonrpc_uds::client::ClientError;
use jsonrpc_uds::client::ClientT;
use jsonrpc_uds::client::ObjectParams;
use jsonrpc_uds::envelope::RpcRequest;
use jsonrpc_uds::envelope::rpc_error;
use jsonrpc_uds::framing::TransportError;
use jsonrpc_uds::server::Handler;
use serde_json as json;
use ssh_auth_protocol::HELLO_METHOD;
use ssh_auth_protocol::HelloRequest;
use ssh_auth_protocol::PROTOCOL_VERSION;
use tokio::net::UnixStream;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio::time::timeout_at;
use uuid::Uuid;

use super::*;

pub use jsonrpc_uds::framing::MAX_FRAME_BYTES;
pub use ssh_auth_protocol::Endpoints;
pub use ssh_auth_protocol::HelloResponse;

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
    let stream = timeout_at(deadline, UnixStream::connect(paths.control())).await??;
    runtime::authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
    let rpc = client::from_stream(stream, Limits::default(), NonZeroUsize::MIN);
    let hello: HelloResponse =
        timeout_at(deadline, rpc.request(HELLO_METHOD, ObjectParams::new())).await??;
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

struct HelloHandler<'a>(&'a HelloResponse);

impl Handler for HelloHandler<'_> {
    async fn call(
        &self,
        request: &RpcRequest,
    ) -> Result<json::Value, jsonrpc_uds::ErrorObjectOwned> {
        if request.method() != HELLO_METHOD {
            return Err(rpc_error(-32601, "Method not found"));
        }
        let _: HelloRequest = request.params()?;
        json::to_value(self.0).map_err(|_| rpc_error(-32603, "Internal error"))
    }
}

pub(crate) async fn handle(stream: UnixStream, hello: &HelloResponse) -> Result<(), ControlError> {
    jsonrpc_uds::server::serve(stream, HelloHandler(hello), Limits::default()).await?;
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
    Rpc(ClientError),
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

impl From<ClientError> for ControlError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::RequestTimeout => Self::Timeout,
            error => Self::Rpc(error),
        }
    }
}
