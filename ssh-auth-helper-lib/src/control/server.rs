use super::*;

/// A bounded control connection sharing daemon-owned management state.
#[derive(Debug)]
pub struct Server {
    wire: Framed<net::UnixStream, JsonRpcCodec>,
    state: Arc<State>,
}

impl Server {
    pub(crate) fn new(stream: net::UnixStream, state: Arc<State>) -> Self {
        let wire = Framed::new(stream, JsonRpcCodec::default());
        Self { wire, state }
    }

    pub(crate) async fn handle(mut self) -> Result<(), ControlError> {
        while let Some(message) =
            timeout_at(Instant::now() + REQUEST_TIMEOUT, self.wire.next()).await?
        {
            let message = message?;
            if let Some(reply) = self.execute(message, Instant::now() + REQUEST_TIMEOUT)? {
                timeout_at(Instant::now() + REQUEST_TIMEOUT, self.wire.send(reply)).await??;
            }
        }
        Ok(())
    }

    pub fn execute(
        &self,
        message: RpcMessage,
        deadline: Instant,
    ) -> Result<Option<RpcMessage>, ControlError> {
        use api::Request;

        let (method, params, id) = match message {
            RpcMessage::Request(request) => (request.method, request.params, Some(request.id)),
            RpcMessage::Notification(notification) => {
                (notification.method, notification.params, None)
            }
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
                Self::invoke::<api::HelloRequest>(params, |_| Ok(self.state.hello.clone()))
            }
            api::RegisterRequest::METHOD => {
                Self::invoke::<api::RegisterRequest>(params, |request| {
                    self.state.register(request, deadline).map_err(Into::into)
                })
            }
            api::UnregisterRequest::METHOD => {
                Self::invoke::<api::UnregisterRequest>(params, |request| {
                    self.state
                        .unregister(&request.id, deadline)
                        .map_err(Into::into)
                })
            }
            api::StatusRequest::METHOD => {
                Self::invoke::<api::StatusRequest>(params, |_| Ok(self.state.status()))
            }
            _ => Err(RpcError::new(-32601, "Method not found")),
        };
        Ok(id.map(|id| RpcMessage::from_result(id, result)))
    }

    fn invoke<R: api::Request>(
        params: json::Value,
        handler: impl FnOnce(R) -> Result<R::Response, RpcError>,
    ) -> Result<json::Value, RpcError> {
        let request =
            json::from_value(params).map_err(|_| RpcError::new(-32602, "Invalid params"))?;
        let response = handler(request)?;
        json::to_value(response).map_err(|_| RpcError::new(-32603, "Internal error"))
    }
}

#[cfg(test)]
mod tests;
