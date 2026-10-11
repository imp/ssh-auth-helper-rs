//! Bounded generic server dispatch. Applications implement method behavior.

use jsonrpsee_types::ErrorObjectOwned;
use jsonrpsee_types::Id;
use jsonrpsee_types::Response;
use jsonrpsee_types::ResponsePayload;
use serde_json as json;
use serde_json::value::RawValue;
use tokio::net::UnixStream;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tokio_util::codec::Framed;
use tokio_util::codec::LinesCodec;

use crate::Limits;
use crate::envelope::RpcRequest;
use crate::envelope::RpcResponse;
use crate::envelope::error_response;
use crate::envelope::parse_request;
use crate::envelope::rpc_error;
use crate::framing::TransportError;
use crate::framing::read_frame;
use crate::framing::write_frame;

/// Implement application operations after envelope validation.
pub trait Handler: Send + Sync {
    /// Execute one call or notification; notifications' results are discarded.
    fn call(
        &self,
        request: &RpcRequest,
    ) -> impl Future<Output = Result<json::Value, ErrorObjectOwned>> + Send;
}

/// Serve an already authorized stream. A frame's entire batch shares one dispatch deadline.
///
/// # Errors
/// Reports framing/I/O/deadline failures; invalid messages receive standard RPC errors.
pub async fn serve(
    stream: UnixStream,
    handler: impl Handler,
    limits: Limits,
) -> Result<(), TransportError> {
    let mut wire = Framed::new(
        stream,
        LinesCodec::new_with_max_length(limits.max_frame_bytes),
    );
    loop {
        let Some(frame) = read_frame(&mut wire, Instant::now() + limits.request_timeout).await?
        else {
            return Ok(());
        };
        let deadline = Instant::now() + limits.request_timeout;
        let response = dispatch(&frame, &handler, deadline, limits).await?;
        if let Some(response) = response {
            write_frame(
                &mut wire,
                &response,
                Instant::now() + limits.request_timeout,
            )
            .await?;
        }
    }
}

async fn dispatch(
    frame: &str,
    handler: &impl Handler,
    deadline: Instant,
    limits: Limits,
) -> Result<Option<json::Value>, TransportError> {
    let raw: Box<RawValue> = match json::from_str(frame) {
        Ok(raw) => raw,
        Err(_) => {
            return Ok(Some(json::to_value(error_response(
                Id::Null,
                rpc_error(-32700, "Parse error"),
            ))?));
        }
    };
    if !raw.get().starts_with('[') {
        return dispatch_one(&raw, handler, deadline)
            .await?
            .map(json::to_value)
            .transpose()
            .map_err(Into::into);
    }
    let entries: Vec<Box<RawValue>> = json::from_str(raw.get())?;
    if entries.is_empty() || entries.len() > limits.max_batch_members {
        return Ok(Some(json::to_value(error_response(
            Id::Null,
            rpc_error(-32600, "Invalid Request"),
        ))?));
    }
    let mut responses = Vec::new();
    for entry in entries {
        if let Some(response) = dispatch_one(&entry, handler, deadline).await? {
            responses.push(response);
            // Stop before accumulating further results after crossing the output bound.
            crate::framing::encode_frame(&responses, limits.max_frame_bytes)?;
        }
    }
    if responses.is_empty() {
        Ok(None)
    } else {
        Ok(Some(json::to_value(responses)?))
    }
}

async fn dispatch_one(
    raw: &RawValue,
    handler: &impl Handler,
    deadline: Instant,
) -> Result<Option<RpcResponse>, TransportError> {
    let request = match parse_request(raw) {
        Ok(request) => request,
        Err(error) => return Ok(Some(error_response(Id::Null, error))),
    };
    let outcome = timeout_at(deadline, handler.call(&request)).await?;
    Ok(request.id().cloned().map(|id| match outcome {
        Ok(value) => Response::new(ResponsePayload::success(value), id),
        Err(error) => error_response(id, error),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use futures_util::SinkExt;
    use tokio::io::AsyncWriteExt;
    use tokio::time::Duration;

    use super::*;

    struct Counter {
        count: Arc<AtomicUsize>,
        result: String,
    }

    impl Handler for Counter {
        async fn call(&self, request: &RpcRequest) -> Result<json::Value, ErrorObjectOwned> {
            self.count.fetch_add(1, Ordering::Relaxed);
            if request.method() == "slow" {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            if request.method() == "unknown" {
                return Err(rpc_error(-32601, "Method not found"));
            }
            Ok(json::Value::String(self.result.clone()))
        }
    }

    async fn exchange(
        frame: &str,
        limits: Limits,
        result: &str,
    ) -> (Option<json::Value>, usize, Result<(), TransportError>) {
        let (client, server) = UnixStream::pair().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let handler = Counter {
            count: Arc::clone(&count),
            result: result.into(),
        };
        let task = tokio::spawn(serve(server, handler, limits));
        let mut client = Framed::new(client, LinesCodec::new_with_max_length(64 * 1024));
        client.send(frame.to_owned()).await.unwrap();
        client.get_mut().shutdown().await.unwrap();
        let reply = read_frame(&mut client, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let outcome = task.await.unwrap();
        (
            reply.map(|frame| json::from_str(&frame).unwrap()),
            count.load(Ordering::Relaxed),
            outcome,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batches_preserve_null_ids_omit_notifications_and_reject_invalid_members() {
        let (reply, count, outcome) = exchange(
            r#"[{"jsonrpc":"2.0","method":"unknown"},{"jsonrpc":"2.0","method":"hello","id":null},17]"#,
            Limits::default(), "ok",
        ).await;
        outcome.unwrap();
        let replies = reply.unwrap();
        let replies = replies.as_array().unwrap();
        assert_eq!(count, 2);
        assert_eq!(replies.len(), 2);
        assert!(replies[0]["id"].is_null());
        assert_eq!(replies[0]["result"], "ok");
        assert_eq!(replies[1]["error"]["code"], -32600);
        let (reply, count, outcome) = exchange(
            r#"[{"jsonrpc":"2.0","method":"hello"},{"jsonrpc":"2.0","method":"unknown"}]"#,
            Limits::default(),
            "ok",
        )
        .await;
        outcome.unwrap();
        assert_eq!(count, 2);
        assert!(reply.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_and_over_capacity_batches_do_not_execute_members() {
        let too_many = format!(
            "[{}]",
            vec![r#"{"jsonrpc":"2.0","method":"hello","id":"a"}"#; 33].join(",")
        );
        for frame in ["[]", too_many.as_str()] {
            let (reply, count, outcome) = exchange(frame, Limits::default(), "ok").await;
            outcome.unwrap();
            assert_eq!(count, 0);
            assert_eq!(reply.unwrap()["error"]["code"], -32600);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aggregate_overflow_sends_no_partial_frame_or_replayed_calls() {
        let limits = Limits {
            max_frame_bytes: 256,
            ..Limits::default()
        };
        let (reply, count, outcome) = exchange(
            r#"[{"jsonrpc":"2.0","method":"hello","id":"a"},{"jsonrpc":"2.0","method":"hello","id":"b"},{"jsonrpc":"2.0","method":"hello","id":"c"}]"#,
            limits, &"x".repeat(150),
        ).await;
        assert!(matches!(outcome, Err(TransportError::TooLarge)));
        assert!(reply.is_none());
        assert_eq!(count, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_batch_shares_one_dispatch_deadline() {
        let limits = Limits {
            request_timeout: Duration::from_millis(50),
            ..Limits::default()
        };
        let (reply, count, outcome) = exchange(
            r#"[{"jsonrpc":"2.0","method":"slow","id":"a"},{"jsonrpc":"2.0","method":"slow","id":"b"},{"jsonrpc":"2.0","method":"slow","id":"c"}]"#,
            limits, "ok",
        ).await;
        assert!(matches!(outcome, Err(TransportError::Timeout)));
        assert!(reply.is_none());
        assert!(count <= 2);
    }
}
