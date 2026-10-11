//! jsonrpsee custom-transport client with bounded Unix framing and strict received envelopes.

use std::io;
use std::num::NonZeroUsize;

use futures_util::SinkExt;
use futures_util::StreamExt;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use jsonrpsee_core::client::ClientBuilder;
use jsonrpsee_core::client::ReceivedMessage;
use jsonrpsee_core::client::TransportReceiverT;
use jsonrpsee_core::client::TransportSenderT;
use jsonrpsee_core::middleware::RpcServiceBuilder;
use tokio::net::UnixStream;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tokio_util::codec::Framed;
use tokio_util::codec::LinesCodec;

use crate::Limits;
use crate::envelope::validate_server_frame;
use crate::framing::TransportError;

pub use jsonrpsee_core::client::ClientT;
pub use jsonrpsee_core::client::Error as ClientError;
pub use jsonrpsee_core::client::SubscriptionClientT;
pub use jsonrpsee_core::params::ObjectParams;

/// Client without the default RPC payload logging middleware.
pub type Client = jsonrpsee_core::client::Client<jsonrpsee_core::client::async_client::RpcService>;

type Wire = Framed<UnixStream, LinesCodec>;

struct Sender {
    sink: SplitSink<Wire, String>,
    max_frame_bytes: usize,
    timeout: Duration,
}

impl TransportSenderT for Sender {
    type Error = TransportError;

    async fn send(&mut self, frame: String) -> Result<(), Self::Error> {
        if frame.len() > self.max_frame_bytes {
            return Err(TransportError::TooLarge);
        }
        timeout_at(Instant::now() + self.timeout, self.sink.send(frame)).await??;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        timeout_at(Instant::now() + self.timeout, self.sink.close()).await??;
        Ok(())
    }
}

struct Receiver {
    stream: SplitStream<Wire>,
    max_batch_members: usize,
}

impl TransportReceiverT for Receiver {
    type Error = TransportError;

    async fn receive(&mut self) -> Result<ReceivedMessage, Self::Error> {
        // An idle watch stream is valid; call deadlines belong to the client engine.
        let frame = self.stream.next().await.transpose()?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "RPC peer closed the stream")
        })?;
        validate_server_frame(&frame, self.max_batch_members).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid RPC server envelope")
        })?;
        Ok(ReceivedMessage::Text(frame))
    }
}

/// Build a client from a stream whose endpoint and peer were already authorized.
///
/// Uses one in-flight call, string IDs, explicit deadlines and bounded notifications.
/// Must be called inside a Tokio runtime. Internal jsonrpsee trace logging must remain
/// disabled when installing a subscriber: it includes received RPC payloads.
pub fn from_stream(stream: UnixStream, limits: Limits, notifications: NonZeroUsize) -> Client {
    let wire = Framed::new(
        stream,
        LinesCodec::new_with_max_length(limits.max_frame_bytes),
    );
    let (sink, receiver) = wire.split();
    let sender = Sender {
        sink,
        max_frame_bytes: limits.max_frame_bytes,
        timeout: limits.request_timeout,
    };
    ClientBuilder::default()
        .set_rpc_middleware(RpcServiceBuilder::new())
        .id_format(jsonrpsee_core::client::IdKind::String)
        .request_timeout(limits.request_timeout)
        .max_concurrent_requests(1)
        .max_buffer_capacity_per_subscription(notifications.get())
        .build_with_tokio(
            sender,
            Receiver {
                stream: receiver,
                max_batch_members: limits.max_batch_members,
            },
        )
}
