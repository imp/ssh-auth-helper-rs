//! Typed, bounded JSON-RPC frames over caller-authorized I/O.
//!
//! Use `Framed::new(stream, JsonRpcCodec::default())` as a stream/sink of
//! `RpcFrame`. The caller owns authorization, dispatch, correlation and deadlines.

use std::io;

use serde_json as json;

pub use codec::JsonRpcCodec;
pub use codec::Limits;
pub use codec::TransportError;
pub use frame::ProtocolError;
pub use frame::RpcError;
pub use frame::RpcFrame;
pub use frame::RpcId;
pub use frame::RpcMessage;
pub use frame::RpcNotification;
pub use frame::RpcRequest;
pub use frame::RpcResponse;
pub use frame::RpcVersion;

mod codec;
mod frame;

pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_MEMBERS: usize = 32;
pub const MAX_ID_BYTES: usize = 128;
