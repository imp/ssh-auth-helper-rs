//! One bounded typed codec for both reading and writing.

use bytes::BytesMut;
use serde::Serialize;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;
use tokio_util::codec::LinesCodec;
use tokio_util::codec::LinesCodecError;

use super::*;

/// Wire limits only; the caller owns all deadlines and execution policy.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_batch_members: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_batch_members: MAX_BATCH_MEMBERS,
        }
    }
}

/// Decode/encode JSON-RPC messages using the same byte and batch bounds.
///
/// Protocol rejections are yielded as `RpcMessage::Invalid` so a server can
/// reply. Framing errors are fatal: discard the connection. LF/CRLF and final
/// EOF lines follow LinesCodec semantics. Encoding errors never modify dst.
#[derive(Debug, Clone)]
pub struct JsonRpcCodec {
    lines: LinesCodec,
    limits: Limits,
}

impl Default for JsonRpcCodec {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

impl JsonRpcCodec {
    pub fn new(limits: Limits) -> Self {
        let lines = LinesCodec::new_with_max_length(limits.max_frame_bytes);
        Self { lines, limits }
    }

    /// Parse one complete JSON-RPC payload under the same limits as the stream.
    ///
    /// # Errors
    /// Rejects over-limit text; protocol failures are retained in the returned frame.
    pub fn parse(&self, text: &str) -> Result<RpcFrame, TransportError> {
        if text.len() <= self.limits.max_frame_bytes {
            Ok(frame::parse(text, self.limits.max_batch_members))
        } else {
            Err(TransportError::TooLarge)
        }
    }

    /// Measure a single outgoing message for incremental aggregate reply budgeting.
    ///
    /// # Errors
    /// Rejects invalid messages and over-limit serialization.
    pub fn encoded_len(&self, message: &RpcMessage) -> Result<usize, TransportError> {
        if message.is_valid() {
            Ok(serialize(message, self.limits.max_frame_bytes)?.len())
        } else {
            Err(TransportError::InvalidFrame)
        }
    }
}

impl Decoder for JsonRpcCodec {
    type Item = RpcFrame;
    type Error = TransportError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.lines
            .decode(src)?
            .map(|text| self.parse(&text))
            .transpose()
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.lines
            .decode_eof(src)?
            .map(|text| self.parse(&text))
            .transpose()
    }
}

impl Encoder<RpcFrame> for JsonRpcCodec {
    type Error = TransportError;

    fn encode(&mut self, frame: RpcFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if frame.is_valid(self.limits.max_batch_members) {
            let bytes = serialize(&frame, self.limits.max_frame_bytes)?;
            dst.reserve(bytes.len() + 1);
            dst.extend_from_slice(&bytes);
            dst.extend_from_slice(b"\n");
            Ok(())
        } else {
            Err(TransportError::InvalidFrame)
        }
    }
}

fn serialize(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, TransportError> {
    // Cursor over a slice cannot grow. Serialize fully before touching Framed's buffer.
    let mut bytes = vec![0; limit];
    let mut cursor = io::Cursor::new(bytes.as_mut_slice());
    json::to_writer(&mut cursor, value)?;
    let length = usize::try_from(cursor.position())?;
    bytes.truncate(length);
    Ok(bytes)
}

/// Fatal framing/I/O errors, or rejected outgoing frames. Sources omit payloads.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("RPC I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("RPC serialization failed: {0}")]
    Json(#[source] json::Error),
    #[error("RPC frame exceeds configured size limit")]
    TooLarge,
    #[error("invalid outgoing RPC frame")]
    InvalidFrame,
}

impl From<json::Error> for TransportError {
    fn from(error: json::Error) -> Self {
        if error.io_error_kind() == Some(io::ErrorKind::WriteZero) {
            Self::TooLarge
        } else {
            Self::Json(error)
        }
    }
}

impl From<LinesCodecError> for TransportError {
    fn from(error: LinesCodecError) -> Self {
        match error {
            LinesCodecError::MaxLineLengthExceeded => Self::TooLarge,
            LinesCodecError::Io(error) => Self::Io(error),
        }
    }
}

impl From<std::num::TryFromIntError> for TransportError {
    fn from(_error: std::num::TryFromIntError) -> Self {
        Self::TooLarge
    }
}

#[cfg(test)]
mod tests;
