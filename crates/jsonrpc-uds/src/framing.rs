//! Bounded line transport over caller-provided I/O. Endpoint authorization belongs to callers.

use std::io;

use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::Serialize;
use serde_json as json;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tokio_util::codec::Framed;
use tokio_util::codec::LinesCodec;
use tokio_util::codec::LinesCodecError;

/// Default maximum UTF-8 bytes before LF, including any framing CR.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

// LinesCodec limits bytes before LF (including CR), strips trailing CR, and emits
// a final unterminated line at EOF. JSON validation still rejects truncated input.
/// Read one bounded line by an absolute deadline. Timeout requires discarding the stream.
///
/// # Errors
/// Reports deadline, size, invalid UTF-8 or I/O failures.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut Framed<R, LinesCodec>,
    deadline: Instant,
) -> Result<Option<String>, TransportError> {
    Ok(timeout_at(deadline, reader.next()).await?.transpose()?)
}

/// Encode, write and flush one JSON line by an absolute deadline.
///
/// # Errors
/// Reports size, serialization, deadline or I/O failures. Failed sends are not retried.
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut Framed<W, LinesCodec>,
    value: &T,
    deadline: Instant,
) -> Result<(), TransportError> {
    let frame = encode_frame(value, writer.codec().max_length())?;
    timeout_at(deadline, writer.send(frame)).await??;
    Ok(())
}

/// Serialize without ever buffering more than the configured frame limit.
///
/// # Errors
/// Returns TooLarge when encoding exceeds the limit, or Json on serialization failure.
pub fn encode_frame<T: Serialize>(value: &T, limit: usize) -> Result<String, TransportError> {
    let mut buffer = BoundedBuffer {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    if let Err(error) = json::to_writer(&mut buffer, value) {
        return Err(if buffer.exceeded {
            TransportError::TooLarge
        } else {
            TransportError::Json(error)
        });
    }
    String::from_utf8(buffer.bytes)
        .map_err(|error| TransportError::Io(io::Error::new(io::ErrorKind::InvalidData, error)))
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl io::Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("RPC frame exceeds configured limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Framing and I/O failures. A timeout or write error requires discarding the stream.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("RPC I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid RPC JSON: {0}")]
    Json(#[from] json::Error),
    #[error("RPC deadline exceeded")]
    Timeout,
    #[error("RPC frame exceeds configured size limit")]
    TooLarge,
}

impl From<tokio::time::error::Elapsed> for TransportError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
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

#[cfg(test)]
use tokio::time::Duration;
#[cfg(test)]
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::io::duplex;

    #[tokio::test(flavor = "current_thread")]
    async fn framing_accepts_exact_limit_and_rejects_an_extra_byte() {
        for (size, ending, accepted) in [
            (MAX_FRAME_BYTES, b"\n".as_slice(), true),
            (MAX_FRAME_BYTES + 1, b"\n".as_slice(), false),
            (MAX_FRAME_BYTES - 1, b"\r\n".as_slice(), true),
            (MAX_FRAME_BYTES, b"\r\n".as_slice(), false),
            (MAX_FRAME_BYTES, b"".as_slice(), true),
            (MAX_FRAME_BYTES + 1, b"".as_slice(), false),
        ] {
            let (mut writer, reader) = duplex(1024);
            let task = tokio::spawn(async move {
                let mut data = vec![b'x'; size];
                data.extend_from_slice(ending);
                let _ = writer.write_all(&data).await;
            });
            let result = read_frame(
                &mut Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES)),
                Instant::now() + REQUEST_TIMEOUT,
            )
            .await;
            if accepted {
                assert_eq!(result.unwrap().unwrap().len(), size);
            } else {
                assert!(matches!(result, Err(TransportError::TooLarge)));
            }
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn final_line_at_eof_is_validated_as_json() {
        let (mut writer, reader) = duplex(64);
        writer
            .write_all(br#"{"jsonrpc":"2.0","id":"a","method":"hello"}"#)
            .await
            .unwrap();
        drop(writer);
        let mut reader = Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        let frame = read_frame(&mut reader, Instant::now() + REQUEST_TIMEOUT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<json::Value>(&frame).unwrap()["id"],
            "a"
        );
        assert!(
            read_frame(&mut reader, Instant::now() + REQUEST_TIMEOUT)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unterminated_request_on_an_open_connection_times_out() {
        let (mut writer, reader) = duplex(64);
        writer
            .write_all(br#"{"jsonrpc":"2.0","id":"a","method":"hello"}"#)
            .await
            .unwrap();
        let mut reader = Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        assert!(matches!(
            read_frame(&mut reader, Instant::now() + Duration::from_millis(10)).await,
            Err(TransportError::Timeout)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_idle_connection_has_a_bounded_read_deadline() {
        let (_writer, reader) = duplex(64);
        assert!(matches!(
            read_frame(
                &mut Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES)),
                Instant::now()
            )
            .await,
            Err(TransportError::Timeout)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_utf8_closes_framing() {
        let (mut writer, reader) = duplex(64);
        writer.write_all(b"\xff\n").await.unwrap();
        let mut reader = Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        assert!(matches!(
            read_frame(&mut reader, Instant::now() + REQUEST_TIMEOUT).await,
            Err(TransportError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }
}

#[cfg(test)]
mod write_tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::io::duplex;

    #[tokio::test(flavor = "current_thread")]
    async fn unread_response_has_a_bounded_write_deadline() {
        let (writer, _reader) = duplex(1);
        let mut writer = Framed::new(writer, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        let result = write_frame(&mut writer, &"reply", Instant::now()).await;
        assert!(matches!(result, Err(TransportError::Timeout)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn outgoing_json_has_a_limit_even_though_the_codec_encoder_does_not() {
        let (writer, mut reader) = duplex(MAX_FRAME_BYTES + 1);
        let mut writer = Framed::new(writer, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        let accepted = "x".repeat(MAX_FRAME_BYTES - 2);
        write_frame(&mut writer, &accepted, Instant::now() + REQUEST_TIMEOUT)
            .await
            .unwrap();
        let mut bytes = vec![0; MAX_FRAME_BYTES + 1];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(serde_json::from_slice::<String>(&bytes).unwrap(), accepted);
        assert!(matches!(
            write_frame(
                &mut writer,
                &"x".repeat(MAX_FRAME_BYTES - 1),
                Instant::now() + REQUEST_TIMEOUT
            )
            .await,
            Err(TransportError::TooLarge)
        ));
        assert_eq!(writer.write_buffer().len(), 0);
        assert!(
            timeout_at(
                Instant::now() + Duration::from_millis(10),
                reader.read(&mut [0])
            )
            .await
            .is_err()
        );
    }
}
