//! Bounded version-1 control framing and readiness. Registry operations come next.

use std::io;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio::time::timeout_at;
use uuid::Uuid;

use crate::Utf8PathBuf;
use crate::runtime::RuntimeError;
use crate::runtime::RuntimePaths;
use crate::runtime::authorize_peer_uid;

pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ID_BYTES: usize = 128;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    id: String,
    op: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Endpoints {
    pub agent: Utf8PathBuf,
    pub control: Utf8PathBuf,
}

/// A daemon's initialized listeners. Readiness does not imply an available upstream.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub instance: Uuid,
    pub supported_versions: Vec<u32>,
    pub ready: bool,
    pub endpoints: Endpoints,
}

impl Hello {
    pub(crate) fn new(paths: &RuntimePaths) -> Self {
        Self {
            instance: Uuid::new_v4(),
            supported_versions: vec![1],
            ready: true,
            endpoints: Endpoints {
                agent: paths.agent().to_owned(),
                control: paths.control().to_owned(),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProtocolError {
    code: String,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: u32,
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<Hello>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<ProtocolError>,
}

/// Query a ready same-user daemon within one absolute startup deadline.
///
/// # Errors
/// Fails closed for incompatible/invalid replies, foreign credentials and timeout.
pub async fn wait_ready(paths: &RuntimePaths, deadline: Instant) -> Result<Hello, ControlError> {
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

async fn hello_once(paths: &RuntimePaths, deadline: Instant) -> Result<Hello, ControlError> {
    let stream = timeout_at(deadline, UnixStream::connect(paths.control())).await??;
    authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
    let mut stream = BufReader::new(stream);
    let id = Uuid::new_v4().to_string();
    write_frame(
        stream.get_mut(),
        &Request {
            version: 1,
            id: id.clone(),
            op: "hello".into(),
        },
        deadline,
    )
    .await?;
    let frame = read_frame(&mut stream, deadline)
        .await?
        .ok_or(ControlError::InvalidReply)?;
    let response: Response = serde_json::from_slice(&frame)?;
    if response.version != 1 || response.id != id || response.error.is_some() {
        return Err(ControlError::InvalidReply);
    }
    let hello = response.result.ok_or(ControlError::InvalidReply)?;
    if !hello.ready
        || hello.instance.is_nil()
        || !hello.supported_versions.contains(&1)
        || hello.endpoints.agent != paths.agent()
        || hello.endpoints.control != paths.control()
    {
        return Err(ControlError::InvalidReply);
    }
    Ok(hello)
}

pub(crate) async fn handle(stream: UnixStream, hello: &Hello) -> Result<(), ControlError> {
    let mut stream = BufReader::new(stream);
    loop {
        let Some(frame) = read_frame(&mut stream, Instant::now() + REQUEST_TIMEOUT).await? else {
            return Ok(());
        };
        let request: Request = serde_json::from_slice(&frame)?;
        if request.id.is_empty() || request.id.len() > MAX_ID_BYTES {
            return Err(ControlError::InvalidRequest);
        }
        let error = if request.version != 1 {
            Some(ProtocolError {
                code: "unsupported_version".into(),
                message: "supported protocol version is 1".into(),
            })
        } else if request.op != "hello" {
            Some(ProtocolError {
                code: "unknown_operation".into(),
                message: "only hello is implemented".into(),
            })
        } else {
            None
        };
        let response = Response {
            version: 1,
            id: request.id,
            result: if error.is_none() {
                Some(hello.clone())
            } else {
                None
            },
            error,
        };
        write_frame(
            stream.get_mut(),
            &response,
            Instant::now() + REQUEST_TIMEOUT,
        )
        .await?;
    }
}

// The limit excludes the terminating LF and includes any CR. BufReader itself has
// fixed capacity; do not use an unbounded read_line/read_until before checking size.
async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    deadline: Instant,
) -> Result<Option<Vec<u8>>, ControlError> {
    timeout_at(deadline, async {
        let mut frame = Vec::new();
        loop {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return if frame.is_empty() {
                    Ok(None)
                } else {
                    Err(ControlError::Incomplete)
                };
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let count = newline.unwrap_or(available.len());
            if count > MAX_FRAME_BYTES - frame.len() {
                return Err(ControlError::TooLarge);
            }
            frame.extend_from_slice(&available[..count]);
            reader.consume(count + usize::from(newline.is_some()));
            if newline.is_some() {
                return Ok(Some(frame));
            }
        }
    })
    .await?
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    deadline: Instant,
) -> Result<(), ControlError> {
    // Callers serialize fixed-schema values with individually bounded strings.
    let mut frame = serde_json::to_vec(value)?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(ControlError::TooLarge);
    }
    frame.push(b'\n');
    timeout_at(deadline, writer.write_all(&frame)).await??;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid control JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Peer(#[from] RuntimeError),
    #[error("control deadline exceeded")]
    Timeout,
    #[error("control frame exceeds 64 KiB")]
    TooLarge,
    #[error("incomplete control frame")]
    Incomplete,
    #[error("request id must contain 1..128 UTF-8 bytes")]
    InvalidRequest,
    #[error("invalid or incompatible daemon readiness reply")]
    InvalidReply,
}

impl From<tokio::time::error::Elapsed> for ControlError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test(flavor = "current_thread")]
    async fn framing_accepts_exact_limit_and_rejects_an_extra_byte() {
        for (size, accepted) in [(MAX_FRAME_BYTES, true), (MAX_FRAME_BYTES + 1, false)] {
            let (mut writer, reader) = duplex(1024);
            let task = tokio::spawn(async move {
                let mut data = vec![b'x'; size];
                data.push(b'\n');
                let _ = writer.write_all(&data).await;
            });
            let result = read_frame(
                &mut BufReader::new(reader),
                Instant::now() + REQUEST_TIMEOUT,
            )
            .await;
            if accepted {
                assert_eq!(result.unwrap().unwrap().len(), size)
            } else {
                assert!(matches!(result, Err(ControlError::TooLarge)))
            }
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn partial_eof_and_incomplete_deadline_are_rejected() {
        let (mut writer, reader) = duplex(64);
        writer.write_all(b"partial").await.unwrap();
        drop(writer);
        assert!(matches!(
            read_frame(
                &mut BufReader::new(reader),
                Instant::now() + REQUEST_TIMEOUT
            )
            .await,
            Err(ControlError::Incomplete)
        ));
        let (_writer, reader) = duplex(64);
        assert!(matches!(
            read_frame(&mut BufReader::new(reader), Instant::now()).await,
            Err(ControlError::Timeout)
        ));
    }

    #[test]
    fn request_rejects_duplicate_and_unknown_fields() {
        for request in [
            r#"{"version":1,"id":"a","id":"b","op":"hello"}"#,
            r#"{"version":1,"id":"a","op":"hello","extra":true}"#,
        ] {
            assert!(serde_json::from_str::<Request>(request).is_err());
        }
    }
}

#[cfg(test)]
mod write_tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test(flavor = "current_thread")]
    async fn unread_response_has_a_bounded_write_deadline() {
        let (mut writer, _reader) = duplex(1);
        let result = write_frame(&mut writer, &"reply", Instant::now()).await;
        assert!(matches!(result, Err(ControlError::Timeout)));
    }
}
