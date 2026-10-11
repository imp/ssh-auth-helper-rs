//! Bounded version-1 control framing and readiness. Registry operations come next.

use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json as json;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::UnixStream;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio::time::timeout_at;
use tokio_util::codec::Framed;
use tokio_util::codec::LinesCodec;
use tokio_util::codec::LinesCodecError;
use uuid::Uuid;

use super::*;

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

impl Request {
    const V1: u32 = 1;

    fn new(id: impl Into<String>, op: impl Into<String>) -> Self {
        Self {
            version: Self::V1,
            id: id.into(),
            op: op.into(),
        }
    }

    fn from_json(frame: &str) -> json::Result<Self> {
        json::from_str(frame)
    }

    fn from_frame(frame: &str) -> Result<Self, ControlError> {
        let request = Self::from_json(frame)?;
        if request.id.is_empty() || request.id.len() > MAX_ID_BYTES {
            Err(ControlError::InvalidRequest)
        } else {
            Ok(request)
        }
    }

    fn hello(id: Uuid) -> Self {
        Self::new(id, "hello")
    }
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
    runtime::authorize_peer_uid(&stream, rustix::process::getuid().as_raw())?;
    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    let id = Uuid::new_v4();
    let hello = Request::hello(id);
    write_frame(&mut stream, &hello, deadline).await?;
    let frame = read_frame(&mut stream, deadline)
        .await?
        .ok_or(ControlError::InvalidReply)?;
    let response: Response = json::from_str(&frame)?;
    if response.version != 1 || response.id != id.to_string() || response.error.is_some() {
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
    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    loop {
        let Some(frame) = read_frame(&mut stream, Instant::now() + REQUEST_TIMEOUT).await? else {
            return Ok(());
        };
        let request = Request::from_frame(&frame)?;
        let error = if request.version != Request::V1 {
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
        write_frame(&mut stream, &response, Instant::now() + REQUEST_TIMEOUT).await?;
    }
}

// LinesCodec limits bytes before LF (including CR), strips trailing CR, and emits
// a final unterminated line at EOF. JSON validation still rejects truncated input.
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut Framed<R, LinesCodec>,
    deadline: Instant,
) -> Result<Option<String>, ControlError> {
    Ok(timeout_at(deadline, reader.next()).await?.transpose()?)
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut Framed<W, LinesCodec>,
    value: &T,
    deadline: Instant,
) -> Result<(), ControlError> {
    // Callers serialize fixed-schema values with individually bounded strings.
    let frame = json::to_string(value)?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(ControlError::TooLarge);
    }
    timeout_at(deadline, writer.send(frame)).await??;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid control JSON: {0}")]
    Json(#[from] json::Error),
    #[error(transparent)]
    Peer(#[from] RuntimeError),
    #[error("control deadline exceeded")]
    Timeout,
    #[error("control frame exceeds 64 KiB")]
    TooLarge,
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

impl From<LinesCodecError> for ControlError {
    fn from(error: LinesCodecError) -> Self {
        match error {
            LinesCodecError::MaxLineLengthExceeded => Self::TooLarge,
            LinesCodecError::Io(error) => Self::Io(error),
        }
    }
}

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
                assert_eq!(result.unwrap().unwrap().len(), size)
            } else {
                assert!(matches!(result, Err(ControlError::TooLarge)))
            }
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn final_line_at_eof_is_validated_as_json() {
        let (mut writer, reader) = duplex(64);
        writer
            .write_all(br#"{"version":1,"id":"a","op":"hello"}"#)
            .await
            .unwrap();
        drop(writer);
        let mut reader = Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        let frame = read_frame(&mut reader, Instant::now() + REQUEST_TIMEOUT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::from_str::<Request>(&frame).unwrap().id, "a");
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
            .write_all(br#"{"version":1,"id":"a","op":"hello"}"#)
            .await
            .unwrap();
        let mut reader = Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        assert!(matches!(
            read_frame(&mut reader, Instant::now() + Duration::from_millis(10)).await,
            Err(ControlError::Timeout)
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
            Err(ControlError::Timeout)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_utf8_closes_framing() {
        let (mut writer, reader) = duplex(64);
        writer.write_all(b"\xff\n").await.unwrap();
        let mut reader = Framed::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        assert!(matches!(
            read_frame(&mut reader, Instant::now() + REQUEST_TIMEOUT).await,
            Err(ControlError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
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
    use tokio::io::AsyncReadExt;
    use tokio::io::duplex;

    #[tokio::test(flavor = "current_thread")]
    async fn unread_response_has_a_bounded_write_deadline() {
        let (writer, _reader) = duplex(1);
        let mut writer = Framed::new(writer, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        let result = write_frame(&mut writer, &"reply", Instant::now()).await;
        assert!(matches!(result, Err(ControlError::Timeout)));
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
            Err(ControlError::TooLarge)
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
