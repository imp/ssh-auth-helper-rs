use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::ser::SerializeSeq;
use tokio::io::AsyncWriteExt;
use tokio::io::duplex;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_util::codec::Framed;

use super::*;

fn response(value: json::Value) -> RpcFrame {
    RpcFrame::Single(RpcMessage::Response(RpcResponse::success(
        RpcId::Number(1),
        value,
    )))
}

fn rejected(text: &str, error: ProtocolError) {
    assert!(
        matches!(JsonRpcCodec::default().parse(text).unwrap(),
        RpcFrame::Single(RpcMessage::Invalid(e)) if e == error),
        "{text}"
    );
}

#[test]
fn wire_kinds_and_null_are_distinct() {
    let codec = JsonRpcCodec::default();
    assert!(matches!(
        codec
            .parse(r#"{"jsonrpc":"2.0","method":"hello"}"#)
            .unwrap(),
        RpcFrame::Single(RpcMessage::Notification(_))
    ));
    assert!(
        matches!(codec.parse(r#"{"jsonrpc":"2.0","method":"hello","id":null}"#).unwrap(),
        RpcFrame::Single(RpcMessage::Request(request)) if request.id == RpcId::Null)
    );
    let RpcFrame::Single(RpcMessage::Response(reply)) = codec
        .parse(r#"{"jsonrpc":"2.0","id":null,"result":null}"#)
        .unwrap()
    else {
        panic!("expected response");
    };
    assert_eq!(reply.id(), &RpcId::Null);
    assert!(matches!(reply, RpcResponse::Success { result: value, .. } if value.is_null()));
}

#[test]
fn strict_envelopes_reject_versions_duplicates_unknown_fields_and_bad_params() {
    for text in [
        r#"{"id":"a","result":true}"#,
        r#"{"jsonrpc":null,"id":"a","result":true}"#,
        r#"{"jsonrpc":"3.0","id":"a","result":true}"#,
        r#"{"jsonrpc":"2.0","id":"a","result":true,"error":{"code":-32603,"message":"bad"}}"#,
        r#"{"jsonrpc":"2.0","id":"a","result":1,"result":2}"#,
        r#"{"jsonrpc":"2.0","id":"a"}"#,
        r#"{"jsonrpc":"2.0","id":"a","error":null}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":true}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":-1}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":1.5}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":""}"#,
        r#"{"jsonrpc":"3.0","method":"hello","id":"a"}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":"a","id":"b"}"#,
        r#"{"jsonrpc":"2.0","method":"hello","params":null}"#,
        r#"{"jsonrpc":"2.0","method":"hello","params":true}"#,
        r#"{"jsonrpc":"2.0","method":"hello","extra":true}"#,
        r#"{"jsonrpc":"2.0","jsonrpc":"2.0","method":"hello"}"#,
        r#"{"jsonrpc":"2.0","method":"a","method":"b"}"#,
        r#"{"jsonrpc":"2.0","method":"a","params":{},"params":{}}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":null,"result":null}"#,
        "[]",
        "17",
        "null",
    ] {
        rejected(text, ProtocolError::InvalidRequest);
    }
    for text in ["not JSON", "{", "[{", ""] {
        rejected(text, ProtocolError::Parse);
    }
}

#[test]
fn payloads_use_values_with_last_duplicate_key_winning() {
    let RpcFrame::Single(RpcMessage::Request(request)) = JsonRpcCodec::default()
        .parse(r#"{"jsonrpc":"2.0","method":"hello","id":1,"params":{"path":"a","path":"b"}}"#)
        .unwrap()
    else {
        panic!("expected request");
    };
    assert_eq!(request.params.unwrap()["path"], "b");
}

#[test]
fn batch_limits_and_invalid_members_are_preserved() {
    let codec = JsonRpcCodec::default();
    let RpcFrame::Batch(messages) = codec.parse(
        r#"[{"jsonrpc":"2.0","method":"hello"},{"jsonrpc":"2.0","id":null,"method":"hello"},17]"#
    ).unwrap() else { panic!("expected batch"); };
    assert_eq!(messages.len(), 3);
    assert!(matches!(
        &messages[2],
        RpcMessage::Invalid(ProtocolError::InvalidRequest)
    ));
    let call = r#"{"jsonrpc":"2.0","method":"hello","id":1}"#;
    assert!(matches!(
        codec
            .parse(&format!("[{}]", vec![call; 32].join(",")))
            .unwrap(),
        RpcFrame::Batch(_)
    ));
    rejected(
        &format!("[{}]", vec![call; 33].join(",")),
        ProtocolError::InvalidRequest,
    );
}

#[test]
fn string_id_limit_counts_utf8_bytes() {
    let text = |id: &str| format!(r#"{{"jsonrpc":"2.0","method":"hello","id":"{id}"}}"#);
    assert!(matches!(
        JsonRpcCodec::default()
            .parse(&text(&"é".repeat(64)))
            .unwrap(),
        RpcFrame::Single(RpcMessage::Request(_))
    ));
    rejected(
        &text(&format!("{}x", "é".repeat(64))),
        ProtocolError::InvalidRequest,
    );
}

#[test]
fn typed_encoding_round_trips_every_wire_kind() {
    for text in [
        r#"{"jsonrpc":"2.0","method":"hello"}"#,
        r#"{"jsonrpc":"2.0","method":"hello","id":null,"params":[]}"#,
        r#"{"jsonrpc":"2.0","id":"a","result":null}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid Request"}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"bad","data":{"detail":1}}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"bad","data":null}}"#,
        r#"[{"jsonrpc":"2.0","method":"hello","id":1},{"jsonrpc":"2.0","method":"event","params":{}}]"#,
    ] {
        let mut codec = JsonRpcCodec::default();
        let mut bytes = BytesMut::new();
        codec
            .encode(codec.parse(text).unwrap(), &mut bytes)
            .unwrap();
        assert_eq!(
            json::from_slice::<json::Value>(&bytes).unwrap(),
            json::from_str::<json::Value>(text).unwrap()
        );
        assert!(codec.decode(&mut bytes).unwrap().is_some());
        assert!(bytes.is_empty());
    }
}

#[test]
fn outgoing_limits_and_invalid_frames_do_not_modify_destination() {
    let text = json::to_string(&response(json::Value::String("x".repeat(40)))).unwrap();
    let mut codec = JsonRpcCodec::new(Limits {
        max_frame_bytes: text.len(),
        ..Limits::default()
    });
    let mut bytes = BytesMut::new();
    codec
        .encode(response(json::Value::String("x".repeat(40))), &mut bytes)
        .unwrap();
    assert_eq!(bytes.len(), text.len() + 1);
    let before = bytes.clone();
    assert!(matches!(
        codec.encode(response(json::Value::String("x".repeat(41))), &mut bytes),
        Err(TransportError::TooLarge)
    ));
    assert_eq!(bytes, before);
    for frame in [
        RpcFrame::Batch(vec![]),
        RpcFrame::Single(RpcMessage::Invalid(ProtocolError::Parse)),
    ] {
        assert!(matches!(
            codec.encode(frame, &mut bytes),
            Err(TransportError::InvalidFrame)
        ));
        assert_eq!(bytes, before);
    }
}

#[test]
fn bounded_writer_stops_during_serialization() {
    struct Many<'a>(&'a AtomicUsize);
    impl Serialize for Many<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut sequence = serializer.serialize_seq(None)?;
            for _ in 0..10_000 {
                self.0.fetch_add(1, Ordering::Relaxed);
                sequence.serialize_element(&0)?;
            }
            sequence.end()
        }
    }
    let count = AtomicUsize::new(0);
    assert!(matches!(
        serialize(&Many(&count), 32),
        Err(TransportError::TooLarge)
    ));
    assert!(count.load(Ordering::Relaxed) < 20);
}

#[test]
fn escaped_newlines_cannot_inject_a_frame() {
    let mut codec = JsonRpcCodec::default();
    let mut bytes = BytesMut::new();
    codec
        .encode(response(json::Value::String("a\r\nb".into())), &mut bytes)
        .unwrap();
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert!(!bytes.contains(&b'\r'));
    let RpcFrame::Single(RpcMessage::Response(reply)) = codec.decode(&mut bytes).unwrap().unwrap()
    else {
        panic!("expected response");
    };
    assert!(matches!(reply, RpcResponse::Success { result: value, .. } if value == "a\r\nb"));
}

#[tokio::test(flavor = "current_thread")]
async fn exact_limits_include_cr_and_final_eof() {
    for (size, ending, accepted) in [
        (crate::MAX_FRAME_BYTES, b"\n".as_slice(), true),
        (crate::MAX_FRAME_BYTES + 1, b"\n".as_slice(), false),
        (crate::MAX_FRAME_BYTES - 1, b"\r\n".as_slice(), true),
        (crate::MAX_FRAME_BYTES, b"\r\n".as_slice(), false),
        (crate::MAX_FRAME_BYTES, b"".as_slice(), true),
        (crate::MAX_FRAME_BYTES + 1, b"".as_slice(), false),
    ] {
        let (mut writer, reader) = duplex(1024);
        let text = format!(
            r#"{{"jsonrpc":"2.0","method":"{}"}}"#,
            "x".repeat(size - 29)
        );
        assert_eq!(text.len(), size);
        let task = tokio::spawn(async move {
            let mut bytes = text.into_bytes();
            bytes.extend_from_slice(ending);
            let _ = writer.write_all(&bytes).await;
        });
        let mut framed = Framed::new(reader, JsonRpcCodec::default());
        let result = timeout(Duration::from_secs(2), framed.next())
            .await
            .unwrap()
            .unwrap();
        if accepted {
            assert!(matches!(
                result.unwrap(),
                RpcFrame::Single(RpcMessage::Notification(_))
            ));
        } else {
            assert!(matches!(result, Err(TransportError::TooLarge)));
        }
        task.abort();
        let _ = task.await;
    }
}

#[test]
fn fragmented_coalesced_and_final_eof_frames_decode() {
    let mut codec = JsonRpcCodec::default();
    let mut bytes = BytesMut::from(&b"{\"jsonrpc\":\"2.0\","[..]);
    assert!(codec.decode(&mut bytes).unwrap().is_none());
    bytes.extend_from_slice(
        b"\"method\":\"hello\"}\r\n{\"jsonrpc\":\"2.0\",\"id\":null,\"result\":null}",
    );
    assert!(matches!(
        codec.decode(&mut bytes).unwrap(),
        Some(RpcFrame::Single(RpcMessage::Notification(_)))
    ));
    assert!(codec.decode(&mut bytes).unwrap().is_none());
    assert!(matches!(
        codec.decode_eof(&mut bytes).unwrap(),
        Some(RpcFrame::Single(RpcMessage::Response(_)))
    ));
    assert!(codec.decode_eof(&mut bytes).unwrap().is_none());
    let mut bad = BytesMut::from(&b"{\"jsonrpc\":"[..]);
    assert!(matches!(
        codec.decode_eof(&mut bad).unwrap(),
        Some(RpcFrame::Single(RpcMessage::Invalid(ProtocolError::Parse)))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn utf8_failure_and_caller_deadlines_are_bounded() {
    let (mut writer, reader) = duplex(64);
    writer.write_all(b"\xff\n").await.unwrap();
    let mut reader = Framed::new(reader, JsonRpcCodec::default());
    assert!(
        matches!(reader.next().await.unwrap(), Err(TransportError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
    );
    let (mut writer, reader) = duplex(64);
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"hello\"}")
        .await
        .unwrap();
    let mut reader = Framed::new(reader, JsonRpcCodec::default());
    assert!(
        timeout(Duration::from_millis(10), reader.next())
            .await
            .is_err()
    );
    drop(reader); // callers discard timed-out connections
    drop(writer);
    let (writer, _unread) = duplex(1);
    let mut writer = Framed::new(writer, JsonRpcCodec::default());
    assert!(
        timeout(
            Duration::from_millis(10),
            writer.send(response(json::Value::Null))
        )
        .await
        .is_err()
    );
}

#[test]
fn excessive_depth_and_invalid_outgoing_payloads_are_rejected() {
    let deep = format!(
        r#"{{"jsonrpc":"2.0","id":1,"result":{}null{}}}"#,
        "[".repeat(150),
        "]".repeat(150)
    );
    rejected(&deep, ProtocolError::Parse);
    let mut codec = JsonRpcCodec::default();
    let mut bytes = BytesMut::from(&b"existing"[..]);
    let before = bytes.clone();
    let frame = RpcFrame::Single(RpcMessage::Request(crate::RpcRequest {
        jsonrpc: crate::RpcVersion::V2,
        id: RpcId::Number(1),
        method: "hello".into(),
        params: Some(json::Value::Null),
    }));
    assert!(matches!(
        codec.encode(frame, &mut bytes),
        Err(TransportError::InvalidFrame)
    ));
    assert_eq!(bytes, before);
    let RpcFrame::Batch(messages) = codec
        .parse(&format!(
            "[{}]",
            vec![r#"{"jsonrpc":"2.0","id":1,"method":"hello"}"#; 32].join(",")
        ))
        .unwrap()
    else {
        panic!("expected batch");
    };
    let mut messages = messages;
    messages.push(RpcMessage::Notification(crate::RpcNotification {
        jsonrpc: crate::RpcVersion::V2,
        method: "event".into(),
        params: None,
    }));
    assert!(matches!(
        codec.encode(RpcFrame::Batch(messages), &mut bytes),
        Err(TransportError::InvalidFrame)
    ));
    assert_eq!(bytes, before);
}

#[test]
fn error_data_with_json_whitespace_is_encoded_as_one_compact_frame() {
    let mut codec = JsonRpcCodec::default();
    let frame = codec.parse("{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32000,\"message\":\"bad\",\"data\":{\n\"detail\":1}}}").unwrap();
    let mut bytes = BytesMut::new();
    codec.encode(frame, &mut bytes).unwrap();
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(
        json::from_slice::<json::Value>(&bytes).unwrap()["error"]["data"]["detail"],
        1
    );
}
