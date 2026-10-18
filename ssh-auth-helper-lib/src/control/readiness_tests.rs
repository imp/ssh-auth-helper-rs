//! Fake peers deliberately use raw lines to exercise rejected wire messages.

use tokio_util::codec::LinesCodec;

use super::*;

fn wire(stream: net::UnixStream) -> Framed<net::UnixStream, LinesCodec> {
    Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES))
}

fn hello_result() -> json::Value {
    json::json!({
        "instance": Uuid::new_v4(),
        "supported_versions": [
            api::PROTOCOL_VERSION
        ],
        "ready": true,
        "endpoints": {
            "agent": "/tmp/agent",
            "control": "/tmp/control"
        }
    })
}

async fn read_request(peer: &mut Framed<net::UnixStream, LinesCodec>) -> json::Value {
    let text = timeout_at(Instant::now() + REQUEST_TIMEOUT, peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    json::from_str(&text).unwrap()
}

async fn closed(peer: &mut Framed<net::UnixStream, LinesCodec>) {
    assert!(
        timeout_at(Instant::now() + REQUEST_TIMEOUT, peer.next())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_hello_uses_one_string_id_and_closes_after_success() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let fake = tokio::spawn(async move {
        let mut peer = wire(server);
        let request = read_request(&mut peer).await;
        assert_eq!(request["jsonrpc"], "2.0");
        assert_eq!(request["method"], api::HELLO_METHOD);
        assert!(request["id"].is_string());
        assert!(request.get("params").is_none());
        peer.send(
            json::to_string(
                &json::json!({"jsonrpc":"2.0", "id":request["id"], "result":hello_result()}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        closed(&mut peer).await;
    });
    let hello = query_hello(client, Instant::now() + REQUEST_TIMEOUT)
        .await
        .unwrap();
    assert!(hello.ready);
    assert_eq!(hello.supported_versions, [api::PROTOCOL_VERSION]);
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_or_uncorrelated_replies_are_rejected_and_not_replayed() {
    for payload in [
        r#"{"jsonrpc":"2.0","id":"wrong","result":RESULT}"#,
        r#"{"jsonrpc":"2.0","id":null,"result":RESULT}"#,
        r#"{"id":ID,"result":RESULT}"#,
        r#"{"jsonrpc":null,"id":ID,"result":RESULT}"#,
        r#"{"jsonrpc":"3.0","id":ID,"result":RESULT}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":RESULT,"error":{"code":-32603,"message":"bad"}}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":RESULT,"result":null}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":true}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":null}"#,
        r#"[{"jsonrpc":"2.0","id":ID,"result":RESULT}]"#,
        r#"{"jsonrpc":"2.0","method":"event","params":{}}"#,
        r#"{"jsonrpc":"2.0","id":ID,"method":"hello"}"#,
        "not JSON",
    ] {
        let (client, server) = net::UnixStream::pair().unwrap();
        let fake = tokio::spawn(async move {
            let mut peer = wire(server);
            let request = read_request(&mut peer).await;
            let text = payload
                .replace("ID", &json::to_string(&request["id"]).unwrap())
                .replace("RESULT", &json::to_string(&hello_result()).unwrap());
            peer.send(text).await.unwrap();
            closed(&mut peer).await;
        });
        assert!(
            matches!(
                query_hello(client, Instant::now() + REQUEST_TIMEOUT).await,
                Err(ControlError::InvalidReply)
            ),
            "{payload}"
        );
        fake.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn correlated_rpc_errors_preserve_structured_data_and_close() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let fake = tokio::spawn(async move {
        let mut peer = wire(server);
        let request = read_request(&mut peer).await;
        peer.send(
            json::to_string(&json::json!({
                "jsonrpc":"2.0", "id":request["id"],
                "error":{"code":-32000,"message":"not ready","data":{"retry":false}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        closed(&mut peer).await;
    });
    let outcome = query_hello(client, Instant::now() + REQUEST_TIMEOUT).await;
    assert!(
        matches!(outcome, Err(ControlError::Rpc(error)) if error.code == -32000 && error.data.as_ref().unwrap()["retry"] == false)
    );
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_closes_without_replaying_the_request() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let fake = tokio::spawn(async move {
        let mut peer = wire(server);
        read_request(&mut peer).await;
        // No reply: any further frame would be an unwanted retry.
        closed(&mut peer).await;
    });
    assert!(matches!(
        query_hello(client, Instant::now() + Duration::from_millis(50)).await,
        Err(ControlError::Timeout)
    ));
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_reply_uses_one_absolute_budget() {
    use tokio::io::AsyncWriteExt;
    let (client, server) = net::UnixStream::pair().unwrap();
    let fake = tokio::spawn(async move {
        let mut peer = wire(server);
        read_request(&mut peer).await;
        peer.get_mut()
            .write_all(b"{\"jsonrpc\":\"2.0\",")
            .await
            .unwrap();
        closed(&mut peer).await;
    });
    assert!(matches!(
        query_hello(client, Instant::now() + Duration::from_millis(50)).await,
        Err(ControlError::Timeout)
    ));
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_received_frame_closes_without_replay() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let fake = tokio::spawn(async move {
        let mut peer = wire(server);
        let request = read_request(&mut peer).await;
        // Client may close before the final LF has been written.
        let _ = peer
            .send(
                json::to_string(&json::json!({
                    "jsonrpc":"2.0", "id":request["id"], "result":"x".repeat(MAX_FRAME_BYTES)
                }))
                .unwrap(),
            )
            .await;
        closed(&mut peer).await;
    });
    assert!(matches!(
        query_hello(client, Instant::now() + REQUEST_TIMEOUT).await,
        Err(ControlError::Transport(TransportError::TooLarge))
    ));
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn eof_without_response_is_invalid() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let fake = tokio::spawn(async move {
        read_request(&mut wire(server)).await;
    });
    assert!(matches!(
        query_hello(client, Instant::now() + REQUEST_TIMEOUT).await,
        Err(ControlError::InvalidReply)
    ));
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn typed_calls_preserve_verb_specific_response_validation() {
    for (method, invalid) in [
        (api::REGISTER_METHOD, false),
        (api::REGISTER_METHOD, true),
        (api::STATUS_METHOD, false),
        (api::STATUS_METHOD, true),
    ] {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let base = Utf8Path::from_path(directory.path()).unwrap();
        let paths = RuntimePaths::discover(Some(base), None).unwrap();
        let listener = net::UnixListener::bind(paths.control()).unwrap();
        let hello = hello_response(&paths);
        let mut instance = hello.instance;
        if invalid && method == api::REGISTER_METHOD {
            instance = Uuid::new_v4();
        }
        let result = if method == api::REGISTER_METHOD {
            json::json!({"registration": {
                "id": api::RegistrationId::new(instance, Uuid::new_v4()).unwrap(),
                "socket": "/tmp/upstream", "registered_at": jiff::Timestamp::now(),
                "health": "unobserved", "active_relays": 0
            }})
        } else {
            let mut response = State::fixture(hello.clone()).status();
            if invalid {
                response.endpoints.agent = "/wrong/agent.sock".into();
            }
            json::to_value(response).unwrap()
        };
        let fake = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = wire(stream);
            let request = read_request(&mut peer).await;
            assert_eq!(request["method"], api::HELLO_METHOD);
            assert!(request.get("params").is_none());
            peer.send(
                json::json!({"jsonrpc":"2.0", "id":request["id"], "result":hello}).to_string(),
            )
            .await
            .unwrap();
            let request = read_request(&mut peer).await;
            assert_eq!(request["method"], method);
            if method == api::REGISTER_METHOD {
                assert_eq!(request["params"], json::json!({"socket":"/tmp/upstream"}));
            } else {
                assert_eq!(request["params"], json::json!({}));
            }
            peer.send(
                json::json!({"jsonrpc":"2.0", "id":request["id"], "result":result}).to_string(),
            )
            .await
            .unwrap();
            closed(&mut peer).await;
        });
        let outcome = if method == api::REGISTER_METHOD {
            register(
                &paths,
                api::RegisterRequest {
                    socket: "/tmp/upstream".into(),
                    label: None,
                },
            )
            .await
            .map(|_| ())
        } else {
            status(&paths).await.map(|_| ())
        };
        if invalid {
            assert!(
                matches!(outcome, Err(ControlError::InvalidReply)),
                "{method}: {outcome:?}"
            );
        } else {
            outcome.unwrap();
        }
        fake.await.unwrap();
    }
}
