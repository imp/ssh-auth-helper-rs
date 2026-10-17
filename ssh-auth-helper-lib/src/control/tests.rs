use super::*;

fn hello() -> State {
    State::fixture(api::HelloResponse {
        instance: Uuid::new_v4(),
        supported_versions: vec![api::PROTOCOL_VERSION],
        ready: true,
        endpoints: api::Endpoints {
            agent: "/tmp/agent".into(),
            control: "/tmp/control".into(),
        },
    })
}

fn exchange(text: &str, hello: &State) -> Option<json::Value> {
    dispatch(
        JsonRpcCodec::default().parse(text).unwrap(),
        hello,
        Instant::now() + REQUEST_TIMEOUT,
    )
    .unwrap()
    .map(|reply| json::to_value(reply).unwrap())
}

#[test]
fn null_ids_are_preserved_and_notification_errors_are_silent() {
    let hello = hello();
    let reply = exchange(r#"{"jsonrpc":"2.0","method":"hello","id":null}"#, &hello).unwrap();
    assert!(reply["id"].is_null());
    assert_eq!(reply["result"]["ready"], true);
    for text in [
        r#"{"jsonrpc":"2.0","method":"hello"}"#,
        r#"{"jsonrpc":"2.0","method":"unknown"}"#,
        r#"{"jsonrpc":"2.0","method":"hello","params":{"unexpected":true}}"#,
    ] {
        assert!(exchange(text, &hello).is_none());
    }
}

#[test]
fn unsupported_batches_and_other_non_messages_return_one_error() {
    for text in [
        "[]",
        r#"[{"jsonrpc":"2.0","method":"hello","id":1}]"#,
        r#"[{"jsonrpc":"2.0","method":"hello"},{"jsonrpc":"2.0","method":"hello","id":1},17]"#,
        "17",
        "null",
        "true",
        r#""hello""#,
        "{}",
    ] {
        let reply = exchange(text, &hello()).unwrap();
        assert_eq!(
            reply,
            json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32600, "message": "Invalid Request"},
            }),
            "{text}"
        );
    }
}

#[test]
fn expired_dispatch_budget_prevents_execution() {
    let message = RpcMessage::request(&RpcId::Number(1), api::HELLO_METHOD, None);
    let outcome = dispatch(message, &hello(), Instant::now());
    assert!(matches!(outcome, Err(ControlError::Timeout)));
}

#[tokio::test(flavor = "current_thread")]
async fn notifications_are_silent_and_later_calls_still_work() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let task = tokio::spawn(async move { handle(server, &hello()).await });
    let mut client = Framed::new(client, JsonRpcCodec::default());
    for text in [
        r#"{"jsonrpc":"2.0","method":"hello"}"#,
        r#"{"jsonrpc":"2.0","method":"unknown"}"#,
    ] {
        let notification = client.codec().parse(text).unwrap();
        client.send(notification).await.unwrap();
    }
    client
        .send(RpcMessage::request(
            &RpcId::Number(1),
            api::HELLO_METHOD,
            None,
        ))
        .await
        .unwrap();
    use tokio::io::AsyncWriteExt;
    client.get_mut().shutdown().await.unwrap();
    let reply = timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let RpcMessage::Response(reply) = reply else {
        panic!("expected reply to call only");
    };
    assert_eq!(reply.id(), &RpcId::Number(1));
    assert!(
        timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
            .await
            .unwrap()
            .is_none()
    );
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_lines_do_not_prevent_a_later_call_on_the_same_connection() {
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::LinesCodec;

    let (client, server) = net::UnixStream::pair().unwrap();
    let task = tokio::spawn(async move { handle(server, &hello()).await });
    let mut client = Framed::new(client, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    for (text, code) in [
        (
            r#"[{"jsonrpc":"2.0","method":"hello","id":1},{"jsonrpc":"2.0","method":"hello","id":2}]"#,
            -32600,
        ),
        (
            r#"{"jsonrpc":"2.0","id":1,"method":"hello"} {"jsonrpc":"2.0","id":2,"method":"hello"}"#,
            -32700,
        ),
        ("null", -32600),
    ] {
        client.send(text.to_owned()).await.unwrap();
        let reply = timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            json::from_str::<json::Value>(&reply).unwrap(),
            json::json!({
                "jsonrpc": "2.0", "id": null,
                "error": {"code": code, "message": if code == -32600 { "Invalid Request" } else { "Parse error" }},
            })
        );
    }
    client
        .send(r#"{"jsonrpc":"2.0","id":3,"method":"hello"}"#.to_owned())
        .await
        .unwrap();
    client.get_mut().shutdown().await.unwrap();
    let reply = timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let reply: json::Value = json::from_str(&reply).unwrap();
    assert_eq!(reply["id"], 3);
    assert_eq!(reply["result"]["ready"], true);
    assert!(
        timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
            .await
            .unwrap()
            .is_none()
    );
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_reply_closes_without_queuing_a_partial_frame() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let task = tokio::spawn(async move {
        let mut hello = hello();
        hello.hello.endpoints.agent = format!("/{}", "x".repeat(MAX_FRAME_BYTES)).into();
        handle(server, &hello).await
    });
    let mut client = Framed::new(client, JsonRpcCodec::default());
    let call = client
        .codec()
        .parse(r#"{"jsonrpc":"2.0","id":1,"method":"hello"}"#)
        .unwrap();
    client.send(call).await.unwrap();
    assert!(
        timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        task.await.unwrap(),
        Err(ControlError::Transport(TransportError::TooLarge))
    ));
}
