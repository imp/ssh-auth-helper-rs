use super::*;

fn hello() -> HelloResponse {
    HelloResponse {
        instance: Uuid::new_v4(),
        supported_versions: vec![PROTOCOL_VERSION],
        ready: true,
        endpoints: Endpoints {
            agent: "/tmp/agent".into(),
            control: "/tmp/control".into(),
        },
    }
}

fn exchange(
    text: &str,
    hello: &HelloResponse,
) -> (Option<json::Value>, usize, Result<(), ControlError>) {
    let codec = JsonRpcCodec::default();
    let mut count = 0;
    let outcome = dispatch(
        codec.parse(text).unwrap(),
        &codec,
        Instant::now() + REQUEST_TIMEOUT,
        MAX_FRAME_BYTES,
        |message| {
            if matches!(
                message,
                RpcMessage::Request(_) | RpcMessage::Notification(_)
            ) {
                count += 1;
            }
            execute(message, hello)
        },
    );
    match outcome {
        Ok(reply) => (
            reply.map(|reply| json::to_value(reply).unwrap()),
            count,
            Ok(()),
        ),
        Err(error) => (None, count, Err(error)),
    }
}

#[test]
fn mixed_batches_preserve_null_ids_and_omit_notification_errors() {
    let (reply, count, outcome) = exchange(
        r#"[{"jsonrpc":"2.0","method":"unknown"},{"jsonrpc":"2.0","method":"hello","id":null},17]"#,
        &hello(),
    );
    outcome.unwrap();
    let reply = reply.unwrap();
    assert_eq!(count, 2);
    assert_eq!(reply.as_array().unwrap().len(), 2);
    assert!(reply[0]["id"].is_null());
    assert_eq!(reply[0]["result"]["ready"], true);
    assert_eq!(reply[1]["error"]["code"], -32600);
    let (reply, count, outcome) = exchange(
        r#"[{"jsonrpc":"2.0","method":"hello"},{"jsonrpc":"2.0","method":"unknown"}]"#,
        &hello(),
    );
    outcome.unwrap();
    assert_eq!(count, 2);
    assert!(reply.is_none());
}

#[test]
fn invalid_batches_do_not_execute_members() {
    let too_many = format!(
        "[{}]",
        vec![r#"{"jsonrpc":"2.0","method":"hello","id":1}"#; 33].join(",")
    );
    for text in ["[]", too_many.as_str()] {
        let (reply, count, outcome) = exchange(text, &hello());
        outcome.unwrap();
        assert_eq!(count, 0);
        assert_eq!(reply.unwrap()["error"]["code"], -32600);
    }
}

#[test]
fn oversized_aggregate_stops_execution_and_emits_no_partial_reply() {
    let codec = JsonRpcCodec::default();
    let frame = codec.parse(r#"[{"jsonrpc":"2.0","method":"hello","id":1},{"jsonrpc":"2.0","method":"hello","id":2},{"jsonrpc":"2.0","method":"hello","id":3}]"#).unwrap();
    let mut count = 0;
    let outcome = dispatch(
        frame,
        &codec,
        Instant::now() + REQUEST_TIMEOUT,
        256,
        |message| {
            count += 1;
            let RpcMessage::Request(request) = message else {
                panic!("expected request");
            };
            let result = json::Value::String("x".repeat(150));
            Ok(Some(RpcMessage::success(request.id, result)))
        },
    );
    assert!(matches!(
        outcome,
        Err(ControlError::Transport(TransportError::TooLarge))
    ));
    assert_eq!(count, 2);
}

#[test]
fn expired_dispatch_budget_prevents_execution() {
    let codec = JsonRpcCodec::default();
    let frame = codec
        .parse(r#"{"jsonrpc":"2.0","method":"hello","id":1}"#)
        .unwrap();
    let outcome = dispatch(frame, &codec, Instant::now(), MAX_FRAME_BYTES, |_| {
        panic!("must not execute")
    });
    assert!(matches!(outcome, Err(ControlError::Timeout)));
}

#[test]
fn batch_members_share_one_dispatch_budget() {
    let codec = JsonRpcCodec::default();
    let frame = codec.parse(r#"[{"jsonrpc":"2.0","method":"hello","id":1},{"jsonrpc":"2.0","method":"hello","id":2}]"#).unwrap();
    let deadline = Instant::now() + Duration::from_millis(50);
    let mut count = 0;
    let outcome = dispatch(frame, &codec, deadline, MAX_FRAME_BYTES, |_| {
        count += 1;
        std::thread::sleep(Duration::from_millis(60));
        Ok(None)
    });
    assert!(matches!(outcome, Err(ControlError::Timeout)));
    assert_eq!(count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn notification_batches_are_silent_and_later_calls_still_work() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let task = tokio::spawn(async move { handle(server, &hello()).await });
    let mut client = Framed::new(client, JsonRpcCodec::default());
    let notifications = client
        .codec()
        .parse(r#"[{"jsonrpc":"2.0","method":"hello"},{"jsonrpc":"2.0","method":"unknown"}]"#)
        .unwrap();
    client.send(notifications).await.unwrap();
    let call = client
        .codec()
        .parse(r#"{"jsonrpc":"2.0","id":1,"method":"hello"}"#)
        .unwrap();
    client.send(call).await.unwrap();
    use tokio::io::AsyncWriteExt;
    client.get_mut().shutdown().await.unwrap();
    let reply = timeout_at(Instant::now() + REQUEST_TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let RpcFrame::Single(RpcMessage::Response(reply)) = reply else {
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
async fn oversized_reply_closes_without_queuing_a_partial_frame() {
    let (client, server) = net::UnixStream::pair().unwrap();
    let task = tokio::spawn(async move {
        let mut hello = hello();
        hello.endpoints.agent = format!("/{}", "x".repeat(MAX_FRAME_BYTES)).into();
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
