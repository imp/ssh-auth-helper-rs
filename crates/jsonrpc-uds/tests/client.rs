use std::num::NonZeroUsize;

use futures_util::SinkExt;
use jsonrpc_uds::Limits;
use jsonrpc_uds::client;
use jsonrpc_uds::client::ClientT;
use jsonrpc_uds::client::ObjectParams;
use jsonrpc_uds::client::SubscriptionClientT;
use jsonrpc_uds::framing::read_frame;
use jsonrpc_uds::framing::write_frame;
use serde_json as json;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio_util::codec::Framed;
use tokio_util::codec::LinesCodec;

async fn streams() -> (UnixStream, UnixStream, tempfile::TempDir) {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let path = directory.path().join("rpc.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let client = UnixStream::connect(&path).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server, directory)
}

fn wire(stream: UnixStream) -> Framed<UnixStream, LinesCodec> {
    Framed::new(stream, LinesCodec::new_with_max_length(64 * 1024))
}

#[tokio::test(flavor = "current_thread")]
async fn identified_calls_and_pre_registered_events_use_the_unix_adapter() {
    let (stream, server, _directory) = streams().await;
    let client = client::from_stream(stream, Limits::default(), NonZeroUsize::new(4).unwrap());
    let mut events = client
        .subscribe_to_method::<json::Value>("event")
        .await
        .unwrap();
    let fake = tokio::spawn(async move {
        let mut server = wire(server);
        let frame = read_frame(&mut server, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        let request: json::Value = json::from_str(&frame).unwrap();
        assert_eq!(request["jsonrpc"], "2.0");
        assert!(request["id"].is_string());
        // Even an early event is retained because the listener is registered first.
        write_frame(
            &mut server,
            &json::json!({"jsonrpc":"2.0","method":"event","params":{"sequence":1}}),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        write_frame(
            &mut server,
            &json::json!({"jsonrpc":"2.0","id":request["id"],"result":true}),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(
            read_frame(&mut server, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
    });
    assert!(
        client
            .request::<bool, _>("watch", ObjectParams::new())
            .await
            .unwrap()
    );
    assert_eq!(events.next().await.unwrap().unwrap()["sequence"], 1);
    drop(events);
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), fake)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_ids_and_lenient_or_ambiguous_versions_never_complete_a_call() {
    for payload in [
        r#"{"jsonrpc":"2.0","id":"wrong","result":true}"#,
        r#"{"id":ID,"result":true}"#,
        r#"{"jsonrpc":null,"id":ID,"result":true}"#,
        r#"{"jsonrpc":"3.0","id":ID,"result":true}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":true,"error":{"code":-32603,"message":"bad"}}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":true,"result":false}"#,
    ] {
        let (stream, server, _directory) = streams().await;
        let client = client::from_stream(stream, Limits::default(), NonZeroUsize::new(1).unwrap());
        let fake = tokio::spawn(async move {
            let mut server = wire(server);
            let frame = read_frame(&mut server, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .unwrap();
            let request: json::Value = json::from_str(&frame).unwrap();
            let reply = payload.replace("ID", &json::to_string(&request["id"]).unwrap());
            server.send(reply).await.unwrap();
        });
        assert!(
            client
                .request::<bool, _>("hello", ObjectParams::new())
                .await
                .is_err(),
            "{payload}"
        );
        drop(client);
        fake.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn request_timeout_does_not_replay_and_client_drop_closes_the_stream() {
    let (stream, server, _directory) = streams().await;
    let limits = Limits {
        request_timeout: Duration::from_millis(50),
        ..Limits::default()
    };
    let client = client::from_stream(stream, limits, NonZeroUsize::new(1).unwrap());
    let fake = tokio::spawn(async move {
        let mut server = wire(server);
        assert!(
            read_frame(&mut server, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_some()
        );
        // No reply. A second request here would demonstrate an unwanted retry.
        assert!(
            read_frame(&mut server, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
    });
    assert!(
        client
            .request::<bool, _>("register", ObjectParams::new())
            .await
            .is_err()
    );
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), fake)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn outgoing_client_messages_obey_the_frame_limit() {
    let (stream, server, _directory) = streams().await;
    let limits = Limits {
        max_frame_bytes: 32,
        ..Limits::default()
    };
    let client = client::from_stream(stream, limits, NonZeroUsize::new(1).unwrap());
    let fake = tokio::spawn(async move {
        assert!(
            read_frame(&mut wire(server), Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
    });
    assert!(
        client
            .request::<bool, _>("method-that-does-not-fit", ObjectParams::new())
            .await
            .is_err()
    );
    drop(client);
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn notifications_can_arrive_after_the_call_deadline_while_idle() {
    let (stream, server, _directory) = streams().await;
    let limits = Limits {
        request_timeout: Duration::from_millis(50),
        ..Limits::default()
    };
    let client = client::from_stream(stream, limits, NonZeroUsize::new(1).unwrap());
    let mut events = client
        .subscribe_to_method::<json::Value>("event")
        .await
        .unwrap();
    let fake = tokio::spawn(async move {
        let mut server = wire(server);
        let frame = read_frame(&mut server, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        let request: json::Value = json::from_str(&frame).unwrap();
        write_frame(
            &mut server,
            &json::json!({"jsonrpc":"2.0","id":request["id"],"result":true}),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        write_frame(
            &mut server,
            &json::json!({"jsonrpc":"2.0","method":"event","params":{"sequence":1}}),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(
            read_frame(&mut server, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
    });
    assert!(
        client
            .request::<bool, _>("watch", ObjectParams::new())
            .await
            .unwrap()
    );
    let event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event["sequence"], 1);
    drop(events);
    drop(client);
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_received_messages_fail_the_pending_call() {
    let (stream, server, _directory) = streams().await;
    let limits = Limits {
        max_frame_bytes: 256,
        ..Limits::default()
    };
    let client = client::from_stream(stream, limits, NonZeroUsize::new(1).unwrap());
    let fake = tokio::spawn(async move {
        let mut server = wire(server);
        let frame = read_frame(&mut server, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        let request: json::Value = json::from_str(&frame).unwrap();
        server
            .send(
                json::to_string(
                    &json::json!({"jsonrpc":"2.0","id":request["id"],"result":"x".repeat(257)}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    });
    assert!(
        client
            .request::<String, _>("hello", ObjectParams::new())
            .await
            .is_err()
    );
    drop(client);
    fake.await.unwrap();
}
