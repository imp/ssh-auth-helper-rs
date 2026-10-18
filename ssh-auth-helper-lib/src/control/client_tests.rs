//! Reuse, per-operation deadlines and fail-closed ownership of a management connection.

use super::*;
use readiness_tests::closed;
use readiness_tests::read_request;
use readiness_tests::wire;
use tokio_util::codec::LinesCodec;

type Peer = Framed<net::UnixStream, LinesCodec>;

fn pair() -> (Client, Peer) {
    let (client, peer) = net::UnixStream::pair().unwrap();
    let paths = RuntimePaths::discover(Some(Utf8Path::new("/tmp")), None).unwrap();
    (
        Client {
            wire: Some(Framed::new(client, JsonRpcCodec::default())),
            hello: hello_response(&paths),
        },
        wire(peer),
    )
}

fn unregister_request(client: &Client) -> api::UnregisterRequest {
    api::UnregisterRequest {
        id: api::RegistrationId::new(client.hello.instance, Uuid::new_v4()).unwrap(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_methods_reuse_one_connection_for_all_verbs() {
    let directory = camino_tempfile::tempdir_in("/tmp").unwrap();
    let base = directory.path();
    let paths = RuntimePaths::discover(Some(&base.join("helper")), None).unwrap();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(crate::daemon::serve_until(paths.clone(), async {
        stopped.await.unwrap();
    }));
    wait_ready(&paths, Instant::now() + STARTUP_TIMEOUT)
        .await
        .unwrap();
    let _upstream = net::UnixListener::bind(base.join("upstream")).unwrap();
    let mut client = Client::connect(&paths).await.unwrap();
    let registration = client
        .register(api::RegisterRequest {
            socket: base.join("upstream"),
            label: None,
        })
        .await
        .unwrap()
        .registration;
    assert_eq!(
        client.status().await.unwrap().registrations.as_slice(),
        std::slice::from_ref(&registration)
    );
    for _ in 0..2 {
        client
            .unregister(api::UnregisterRequest {
                id: registration.id.clone(),
            })
            .await
            .unwrap();
    }
    assert!(client.status().await.unwrap().registrations.is_empty());
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    assert!(client.status().await.is_err());
    assert!(matches!(
        client.status().await,
        Err(ControlError::Disconnected)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn each_call_gets_a_fresh_budget_and_rpc_errors_allow_reuse() {
    let (mut client, mut peer) = pair();
    let request = unregister_request(&client);
    let expected = json::to_value(&request).unwrap();
    let fake = tokio::spawn(async move {
        let mut previous_id = json::Value::Null;
        for index in 0..3 {
            let request = read_request(&mut peer).await;
            assert_eq!(request["method"], api::UNREGISTER_METHOD);
            assert_eq!(request["params"], expected);
            assert_ne!(request["id"], previous_id);
            previous_id = request["id"].clone();
            tokio::time::sleep(Duration::from_millis(900)).await;
            let reply = if index == 0 {
                json::json!({"jsonrpc":"2.0", "id":request["id"],
                    "error":{"code":-32000,"message":"rejected","data":{"retry":false}}})
            } else {
                json::json!({"jsonrpc":"2.0", "id":request["id"], "result":{}})
            };
            peer.send(reply.to_string()).await.unwrap();
        }
        closed(&mut peer).await;
    });
    let start = Instant::now();
    let outcome = client.unregister(request.clone()).await;
    assert!(
        matches!(&outcome,
        Err(ControlError::Rpc(error)) if error.code == -32000 && error.data.as_ref().unwrap()["retry"] == false),
        "{outcome:?}"
    );
    client.unregister(request.clone()).await.unwrap();
    client.unregister(request).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(2700));
    drop(client);
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_replies_and_eof_close_the_client_without_replay() {
    for payload in [
        r#"{"jsonrpc":"2.0","id":"wrong","result":{}}"#,
        r#"{"jsonrpc":"3.0","id":ID,"result":{}}"#,
        r#"{"jsonrpc":"2.0","id":ID,"result":true}"#,
        "not JSON",
        "eof",
    ] {
        let (mut client, mut peer) = pair();
        let request = unregister_request(&client);
        let fake = tokio::spawn(async move {
            let request = read_request(&mut peer).await;
            if payload == "eof" {
                return;
            }
            peer.send(payload.replace("ID", &request["id"].to_string()))
                .await
                .unwrap();
            closed(&mut peer).await;
        });
        assert!(
            matches!(
                client.unregister(request).await,
                Err(ControlError::InvalidReply)
            ),
            "{payload}"
        );
        assert!(matches!(
            client.status().await,
            Err(ControlError::Disconnected)
        ));
        fake.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn timed_out_call_closes_the_client_and_cannot_be_replayed() {
    let (mut client, mut peer) = pair();
    let request = unregister_request(&client);
    let fake = tokio::spawn(async move {
        read_request(&mut peer).await;
        assert!(
            timeout_at(Instant::now() + REQUEST_TIMEOUT * 2, peer.next())
                .await
                .unwrap()
                .is_none()
        );
    });
    assert!(matches!(
        client.unregister(request).await,
        Err(ControlError::Timeout)
    ));
    assert!(matches!(
        client.status().await,
        Err(ControlError::Disconnected)
    ));
    fake.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_call_drops_the_connection_and_cannot_be_replayed() {
    let (mut client, mut peer) = pair();
    let request = unregister_request(&client);
    let (observed, observing) = tokio::sync::oneshot::channel();
    let fake = tokio::spawn(async move {
        read_request(&mut peer).await;
        observed.send(()).unwrap();
        closed(&mut peer).await;
    });
    {
        let operation = client.unregister(request);
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => panic!("unexpected completion: {result:?}"),
            result = observing => result.unwrap(),
        }
    }
    assert!(matches!(
        client.status().await,
        Err(ControlError::Disconnected)
    ));
    fake.await.unwrap();
}
