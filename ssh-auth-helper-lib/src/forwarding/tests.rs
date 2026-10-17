use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::*;

struct Fixture {
    _directory: tempfile::TempDir,
    _agent: net::UnixListener,
    _control: net::UnixListener,
    base: Utf8PathBuf,
    state: Arc<State>,
}

impl Fixture {
    fn new(config: ForwardingConfig) -> Self {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let base = Utf8Path::from_path(directory.path()).unwrap().to_owned();
        let paths = RuntimePaths::discover(Some(&base), None).unwrap();
        let agent = net::UnixListener::bind(paths.agent()).unwrap();
        let control = net::UnixListener::bind(paths.control()).unwrap();
        let state = Arc::new(State::new(control::hello_response(&paths), config).unwrap());
        Self {
            _directory: directory,
            _agent: agent,
            _control: control,
            base,
            state,
        }
    }

    fn register(&self, name: &str) -> api::RegistrationId {
        self.state
            .register(
                api::RegisterRequest {
                    socket: self.base.join(name),
                    label: None,
                },
                Instant::now() + control::REQUEST_TIMEOUT,
            )
            .unwrap()
            .registration
            .id
    }

    fn client(&self) -> (net::UnixStream, task::JoinHandle<()>) {
        let (client, server) = net::UnixStream::pair().unwrap();
        (
            client,
            tokio::spawn(handle(server, Arc::clone(&self.state))),
        )
    }
}

// mode 0: healthy, 1: silent probe, 2: disconnect after sign, 3: slow sign.
struct Agent {
    task: task::JoinHandle<()>,
    mode: Arc<AtomicU8>,
    probes: Arc<AtomicUsize>,
    signs: Arc<AtomicUsize>,
}

impl Agent {
    fn start(path: &Utf8Path, tag: u8) -> Self {
        let listener = net::UnixListener::bind(path).unwrap();
        let mode = Arc::new(AtomicU8::new(0));
        let probes = Arc::new(AtomicUsize::new(0));
        let signs = Arc::new(AtomicUsize::new(0));
        let task_mode = Arc::clone(&mode);
        let task_probes = Arc::clone(&probes);
        let task_signs = Arc::clone(&signs);
        let task = tokio::spawn(async move {
            let mut connections = task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let mode = task_mode.load(Ordering::SeqCst);
                        let probes = Arc::clone(&task_probes);
                        let signs = Arc::clone(&task_signs);
                        connections.spawn(async move {
                            let mut request = [0; 5];
                            if stream.read_exact(&mut request).await.is_err() { return }
                            assert_eq!(request, [0,0,0,1,11]);
                            probes.fetch_add(1, Ordering::SeqCst);
                            if mode == 1 { std::future::pending::<()>().await; }
                            if stream.write_all(&[0,0,0,5,12,0,0,0,0]).await.is_err() { return }
                            let mut bound = false;
                            let mut bytes = [0; 8192];
                            loop {
                                let size = match stream.read(&mut bytes).await { Ok(0) => break, Ok(size) => size, Err(_) => return };
                                if bytes[..size].iter().all(|byte| *byte == 0xab) {
                                    if stream.write_all(&bytes[..size]).await.is_err() { return }
                                    continue;
                                }
                                for byte in &bytes[..size] {
                                    let reply = match byte {
                                        b'?' => vec![tag],
                                        b'b' => { bound = true; vec![tag] },
                                        b's' => {
                                            signs.fetch_add(1, Ordering::SeqCst);
                                            if mode == 2 { return }
                                            if mode == 3 { tokio::time::sleep(Duration::from_millis(300)).await; }
                                            vec![tag, u8::from(bound)]
                                        },
                                        byte => vec![*byte],
                                    };
                                    if stream.write_all(&reply).await.is_err() { return }
                                }
                            }
                            let _ = stream.shutdown().await;
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                }
            }
        });
        Self {
            task,
            mode,
            probes,
            signs,
        }
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn limits() -> ForwardingConfig {
    ForwardingConfig {
        probe_timeout: Duration::from_millis(60),
        setup_timeout: Duration::from_millis(200),
        retry_initial: Duration::from_millis(150),
        retry_max: Duration::from_millis(500),
        ..ForwardingConfig::default()
    }
}

async fn tag(client: &mut net::UnixStream) -> u8 {
    client.write_all(b"?").await.unwrap();
    timeout_at(Instant::now() + Duration::from_secs(2), client.read_u8())
        .await
        .unwrap()
        .unwrap()
}

async fn finish(client: net::UnixStream, task: task::JoinHandle<()>) {
    drop(client);
    timeout_at(Instant::now() + Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout_at(Instant::now() + Duration::from_secs(2), async {
        while !condition() {
            task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn identities_reply_checks_all_lengths_counts_type_and_trailing_bytes() {
    for body in [
        &[12, 0, 0, 0, 0][..],
        &[12, 0, 0, 0, 1, 0, 0, 0, 1, 0xaa, 0, 0, 0, 1, 0xbb],
    ] {
        assert!(IdentitiesAnswer::validate(body).is_ok());
    }
    for body in [
        &[][..],
        &[5],
        &[12],
        &[12, 0, 0, 0, 1],
        &[12, 0, 0, 0, 0, 0],
        &[12, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 0],
        &[12, 255, 255, 255, 255],
    ] {
        assert!(matches!(
            IdentitiesAnswer::validate(body),
            Err(api::ProbeFailure::MalformedReply)
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sticky_selection_survives_brief_b_and_silent_a_fails_over_without_preemption() {
    let fixture = Fixture::new(limits());
    let a = Agent::start(&fixture.base.join("a"), b'A');
    let aid = fixture.register("a");
    let (mut old, old_task) = fixture.client();
    assert_eq!(tag(&mut old).await, b'A');
    let b = Agent::start(&fixture.base.join("b"), b'B');
    let bid = fixture.register("b");
    let (mut client, task) = fixture.client();
    assert_eq!(tag(&mut client).await, b'A');
    finish(client, task).await;
    assert_eq!(b.probes.load(Ordering::SeqCst), 0);
    a.mode.store(1, Ordering::SeqCst);
    let (mut client, task) = fixture.client();
    assert_eq!(tag(&mut client).await, b'B');
    assert_eq!(fixture.state.status().active.as_ref(), Some(&bid));
    assert_eq!(tag(&mut old).await, b'A');
    finish(client, task).await;
    a.mode.store(0, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(170)).await;
    let (mut client, task) = fixture.client();
    assert_eq!(tag(&mut client).await, b'B');
    let status = fixture.state.status();
    assert_eq!(status.registrations[0].id, aid);
    assert_eq!(
        status.registrations[0].health,
        api::Health::TemporarilyUnavailable
    );
    assert_eq!(
        status.registrations[0].last_failure,
        Some(api::ProbeFailure::Timeout)
    );
    assert!(status.registrations[0].observed_at.is_some());
    finish(client, task).await;
    finish(old, old_task).await;
    assert_eq!(fixture.state.status().active_relays, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn pinning_keeps_session_state_and_uncertain_signs_are_never_replayed() {
    let fixture = Fixture::new(limits());
    let a = Agent::start(&fixture.base.join("a"), b'A');
    a.mode.store(2, Ordering::SeqCst);
    fixture.register("a");
    let b = Agent::start(&fixture.base.join("b"), b'B');
    fixture.register("b");
    let (mut client, task) = fixture.client();
    client.write_all(b"bs").await.unwrap();
    assert_eq!(client.read_u8().await.unwrap(), b'A');
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
    finish(client, task).await;
    assert_eq!(a.signs.load(Ordering::SeqCst), 1);
    assert_eq!(b.signs.load(Ordering::SeqCst), 0);
    a.mode.store(1, Ordering::SeqCst);
    let (mut client, task) = fixture.client();
    client.write_all(b"bs").await.unwrap();
    let mut reply = [0; 3];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [b'B', b'B', 1]);
    finish(client, task).await;
}

#[tokio::test(flavor = "current_thread")]
async fn one_stalled_probe_is_shared_by_waiters_and_management_does_not_wait() {
    let fixture = Fixture::new(limits());
    let a = Agent::start(&fixture.base.join("a"), b'A');
    a.mode.store(1, Ordering::SeqCst);
    fixture.register("a");
    let mut clients = Vec::new();
    for _ in 0..12 {
        clients.push(fixture.client());
    }
    until(|| a.probes.load(Ordering::SeqCst) == 1).await;
    assert_eq!(fixture.state.status().registrations.len(), 1);
    for (mut client, task) in clients {
        assert_eq!(
            timeout_at(
                Instant::now() + Duration::from_secs(2),
                client.read(&mut [0; 1])
            )
            .await
            .unwrap()
            .unwrap(),
            0
        );
        finish(client, task).await;
    }
    assert_eq!(a.probes.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn queue_timeout_does_not_poison_a_healthy_active_agent() {
    let fixture = Fixture::new(limits());
    let _a = Agent::start(&fixture.base.join("a"), b'A');
    let aid = fixture.register("a");
    let (mut first, first_task) = fixture.client();
    assert_eq!(tag(&mut first).await, b'A');
    finish(first, first_task).await;
    let ticket = fixture.state.next_probe().unwrap();
    let _permit = ticket.gate.acquire().await.unwrap();
    let (mut client, task) = fixture.client();
    assert_eq!(
        timeout_at(
            Instant::now() + Duration::from_secs(2),
            client.read(&mut [0; 1])
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    finish(client, task).await;
    let status = fixture.state.status();
    assert_eq!(status.active, Some(aid));
    assert_eq!(status.registrations[0].health, api::Health::Available);
    assert_eq!(status.last_setup_failure, Some(api::SetupFailure::Deadline));
}

#[tokio::test(flavor = "current_thread")]
async fn unregister_during_a_probe_cannot_restore_the_old_id() {
    let fixture = Fixture::new(limits());
    let a = Agent::start(&fixture.base.join("a"), b'A');
    a.mode.store(1, Ordering::SeqCst);
    let id = fixture.register("a");
    let (mut client, task) = fixture.client();
    until(|| a.probes.load(Ordering::SeqCst) == 1).await;
    fixture
        .state
        .unregister(&id, Instant::now() + control::REQUEST_TIMEOUT)
        .unwrap();
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
    finish(client, task).await;
    let status = fixture.state.status();
    assert!(status.active.is_none());
    assert!(status.registrations.is_empty());
    assert_eq!(
        status.recent_retirements[0].registration.health,
        api::Health::Unobserved
    );
}

#[tokio::test(flavor = "current_thread")]
async fn large_streams_half_close_and_cancellation_release_relay_counts() {
    let fixture = Fixture::new(limits());
    let _a = Agent::start(&fixture.base.join("a"), b'A');
    let id = fixture.register("a");
    let (mut client, task) = fixture.client();
    assert_eq!(tag(&mut client).await, b'A');
    assert_eq!(fixture.state.status().active_relays, 1);
    fixture
        .state
        .unregister(&id, Instant::now() + control::REQUEST_TIMEOUT)
        .unwrap();
    let (mut reader, mut writer) = client.into_split();
    let payload = vec![0xab; 256 * 1024];
    let sending = async {
        writer.write_all(&payload).await.unwrap();
        writer.shutdown().await.unwrap();
    };
    let receiving = async {
        let mut received = Vec::new();
        reader.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, payload);
    };
    tokio::join!(sending, receiving);
    task.await.unwrap();
    assert_eq!(fixture.state.status().active_relays, 0);
    let (mut client, task) = fixture.client();
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
    task.await.unwrap();
    fixture.register("a");
    let (mut client, task) = fixture.client();
    assert_eq!(tag(&mut client).await, b'A');
    task.abort();
    let _ = task.await;
    drop(client);
    assert_eq!(fixture.state.status().active_relays, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn framed_probe_failures_are_bounded_and_fragmented_replies_do_not_leak() {
    for (response, healthy) in [
        (vec![0, 0, 0, 0], false),
        (vec![0, 16, 0, 1], false),
        (vec![0, 0], false),
        (vec![0, 0, 0, 5, 12, 0], false),
        (vec![0, 0, 0, 5, 5, 0, 0, 0, 0], false),
        (vec![0, 0, 0, 6, 12, 0, 0, 0, 0, 0], false),
        (vec![0, 0, 0, 5, 12, 0, 0, 0, 0], true),
    ] {
        let fixture = Fixture::new(limits());
        let listener = net::UnixListener::bind(fixture.base.join("a")).unwrap();
        fixture.register("a");
        let fake = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 5];
            stream.read_exact(&mut request).await.unwrap();
            for byte in response {
                if stream.write_all(&[byte]).await.is_err() {
                    return;
                }
                task::yield_now().await;
            }
            if healthy {
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                stream.write_all(&byte).await.unwrap();
            }
        });
        let (mut client, task) = fixture.client();
        if healthy {
            client.write_all(&[0x80]).await.unwrap();
            assert_eq!(client.read_u8().await.unwrap(), 0x80);
        } else {
            assert_eq!(
                timeout_at(
                    Instant::now() + Duration::from_secs(2),
                    client.read(&mut [0; 1])
                )
                .await
                .unwrap()
                .unwrap(),
                0
            );
            assert_eq!(
                fixture.state.status().registrations[0].health,
                api::Health::TemporarilyUnavailable
            );
        }
        finish(client, task).await;
        fake.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn identity_change_after_connect_retires_the_old_registration() {
    let fixture = Fixture::new(limits());
    let path = fixture.base.join("a");
    let listener = net::UnixListener::bind(&path).unwrap();
    let id = fixture.register("a");
    let old = fixture.base.join("old");
    let fake = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 5];
        stream.read_exact(&mut request).await.unwrap();
        std::fs::rename(&path, old).unwrap();
        let _replacement = net::UnixListener::bind(&path).unwrap();
        stream
            .write_all(&[0, 0, 0, 5, 12, 0, 0, 0, 0])
            .await
            .unwrap();
        assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
    });
    let (mut client, task) = fixture.client();
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
    finish(client, task).await;
    fake.await.unwrap();
    let status = fixture.state.status();
    assert!(status.registrations.is_empty());
    assert!(status.active.is_none());
    assert_eq!(status.recent_retirements[0].registration.id, id);
    assert_eq!(
        status.recent_retirements[0].reason,
        ssh_auth_helper_api::RetirementReason::Replaced
    );
}

#[tokio::test(flavor = "current_thread")]
async fn backoff_allows_later_candidates_and_recovery_needs_no_reregistration() {
    let mut config = limits();
    config.probe_timeout = Duration::from_millis(40);
    config.setup_timeout = Duration::from_millis(90);
    config.retry_initial = Duration::from_millis(400);
    let fixture = Fixture::new(config);
    let mut agents = Vec::new();
    for name in ["a", "b", "c", "d"] {
        let agent = Agent::start(&fixture.base.join(name), b'X');
        agent.mode.store(1, Ordering::SeqCst);
        fixture.register(name);
        agents.push(agent);
    }
    let _live = Agent::start(&fixture.base.join("e"), b'E');
    let eid = fixture.register("e");
    let (mut first, task) = fixture.client();
    assert_eq!(first.read(&mut [0; 1]).await.unwrap(), 0);
    finish(first, task).await;
    let (mut second, task) = fixture.client();
    assert_eq!(tag(&mut second).await, b'E');
    assert_eq!(fixture.state.status().active, Some(eid));
    finish(second, task).await;

    let fixture = Fixture::new(limits());
    let a = Agent::start(&fixture.base.join("a"), b'A');
    a.mode.store(1, Ordering::SeqCst);
    let id = fixture.register("a");
    let (mut first, task) = fixture.client();
    assert_eq!(first.read(&mut [0; 1]).await.unwrap(), 0);
    finish(first, task).await;
    a.mode.store(0, Ordering::SeqCst);
    let (mut blocked, task) = fixture.client();
    assert_eq!(blocked.read(&mut [0; 1]).await.unwrap(), 0);
    finish(blocked, task).await;
    assert_eq!(a.probes.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(170)).await;
    let (mut recovered, task) = fixture.client();
    assert_eq!(tag(&mut recovered).await, b'A');
    let status = fixture.state.status();
    assert_eq!(status.active, Some(id));
    assert_eq!(status.registrations[0].health, api::Health::Available);
    assert!(status.registrations[0].last_failure.is_none());
    assert!(status.registrations[0].retry_after_ms.is_none());
    finish(recovered, task).await;
}

#[tokio::test(flavor = "current_thread")]
async fn retirement_eviction_does_not_lose_an_existing_relay_count() {
    let fixture = Fixture::new(limits());
    let _agent = Agent::start(&fixture.base.join("a"), b'A');
    let id = fixture.register("a");
    let (mut client, task) = fixture.client();
    assert_eq!(tag(&mut client).await, b'A');
    fixture
        .state
        .unregister(&id, Instant::now() + control::REQUEST_TIMEOUT)
        .unwrap();
    for _ in 0..ssh_auth_helper_api::MAX_RETIREMENTS + 1 {
        let id = fixture.register("a");
        fixture
            .state
            .unregister(&id, Instant::now() + control::REQUEST_TIMEOUT)
            .unwrap();
    }
    let status = fixture.state.status();
    assert_eq!(status.active_relays, 1);
    assert!(
        status
            .recent_retirements
            .iter()
            .all(|entry| entry.registration.id != id)
    );
    finish(client, task).await;
    assert_eq!(fixture.state.status().active_relays, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn agent_and_management_limits_are_independent_and_shutdown_releases_relays() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let base = Utf8Path::from_path(directory.path()).unwrap();
    let paths = RuntimePaths::discover(Some(&base.join("helper")), None).unwrap();
    let mut config = limits();
    config.agent_clients = 2;
    let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
    let server_paths = paths.clone();
    let server = tokio::spawn(crate::daemon::serve_until_with_config(
        server_paths,
        config,
        async {
            let _ = stopped.await;
        },
    ));
    control::wait_ready(&paths, Instant::now() + control::STARTUP_TIMEOUT)
        .await
        .unwrap();
    let _agent = Agent::start(&base.join("upstream"), b'A');
    control::register(
        &paths,
        api::RegisterRequest {
            socket: base.join("upstream"),
            label: None,
        },
    )
    .await
    .unwrap();
    let mut a = net::UnixStream::connect(paths.agent()).await.unwrap();
    assert_eq!(tag(&mut a).await, b'A');
    let mut b = net::UnixStream::connect(paths.agent()).await.unwrap();
    assert_eq!(tag(&mut b).await, b'A');
    let mut rejected = net::UnixStream::connect(paths.agent()).await.unwrap();
    assert_eq!(
        timeout_at(
            Instant::now() + Duration::from_secs(1),
            rejected.read(&mut [0; 1])
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    assert_eq!(control::status(&paths).await.unwrap().active_relays, 2);
    let mut stalled = Vec::new();
    for _ in 0..MAX_CONTROL_CLIENTS {
        let mut client = net::UnixStream::connect(paths.control()).await.unwrap();
        client.write_all(b"{").await.unwrap();
        stalled.push(client);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut extra = net::UnixStream::connect(paths.control()).await.unwrap();
    assert_eq!(
        timeout_at(
            Instant::now() + Duration::from_secs(1),
            extra.read(&mut [0; 1])
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    assert_eq!(tag(&mut a).await, b'A');
    assert_eq!(tag(&mut b).await, b'A');
    drop(stalled);
    drop(b);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(control::status(&paths).await.unwrap().active_relays, 1);
    let mut replacement = net::UnixStream::connect(paths.agent()).await.unwrap();
    assert_eq!(tag(&mut replacement).await, b'A');
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    assert_eq!(a.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(replacement.read(&mut [0; 1]).await.unwrap(), 0);
    assert!(!paths.agent().exists());
    assert!(!paths.control().exists());
}

#[tokio::test(flavor = "current_thread")]
async fn slow_signing_outlives_setup_budget_without_blocking_other_clients() {
    let fixture = Fixture::new(limits());
    let a = Agent::start(&fixture.base.join("a"), b'A');
    a.mode.store(3, Ordering::SeqCst);
    fixture.register("a");
    let (mut signing, signing_task) = fixture.client();
    assert_eq!(tag(&mut signing).await, b'A');
    signing.write_all(b"s").await.unwrap();
    until(|| a.signs.load(Ordering::SeqCst) == 1).await;
    let (mut other, other_task) = fixture.client();
    assert_eq!(tag(&mut other).await, b'A');
    finish(other, other_task).await;
    let mut answer = [0; 2];
    timeout_at(
        Instant::now() + Duration::from_secs(2),
        signing.read_exact(&mut answer),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(answer, [b'A', 0]);
    finish(signing, signing_task).await;
    assert_eq!(fixture.state.status().active_relays, 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retry_backoff_doubles_caps_and_a_success_resets_it() {
    let fixture = Fixture::new(ForwardingConfig::default());
    let _listener = net::UnixListener::bind(fixture.base.join("a")).unwrap();
    let id = fixture.register("a");
    for seconds in [1, 2, 4, 8, 16, 30, 30] {
        let ticket = fixture.state.next_probe().unwrap();
        fixture
            .state
            .probe_failed(&ticket, api::ProbeFailure::Timeout);
        let snapshot = fixture.state.status();
        assert_eq!(
            snapshot.registrations[0].retry_after_ms,
            Some(seconds * 1000)
        );
        assert!(matches!(
            fixture.state.next_probe(),
            Err(api::SetupFailure::Backoff)
        ));
        tokio::time::advance(Duration::from_secs(seconds)).await;
    }
    let ticket = fixture.state.next_probe().unwrap();
    let lease = fixture.state.pin(&ticket).unwrap();
    assert_eq!(fixture.state.status().active, Some(id));
    drop(lease);
    let ticket = fixture.state.next_probe().unwrap();
    fixture
        .state
        .probe_failed(&ticket, api::ProbeFailure::Transport);
    assert_eq!(
        fixture.state.status().registrations[0].retry_after_ms,
        Some(1000)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn missing_socket_is_retired_and_a_relay_error_is_neutral() {
    let fixture = Fixture::new(limits());
    let _listener = net::UnixListener::bind(fixture.base.join("a")).unwrap();
    let id = fixture.register("a");
    let ticket = fixture.state.next_probe().unwrap();
    let lease = fixture.state.pin(&ticket).unwrap();
    fixture.state.relay_failed(&id);
    let snapshot = fixture.state.status();
    assert_eq!(snapshot.active, Some(id.clone()));
    assert_eq!(snapshot.registrations[0].health, api::Health::Available);
    assert_eq!(snapshot.last_relay_error.unwrap().registration, id.clone());
    drop(lease);
    std::fs::remove_file(fixture.base.join("a")).unwrap();
    let (mut client, task) = fixture.client();
    assert_eq!(
        client.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    task.await.unwrap();
    let snapshot = fixture.state.status();
    assert!(snapshot.registrations.is_empty());
    assert!(snapshot.active.is_none());
    assert_eq!(snapshot.recent_retirements[0].registration.id, id);
    assert_eq!(
        snapshot.recent_retirements[0].reason,
        ssh_auth_helper_api::RetirementReason::Missing
    );
}

#[test]
fn configuration_rejects_zero_inverted_and_excessive_limits() {
    assert!(ForwardingConfig::default().validate().is_ok());
    for config in [
        ForwardingConfig {
            probe_timeout: Duration::ZERO,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            setup_timeout: Duration::from_millis(1),
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            setup_timeout: Duration::from_secs(4),
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            retry_initial: Duration::ZERO,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            retry_initial: Duration::from_secs(31),
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            retry_max: Duration::from_secs(31),
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            agent_clients: 0,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            agent_clients: 129,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            max_probe_bytes: 4,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            max_probe_bytes: 1024 * 1024 + 1,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            relay_buffer_bytes: 0,
            ..ForwardingConfig::default()
        },
        ForwardingConfig {
            relay_buffer_bytes: 64 * 1024 + 1,
            ..ForwardingConfig::default()
        },
    ] {
        assert!(config.validate().is_err(), "{config:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reregistration_discards_a_stale_successful_probe_and_uses_the_new_id() {
    let fixture = Fixture::new(limits());
    let listener = net::UnixListener::bind(fixture.base.join("a")).unwrap();
    let (observed, observing) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let fake = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        let mut probe = [0; 5];
        old.read_exact(&mut probe).await.unwrap();
        observed.send(()).unwrap();
        released.await.unwrap();
        old.write_all(&[0, 0, 0, 5, 12, 0, 0, 0, 0]).await.unwrap();
        assert_eq!(old.read(&mut [0; 1]).await.unwrap(), 0);
        let (mut new, _) = listener.accept().await.unwrap();
        new.read_exact(&mut probe).await.unwrap();
        new.write_all(&[0, 0, 0, 5, 12, 0, 0, 0, 0]).await.unwrap();
        assert_eq!(new.read_u8().await.unwrap(), b'?');
        new.write_all(b"A").await.unwrap();
    });
    let old = fixture.register("a");
    let (mut client, task) = fixture.client();
    observing.await.unwrap();
    fixture
        .state
        .unregister(&old, Instant::now() + control::REQUEST_TIMEOUT)
        .unwrap();
    let new = fixture.register("a");
    assert_ne!(old, new);
    release.send(()).unwrap();
    assert_eq!(tag(&mut client).await, b'A');
    let snapshot = fixture.state.status();
    assert_eq!(snapshot.active, Some(new));
    assert_eq!(
        snapshot.recent_retirements[0].registration.health,
        api::Health::Unobserved
    );
    finish(client, task).await;
    fake.await.unwrap();
    assert_eq!(fixture.state.status().active_relays, 0);
}
