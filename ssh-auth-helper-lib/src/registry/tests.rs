use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::sync::Arc;

use super::*;

struct Fixture {
    _directory: tempfile::TempDir,
    _agent: UnixListener,
    _control: UnixListener,
    base: Utf8PathBuf,
    state: Arc<State>,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let base = Utf8Path::from_path(directory.path()).unwrap().to_owned();
        let paths = RuntimePaths::discover(Some(&base), None).unwrap();
        let agent = UnixListener::bind(paths.agent()).unwrap();
        let control = UnixListener::bind(paths.control()).unwrap();
        let state = Arc::new(State::new(control::hello_response(&paths)).unwrap());
        Self {
            _directory: directory,
            _agent: agent,
            _control: control,
            base,
            state,
        }
    }

    fn register(&self, request: RegisterRequest) -> Result<RegisterResponse, RegistryError> {
        self.state
            .register(request, Instant::now() + control::REQUEST_TIMEOUT)
    }

    fn unregister(&self, id: &RegistrationId) -> UnregisterResponse {
        self.state
            .unregister(id, Instant::now() + control::REQUEST_TIMEOUT)
            .unwrap()
    }

    fn request(&self, name: &str) -> RegisterRequest {
        RegisterRequest {
            socket: self.base.join(name),
            label: None,
        }
    }
}

#[test]
fn silent_sockets_and_aliases_deduplicate_without_changing_order_or_metadata() {
    let fixture = Fixture::new();
    let a = fixture.request("a");
    let b = fixture.request("b");
    let _a = UnixListener::bind(&a.socket).unwrap();
    let _b = UnixListener::bind(&b.socket).unwrap();
    let mut labeled = a.clone();
    labeled.label = Some("first laptop".into());
    let first = fixture.register(labeled).unwrap().registration;
    fixture.register(b).unwrap();
    let alias = fixture.request("alias");
    symlink(&a.socket, &alias.socket).unwrap();
    let duplicate = fixture.register(alias).unwrap().registration;
    assert_eq!(first, duplicate);
    assert_eq!(first.health, Health::Unobserved);
    assert!(first.observed_at.is_none());
    assert_eq!(fixture.state.status().registrations[0], first);
    assert_eq!(fixture.state.status().registrations.len(), 2);
    assert!(fixture.state.status().active.is_none());
    assert_eq!(fixture.state.status().active_relays, 0);
}

#[test]
fn replacement_retires_old_id_and_late_or_foreign_cleanup_preserves_new_socket() {
    let fixture = Fixture::new();
    let request = fixture.request("upstream");
    let _old = UnixListener::bind(&request.socket).unwrap();
    let old = fixture.register(request.clone()).unwrap().registration;
    fs::rename(&request.socket, fixture.base.join("old")).unwrap();
    let _new = UnixListener::bind(&request.socket).unwrap();
    let new = fixture.register(request).unwrap().registration;
    assert_ne!(old.id, new.id);
    fixture.unregister(&old.id);
    fixture.unregister(&old.id);
    fixture.unregister(&RegistrationId::new(Uuid::new_v4(), Uuid::new_v4()).unwrap());
    let status = fixture.state.status();
    assert_eq!(status.registrations.as_slice(), std::slice::from_ref(&new));
    assert_eq!(status.recent_retirements[0].registration, old);
    assert_eq!(
        status.recent_retirements[0].reason,
        RetirementReason::Replaced
    );
    fixture.unregister(&new.id);
    fixture.unregister(&new.id);
    let status = fixture.state.status();
    assert!(status.registrations.is_empty());
    assert_eq!(status.recent_retirements.len(), 2);
    assert_eq!(
        status.recent_retirements[1].reason,
        RetirementReason::Unregistered
    );
}

#[test]
fn helper_paths_symlinks_and_hardlinks_are_rejected_without_mutation() {
    let fixture = Fixture::new();
    let before = fixture.state.status();
    for (index, endpoint) in [
        fixture.state.hello.endpoints.agent.clone(),
        fixture.state.hello.endpoints.control.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        for path in [
            endpoint.clone(),
            fixture.base.join(format!("symlink{index}")),
            fixture.base.join(format!("hardlink{index}")),
        ] {
            if path != endpoint {
                if path.file_name().unwrap().starts_with("symlink") {
                    symlink(&endpoint, &path).unwrap();
                } else {
                    fs::hard_link(&endpoint, &path).unwrap();
                }
            }
            assert!(matches!(
                fixture.register(RegisterRequest {
                    socket: path,
                    label: None
                }),
                Err(RegistryError::RecursiveSocket)
            ));
            assert_eq!(fixture.state.status(), before);
        }
    }
}

#[test]
fn invalid_paths_and_labels_preserve_existing_registration() {
    let fixture = Fixture::new();
    let request = fixture.request("upstream");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    fixture.register(request.clone()).unwrap();
    fs::write(fixture.base.join("file"), "not a socket").unwrap();
    let before = fixture.state.status();
    for socket in [
        "relative".into(),
        fixture.base.join("missing"),
        fixture.base.join("file"),
        fixture.base.clone(),
        fixture.base.join("../upstream"),
        Utf8PathBuf::from("/tmp/nul\0socket"),
        fixture.base.join("line\nsocket"),
    ] {
        assert!(
            fixture
                .register(RegisterRequest {
                    socket,
                    label: None
                })
                .is_err()
        );
        assert_eq!(fixture.state.status(), before);
    }
    let mut invalid = request;
    invalid.label = Some("é".repeat(65));
    assert!(matches!(
        fixture.register(invalid),
        Err(RegistryError::LabelTooLong)
    ));
    assert_eq!(fixture.state.status(), before);
}

#[test]
fn ownership_check_rejects_socket_metadata_for_a_different_expected_uid() {
    let fixture = Fixture::new();
    let request = fixture.request("upstream");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    let metadata = fs::metadata(request.socket).unwrap();
    assert!(matches!(
        SocketIdentity::from_metadata(&metadata, metadata.uid().wrapping_add(1)),
        Err(RegistryError::UnsafeSocket)
    ));
}

#[test]
fn concurrent_registration_has_one_id_and_one_entry() {
    let fixture = Fixture::new();
    let request = fixture.request("upstream");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let state = Arc::clone(&fixture.state);
        let request = request.clone();
        tasks.push(std::thread::spawn(move || {
            state
                .register(request, Instant::now() + control::REQUEST_TIMEOUT)
                .unwrap()
                .registration
                .id
        }));
    }
    let ids: Vec<_> = tasks.into_iter().map(|task| task.join().unwrap()).collect();
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(fixture.state.status().registrations.len(), 1);
}

#[test]
fn full_registry_rejects_new_socket_but_allows_deduplication_and_replacement() {
    let fixture = Fixture::new();
    let mut listeners = Vec::new();
    for index in 0..MAX_REGISTRATIONS {
        let request = fixture.request(&format!("s{index}"));
        listeners.push(UnixListener::bind(&request.socket).unwrap());
        fixture.register(request).unwrap();
    }
    let before = fixture.state.status();
    let extra = fixture.request("extra");
    let _extra = UnixListener::bind(&extra.socket).unwrap();
    assert!(matches!(
        fixture.register(extra),
        Err(RegistryError::Capacity)
    ));
    assert_eq!(fixture.state.status(), before);
    assert_eq!(
        fixture
            .register(fixture.request("s0"))
            .unwrap()
            .registration,
        before.registrations[0]
    );
    fs::rename(fixture.base.join("s0"), fixture.base.join("old")).unwrap();
    let _replacement = UnixListener::bind(fixture.base.join("s0")).unwrap();
    let replacement = fixture
        .register(fixture.request("s0"))
        .unwrap()
        .registration;
    let status = fixture.state.status();
    assert_eq!(status.registrations.len(), MAX_REGISTRATIONS);
    assert_eq!(status.registrations.last().unwrap(), &replacement);
    assert_eq!(
        status.recent_retirements[0].registration,
        before.registrations[0]
    );
}

#[test]
fn recent_retirement_history_has_a_fixed_bound() {
    let fixture = Fixture::new();
    let request = fixture.request("upstream");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    let mut ids = Vec::new();
    for _ in 0..MAX_RETIREMENTS + 5 {
        let registration = fixture.register(request.clone()).unwrap().registration;
        fixture.unregister(&registration.id);
        ids.push(registration.id);
    }
    let status = fixture.state.status();
    assert!(status.registrations.is_empty());
    assert_eq!(status.recent_retirements.len(), MAX_RETIREMENTS);
    assert_eq!(status.recent_retirements[0].registration.id, ids[5]);
    assert_eq!(
        status.recent_retirements.last().unwrap().registration.id,
        *ids.last().unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn escaped_metadata_cannot_make_status_exceed_the_rpc_frame_bound() {
    let fixture = Fixture::new();
    let mut listeners = Vec::new();
    for index in 0..MAX_REGISTRATIONS {
        let mut request = fixture.request(&format!("{}-{index}", "s".repeat(50)));
        request.label = Some("\0".repeat(MAX_LABEL_BYTES));
        listeners.push(UnixListener::bind(&request.socket).unwrap());
        let before = fixture.state.status();
        if let Err(error) = fixture.register(request) {
            assert!(matches!(error, RegistryError::Capacity));
            assert_eq!(fixture.state.status(), before);
            break;
        }
    }
    let status = fixture.state.status();
    assert!(!status.registrations.is_empty());
    let id = jsonrpc_uds::RpcId::String("\0".repeat(jsonrpc_uds::MAX_ID_BYTES));
    let reply = jsonrpc_uds::RpcMessage::success(id, json::to_value(&status).unwrap());
    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use tokio_util::codec::Framed;
    let (writer, reader) = tokio::io::duplex(control::MAX_FRAME_BYTES + 1);
    let mut writer = Framed::new(writer, jsonrpc_uds::JsonRpcCodec::default());
    writer.send(reply).await.unwrap();
    let mut reader = Framed::new(reader, jsonrpc_uds::JsonRpcCodec::default());
    let (_, result) = reader.next().await.unwrap().unwrap().into_result().unwrap();
    assert_eq!(result.unwrap(), json::to_value(status).unwrap());
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires an isolated root container for real foreign socket ownership"]
fn foreign_owned_socket_is_rejected_without_mutation() {
    assert_eq!(rustix::process::getuid().as_raw(), 0);
    let fixture = Fixture::new();
    let request = fixture.request("foreign");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    rustix::fs::chown(
        &request.socket,
        Some(rustix::process::Uid::from_raw(1001)),
        None,
    )
    .unwrap();
    let before = fixture.state.status();
    assert!(matches!(
        fixture.register(request),
        Err(RegistryError::UnsafeSocket)
    ));
    assert_eq!(fixture.state.status(), before);
}

#[test]
fn expired_mutation_deadlines_leave_registry_unchanged() {
    let fixture = Fixture::new();
    let request = fixture.request("upstream");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    let before = fixture.state.status();
    assert!(matches!(
        fixture.state.register(request.clone(), Instant::now()),
        Err(RegistryError::Deadline)
    ));
    assert_eq!(fixture.state.status(), before);
    let registration = fixture.register(request).unwrap().registration;
    let before = fixture.state.status();
    assert!(matches!(
        fixture.state.unregister(&registration.id, Instant::now()),
        Err(RegistryError::Deadline)
    ));
    assert_eq!(fixture.state.status(), before);
}

#[test]
fn label_limit_accepts_exactly_128_utf8_bytes() {
    let fixture = Fixture::new();
    let mut request = fixture.request("upstream");
    let _upstream = UnixListener::bind(&request.socket).unwrap();
    request.label = Some("é".repeat(64));
    assert_eq!(
        fixture
            .register(request)
            .unwrap()
            .registration
            .label
            .unwrap()
            .len(),
        MAX_LABEL_BYTES
    );
}
