use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use camino::Utf8Path;
use camino::Utf8PathBuf;
use serde_json as json;
use ssh_auth_helper_lib::control;
use ssh_auth_helper_lib::runtime::RuntimeDirectory;
use ssh_auth_helper_lib::runtime::RuntimeError;
use ssh_auth_helper_lib::runtime::RuntimeLock;
use ssh_auth_helper_lib::runtime::RuntimePaths;
use ssh_auth_helper_lib::runtime::RuntimeSockets;
use tokio::net;
use tokio::time::Instant;

fn fixture() -> (tempfile::TempDir, RuntimePaths) {
    let parent = tempfile::tempdir_in("/tmp").unwrap();
    let path = Utf8PathBuf::from_path_buf(parent.path().join("helper")).unwrap();
    let paths = RuntimePaths::discover(Some(&path), None).unwrap();
    (parent, paths)
}

fn mode(path: &Utf8Path, value: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(value)).unwrap();
}

struct Server(Child);

impl Server {
    fn spawn(paths: &RuntimePaths) -> Self {
        Self(
            Command::new(env!("CARGO_BIN_EXE_ssh-auth-helper"))
                .arg("--runtime-dir")
                .arg(paths.directory())
                .arg("serve")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }

    async fn ready(&mut self, paths: &RuntimePaths) -> control::HelloResponse {
        let result = control::wait_ready(paths, Instant::now() + Duration::from_secs(5)).await;
        if result.is_err() {
            let _ = self.0.kill();
            let _ = self.0.wait();
            let mut error = String::new();
            self.0
                .stderr
                .as_mut()
                .unwrap()
                .read_to_string(&mut error)
                .unwrap();
            panic!("server readiness failed: {result:?}; {error}");
        }
        result.unwrap()
    }

    fn stop(&mut self, success: bool) {
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(self.0.id()).unwrap()).unwrap(),
            rustix::process::Signal::TERM,
        )
        .unwrap();
        let status = self.0.wait().unwrap();
        assert_eq!(status.success(), success, "{status}");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn foreground_readiness_private_modes_and_orderly_cleanup() {
    let (_parent, paths) = fixture();
    let mut server = Server::spawn(&paths);
    let first = server.ready(&paths).await;
    assert!(first.ready);
    assert_eq!(
        first,
        control::wait_ready(&paths, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
    );
    for (path, expected) in [
        (paths.directory(), 0o700),
        (paths.agent(), 0o600),
        (paths.control(), 0o600),
        (paths.lock(), 0o600),
    ] {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert_eq!(metadata.mode() & 0o7777, expected);
        assert_eq!(metadata.uid(), rustix::process::getuid().as_raw());
    }
    let inode = fs::metadata(paths.lock()).unwrap().ino();
    let mut agent = UnixStream::connect(paths.agent()).unwrap();
    agent
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(agent.read(&mut [0]).unwrap(), 0);
    server.stop(true);
    assert!(!paths.agent().exists());
    assert!(!paths.control().exists());
    assert_eq!(fs::metadata(paths.lock()).unwrap().ino(), inode);
}

#[tokio::test(flavor = "current_thread")]
async fn crash_restart_recovers_sockets_and_preserves_lock_contents() {
    let (_parent, paths) = fixture();
    let mut server = Server::spawn(&paths);
    let first = server.ready(&paths).await;
    fs::write(paths.lock(), b"preserved contents").unwrap();
    let inode = fs::metadata(paths.lock()).unwrap().ino();
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    assert!(paths.agent().exists() && paths.control().exists());
    let mut restarted = Server::spawn(&paths);
    let next = restarted.ready(&paths).await;
    assert_ne!(first.instance, next.instance);
    assert_eq!(fs::metadata(paths.lock()).unwrap().ino(), inode);
    assert_eq!(fs::read(paths.lock()).unwrap(), b"preserved contents");
    restarted.stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_processes_have_one_ready_owner() {
    let (_parent, paths) = fixture();
    let mut servers: Vec<_> = (0..8).map(|_| Server::spawn(&paths)).collect();
    let ready = control::wait_ready(&paths, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let pid = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let running: Vec<_> = servers
                .iter_mut()
                .filter_map(|server| match server.0.try_wait().unwrap() {
                    None => Some(server.0.id()),
                    Some(status) => {
                        assert!(!status.success());
                        None
                    }
                })
                .collect();
            if running.len() == 1 {
                break running[0];
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        ready.instance,
        control::wait_ready(&paths, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .instance
    );
    servers
        .iter_mut()
        .find(|server| server.0.id() == pid)
        .unwrap()
        .stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn held_lock_without_sockets_blocks_a_process_and_release_allows_start() {
    let (_parent, paths) = fixture();
    let held = RuntimeLock::acquire(RuntimeDirectory::prepare(paths.clone()).unwrap()).unwrap();
    let mut contender = Server::spawn(&paths);
    assert!(!contender.0.wait().unwrap().success());
    assert!(!paths.agent().exists() && !paths.control().exists());
    drop(held);
    let mut server = Server::spawn(&paths);
    server.ready(&paths).await;
    server.stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn live_or_silent_listener_preserves_both_endpoints() {
    for live_name in ["agent.sock", "control.sock"] {
        let (_parent, paths) = fixture();
        RuntimeDirectory::prepare(paths.clone()).unwrap();
        let agent = UnixListener::bind(paths.agent()).unwrap();
        let control = UnixListener::bind(paths.control()).unwrap();
        for path in [paths.agent(), paths.control()] {
            mode(path, 0o600);
        }
        let before = [
            fs::metadata(paths.agent()).unwrap().ino(),
            fs::metadata(paths.control()).unwrap().ino(),
        ];
        let live = if live_name == "agent.sock" {
            drop(control);
            agent
        } else {
            drop(agent);
            control
        };
        let mut contender = Server::spawn(&paths);
        assert!(!contender.0.wait().unwrap().success());
        assert_eq!(
            before,
            [
                fs::metadata(paths.agent()).unwrap().ino(),
                fs::metadata(paths.control()).unwrap().ino()
            ]
        );
        // The recovery probe connects but sends no protocol payload.
        let (mut stream, _) = live.accept().unwrap();
        // The child has exited, so its probe cannot retain the connection. Darwin
        // may reject SO_RCVTIMEO on this already-disconnected accepted socket.
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_preserves_replacement_file_socket_and_symlink() {
    for kind in ["file", "socket", "symlink"] {
        let (_parent, paths) = fixture();
        let mut server = Server::spawn(&paths);
        server.ready(&paths).await;
        fs::remove_file(paths.agent()).unwrap();
        let replacement = match kind {
            "file" => {
                fs::write(paths.agent(), b"keep").unwrap();
                None
            }
            "socket" => {
                let listener = UnixListener::bind(paths.agent()).unwrap();
                mode(paths.agent(), 0o600);
                Some(listener)
            }
            "symlink" => {
                symlink("missing-target", paths.agent()).unwrap();
                None
            }
            _ => unreachable!(),
        };
        let before = fs::symlink_metadata(paths.agent()).unwrap();
        server.stop(false); // Cleanup reports that it preserved an unexpected replacement.
        let after = fs::symlink_metadata(paths.agent()).unwrap();
        assert_eq!((before.ino(), before.mode()), (after.ino(), after.mode()));
        assert!(!paths.control().exists());
        drop(replacement);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_uses_original_directory_after_rename() {
    let (_parent, paths) = fixture();
    let mut server = Server::spawn(&paths);
    server.ready(&paths).await;
    let moved = paths.directory().with_file_name("moved");
    fs::rename(paths.directory(), &moved).unwrap();
    fs::create_dir(paths.directory()).unwrap();
    mode(paths.directory(), 0o700);
    fs::write(paths.agent(), b"replacement directory").unwrap();
    server.stop(true);
    assert!(!moved.join("agent.sock").exists());
    assert!(!moved.join("control.sock").exists());
    assert!(moved.join("daemon.lock").exists());
    assert_eq!(fs::read(paths.agent()).unwrap(), b"replacement directory");
}

#[tokio::test(flavor = "current_thread")]
async fn changed_directory_and_lock_identity_abort_before_binding() {
    for change_lock in [false, true] {
        let (_parent, paths) = fixture();
        let lock = RuntimeLock::acquire(RuntimeDirectory::prepare(paths.clone()).unwrap()).unwrap();
        if change_lock {
            fs::rename(paths.lock(), paths.lock().with_file_name("old.lock")).unwrap();
            fs::write(paths.lock(), b"replacement").unwrap();
            mode(paths.lock(), 0o600);
        } else {
            fs::rename(paths.directory(), paths.directory().with_file_name("moved")).unwrap();
            fs::create_dir(paths.directory()).unwrap();
            mode(paths.directory(), 0o700);
        }
        assert!(
            RuntimeSockets::bind(lock, Instant::now() + Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(!paths.agent().exists() && !paths.control().exists());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn expired_startup_budget_does_not_bind() {
    let (_parent, paths) = fixture();
    let lock = RuntimeLock::acquire(RuntimeDirectory::prepare(paths.clone()).unwrap()).unwrap();
    assert!(matches!(
        RuntimeSockets::bind(lock, Instant::now()).await,
        Err(RuntimeError::Deadline(_))
    ));
    assert!(!paths.agent().exists() && !paths.control().exists());
}

fn request(paths: &RuntimePaths, value: &str) -> Option<json::Value> {
    let mut stream = UnixStream::connect(paths.control()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream.write_all(value.as_bytes()).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut line = String::new();
    if BufReader::new(stream).read_line(&mut line).unwrap_or(0) == 0 {
        None
    } else {
        Some(json_value(&line).unwrap())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn control_errors_and_stalled_peers_do_not_block_readiness() {
    let (_parent, paths) = fixture();
    let mut server = Server::spawn(&paths);
    let hello = server.ready(&paths).await;
    for (input, code) in [
        (
            "{\"jsonrpc\":\"3.0\",\"id\":\"v\",\"method\":\"hello\"}\n",
            -32600,
        ),
        (
            "{\"jsonrpc\":\"2.0\",\"id\":\"o\",\"method\":\"register\"}\n",
            -32601,
        ),
    ] {
        assert_eq!(request(&paths, input).unwrap()["error"]["code"], code);
    }
    for (input, code) in [
        ("not-json\n", -32700),
        (
            "{\"jsonrpc\":\"2.0\",\"id\":\"\",\"method\":\"hello\"}\n",
            -32600,
        ),
        (
            "{\"jsonrpc\":\"2.0\",\"id\":\"a\",\"method\":\"hello\",\"x\":0}\n",
            -32600,
        ),
        (
            "{\"jsonrpc\":\"2.0\",\"id\":\"a\",\"id\":\"b\",\"method\":\"hello\"}\n",
            -32600,
        ),
        ("partial", -32700),
        ("{\"jsonrpc\":\"2.0\"", -32700),
        (
            "{\"jsonrpc\":\"2.0\",\"id\":\"a\",\"method\":\"hello\",\"params\":{\"unexpected\":0}}\n",
            -32602,
        ),
    ] {
        assert_eq!(request(&paths, input).unwrap()["error"]["code"], code);
    }
    let mut stalled = UnixStream::connect(paths.control()).unwrap();
    stalled.write_all(b"{").unwrap();
    assert_eq!(
        hello.instance,
        control::wait_ready(&paths, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .instance
    );
    stalled
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    assert_eq!(stalled.read(&mut [0]).unwrap(), 0);
    server.stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_and_sequential_hello_requests_work_on_one_connection() {
    let (_parent, paths) = fixture();
    let mut server = Server::spawn(&paths);
    server.ready(&paths).await;
    let mut stream = UnixStream::connect(paths.control()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    for id in ["one", "two"] {
        for part in [
            "{\"jsonrpc\":\"2.0\",",
            "\"method\":\"hello\",\"id\":",
            &format!("\"{id}\"}}\n"),
        ] {
            stream.write_all(part.as_bytes()).unwrap();
        }
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let reply = json_value(&line).unwrap();
        assert_eq!(reply["id"], id);
        assert_eq!(reply["result"]["ready"], true);
    }
    server.stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn crlf_and_final_eof_requests_work_and_preserve_escaped_cr() {
    let (_parent, paths) = fixture();
    let mut server = Server::spawn(&paths);
    server.ready(&paths).await;
    let mut stream = UnixStream::connect(paths.control()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"one\\r\",\"method\":\"hello\"}\r\n{\"jsonrpc\":\"2.0\",\"id\":\"two\",\"method\":\"hello\"}")
        .unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut reader = BufReader::new(stream);
    for id in ["one\r", "two"] {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.ends_with('\n'));
        let reply = json_value(&line).unwrap();
        assert_eq!(reply["id"], id);
        assert_eq!(reply["result"]["ready"], true);
    }
    assert_eq!(reader.read(&mut [0]).unwrap(), 0);
    server.stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_accepts_crlf_and_a_final_reply_at_eof() {
    for ending in [b"\r\n".as_slice(), b"".as_slice()] {
        let (_parent, paths) = fixture();
        RuntimeDirectory::prepare(paths.clone()).unwrap();
        let listener = net::UnixListener::bind(paths.control()).unwrap();
        let endpoints = json::json!({"agent": paths.agent(), "control": paths.control()});
        let instance = uuid::Uuid::new_v4();
        let fake = tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            use tokio::io::AsyncWriteExt;
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request = json_value(&line).unwrap();
            let reply = json::json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": {"instance": instance, "supported_versions": [1], "ready": true, "endpoints": endpoints}
            });
            let mut bytes = json::to_vec(&reply).unwrap();
            bytes.extend_from_slice(ending);
            reader.get_mut().write_all(&bytes).await.unwrap();
        });
        let hello = control::wait_ready(&paths, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(hello.instance, instance);
        fake.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_rejects_an_incompatible_live_server_without_waiting_for_takeover() {
    let (_parent, paths) = fixture();
    RuntimeDirectory::prepare(paths.clone()).unwrap();
    let listener = net::UnixListener::bind(paths.control()).unwrap();
    let fake = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (mut stream, _) = listener.accept().await.unwrap();
        stream
            .write_all(b"{\"jsonrpc\":\"3.0\",\"id\":\"wrong\",\"result\":null}\n")
            .await
            .unwrap();
    });
    assert!(
        control::wait_ready(&paths, Instant::now() + Duration::from_secs(1))
            .await
            .is_err()
    );
    fake.await.unwrap();
    assert!(paths.control().exists());
}

#[cfg(target_os = "linux")]
mod privileged {
    use super::*;
    use std::os::unix::process::CommandExt;

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated Linux root container and python3"]
    async fn both_listener_paths_reject_real_foreign_peers() {
        assert!(rustix::process::getuid().is_root());
        let (parent, paths) = fixture();
        let mut server = Server::spawn(&paths);
        server.ready(&paths).await;
        // Weaken only this isolated fixture to reach the independent kernel UID gate.
        fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o711)).unwrap();
        mode(paths.directory(), 0o711);
        for path in [paths.agent(), paths.control()] {
            mode(path, 0o666);
            let output = Command::new("python3").arg("-c")
                .arg("import socket,sys; s=socket.socket(socket.AF_UNIX); s.settimeout(2); s.connect(sys.argv[1]);\ntry:\n s.sendall(b'{\"jsonrpc\":\"2.0\",\"id\":\"foreign\",\"method\":\"hello\"}\\n'); data=s.recv(4096); assert data == b'', data\nexcept (BrokenPipeError,ConnectionResetError): pass")
                .arg(path.as_str()).uid(1001).gid(1001).output().unwrap();
            assert!(output.status.success(), "{:?}", output);
        }
        mode(paths.directory(), 0o700);
        for path in [paths.agent(), paths.control()] {
            mode(path, 0o600);
        }
        server.stop(true);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ids_use_utf8_byte_limits_and_missing_daemon_wait_is_bounded() {
    let (_parent, paths) = fixture();
    assert!(matches!(
        control::wait_ready(&paths, Instant::now()).await,
        Err(control::ControlError::Timeout)
    ));
    let mut server = Server::spawn(&paths);
    server.ready(&paths).await;
    let id = "é".repeat(64);
    let accepted = format!("{{\"jsonrpc\":\"2.0\",\"id\":\"{id}\",\"method\":\"hello\"}}\n");
    assert_eq!(request(&paths, &accepted).unwrap()["id"], id);
    let rejected = format!("{{\"jsonrpc\":\"2.0\",\"id\":\"{id}x\",\"method\":\"hello\"}}\n");
    assert_eq!(request(&paths, &rejected).unwrap()["error"]["code"], -32600);
    server.stop(true);
}

#[tokio::test(flavor = "current_thread")]
async fn restrictive_inherited_umask_does_not_repair_a_lock() {
    let (_parent, paths) = fixture();
    // Set a mask in a fresh child before exec, never in the test process.
    RuntimeDirectory::prepare(paths.clone()).unwrap();
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg("umask 0777; exec \"$1\" --runtime-dir \"$2\" serve")
        .arg("fixture")
        .arg(env!("CARGO_BIN_EXE_ssh-auth-helper"))
        .arg(paths.directory())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::metadata(paths.lock()).unwrap().mode() & 0o7777, 0);
    assert!(!paths.agent().exists() && !paths.control().exists());
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_rejects_invalid_helper_metadata_without_removing_the_listener() {
    for scenario in [
        "nil",
        "not_ready",
        "capability",
        "agent",
        "control",
        "unknown",
    ] {
        let (_parent, paths) = fixture();
        RuntimeDirectory::prepare(paths.clone()).unwrap();
        let listener = net::UnixListener::bind(paths.control()).unwrap();
        let mut result = json::json!({
            "instance": uuid::Uuid::from_u128(1),
            "supported_versions": [1],
            "ready": true,
            "endpoints": {"agent": paths.agent(), "control": paths.control()}
        });
        match scenario {
            "nil" => result["instance"] = json::json!(uuid::Uuid::nil()),
            "not_ready" => result["ready"] = json::json!(false),
            "capability" => result["supported_versions"] = json::json!([2]),
            "agent" => result["endpoints"]["agent"] = json::json!("/other/agent.sock"),
            "control" => result["endpoints"]["control"] = json::json!("/other/control.sock"),
            "unknown" => result["unexpected"] = json::json!(true),
            _ => unreachable!(),
        }
        let fake = tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            use tokio::io::AsyncWriteExt;
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request = json_value(&line).unwrap();
            let response = json::json!({"jsonrpc":"2.0", "id":request["id"], "result":result});
            let mut bytes = json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            reader.get_mut().write_all(&bytes).await.unwrap();
        });
        assert!(
            control::wait_ready(&paths, Instant::now() + Duration::from_secs(1))
                .await
                .is_err(),
            "{scenario}"
        );
        fake.await.unwrap();
        assert!(paths.control().exists());
    }
}

fn json_value(text: &str) -> json::Result<json::Value> {
    json::from_str(text)
}
