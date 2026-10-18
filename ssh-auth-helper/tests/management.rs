//! Management CLI against an isolated foreground daemon and raw fake peers.

use std::ffi;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;

use camino::Utf8PathBuf;
use serde_json as json;
use ssh_auth_helper_lib::control;
use ssh_auth_helper_lib::runtime::RuntimePaths;
use tokio::time::Instant;

struct Server {
    _directory: camino_tempfile::Utf8TempDir,
    paths: RuntimePaths,
    child: Child,
}

impl Server {
    async fn start() -> Self {
        let directory = camino_tempfile::tempdir_in("/tmp").unwrap();
        let path = directory.path().join("helper");
        let paths = RuntimePaths::discover(Some(&path), None).unwrap();
        let child = command(&paths)
            .arg("serve")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let server = Self {
            _directory: directory,
            paths,
            child,
        };
        control::wait_ready(&server.paths, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        server
    }

    fn upstream(&self, name: &str) -> Utf8PathBuf {
        self.paths.directory().parent().unwrap().join(name)
    }

    fn status(&self) -> json::Value {
        let output = command(&self.paths)
            .args(["status", "--json"])
            .output()
            .unwrap();
        successful(&output);
        json::from_slice(&output.stdout).unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn command(paths: &RuntimePaths) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ssh-auth-helper"));
    command.arg("--runtime-dir").arg(paths.directory());
    // Child-specific configuration does not mutate the test runner's environment.
    command.env_remove("SSH_AUTH_SOCK");
    command
}

fn successful(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn register_json_status_and_idempotent_silent_cleanup_work() {
    let server = Server::start().await;
    assert!(
        server.status()["registrations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let socket = server.upstream("original");
    let _upstream = UnixListener::bind(&socket).unwrap();
    let output = command(&server.paths)
        .arg("register")
        .arg("--socket")
        .arg(&socket)
        .output()
        .unwrap();
    successful(&output);
    assert!(output.stdout.is_empty());
    let first = server.status();
    let registration = first["registrations"][0].clone();
    assert_eq!(registration["socket"], socket.as_str());
    assert_eq!(registration["health"], "unobserved");
    assert!(registration.get("observed_at").is_none());
    assert!(first["active"].is_null());
    assert_eq!(first["active_relays"], 0);
    let output = command(&server.paths)
        .arg("register")
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .output()
        .unwrap();
    successful(&output);
    let repeated: json::Value = json::from_slice(&output.stdout).unwrap();
    assert_eq!(repeated["registration"], registration);
    let id = registration["id"].as_str().unwrap();
    for _ in 0..2 {
        let output = command(&server.paths)
            .args(["unregister", id])
            .output()
            .unwrap();
        successful(&output);
        assert!(output.stdout.is_empty());
    }
    let output = command(&server.paths)
        .args(["unregister", id, "--json"])
        .output()
        .unwrap();
    successful(&output);
    assert_eq!(
        json::from_slice::<json::Value>(&output.stdout).unwrap(),
        json::json!({})
    );
    let status = server.status();
    assert!(status["registrations"].as_array().unwrap().is_empty());
    assert_eq!(status["recent_retirements"].as_array().unwrap().len(), 1);
    let output = command(&server.paths).arg("status").output().unwrap();
    successful(&output);
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Registered agents: 0")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn socket_environment_is_captured_by_clap_and_explicit_option_overrides_it() {
    let server = Server::start().await;
    let socket = server.upstream("original");
    let _upstream = UnixListener::bind(&socket).unwrap();
    let output = command(&server.paths)
        .env("SSH_AUTH_SOCK", &socket)
        .arg("register")
        .args(["--label", "laptop", "--json"])
        .output()
        .unwrap();
    successful(&output);
    let response: json::Value = json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["registration"]["label"], "laptop");
    let output = command(&server.paths)
        .env("SSH_AUTH_SOCK", ffi::OsStr::from_bytes(b"/tmp/\xff"))
        .arg("register")
        .arg("--socket")
        .arg(&socket)
        .output()
        .unwrap();
    successful(&output);
    assert!(output.stdout.is_empty());
    let before = server.status();
    for output in [
        command(&server.paths).arg("register").output().unwrap(),
        command(&server.paths)
            .env("SSH_AUTH_SOCK", ffi::OsStr::from_bytes(b"/tmp/\xff"))
            .arg("register")
            .output()
            .unwrap(),
    ] {
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
    assert_eq!(server.status(), before);
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_and_invalid_requests_preserve_the_current_registry() {
    let server = Server::start().await;
    let socket = server.upstream("original");
    let _old = UnixListener::bind(&socket).unwrap();
    successful(
        &command(&server.paths)
            .arg("register")
            .arg("--socket")
            .arg(&socket)
            .output()
            .unwrap(),
    );
    let old = server.status()["registrations"][0].clone();
    fs::rename(&socket, server.upstream("old")).unwrap();
    let _new = UnixListener::bind(&socket).unwrap();
    successful(
        &command(&server.paths)
            .arg("register")
            .arg("--socket")
            .arg(&socket)
            .output()
            .unwrap(),
    );
    let before = server.status();
    assert_ne!(before["registrations"][0]["id"], old["id"]);
    successful(
        &command(&server.paths)
            .args(["unregister", old["id"].as_str().unwrap()])
            .output()
            .unwrap(),
    );
    assert_eq!(server.status(), before);
    for output in [
        command(&server.paths)
            .arg("register")
            .arg("--socket")
            .arg(server.paths.agent())
            .output()
            .unwrap(),
        command(&server.paths)
            .arg("register")
            .arg("--socket")
            .arg(server.upstream("missing"))
            .output()
            .unwrap(),
        command(&server.paths)
            .arg("register")
            .arg("--socket")
            .arg(&socket)
            .arg("--label")
            .arg("é".repeat(65))
            .output()
            .unwrap(),
        command(&server.paths)
            .args(["unregister", "bad-id"])
            .output()
            .unwrap(),
    ] {
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert_eq!(server.status(), before);
    }
    let mut stream = UnixStream::connect(server.paths.control()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    writeln!(stream, "{}", json::json!({"jsonrpc":"2.0","id":1,"method":"register","params":{"socket":socket,"extra":true}})).unwrap();
    let mut reply = String::new();
    BufReader::new(stream).read_line(&mut reply).unwrap();
    assert_eq!(
        json::from_str::<json::Value>(&reply).unwrap()["error"]["code"],
        -32602
    );
    assert_eq!(server.status(), before);
}

#[test]
fn management_does_not_start_a_missing_daemon() {
    let directory = camino_tempfile::tempdir_in("/tmp").unwrap();
    let path = directory.path().join("helper");
    let paths = RuntimePaths::discover(Some(&path), None).unwrap();
    let output = command(&paths).arg("status").output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!paths.directory().exists());
}

#[test]
fn uncertain_registration_reply_is_not_replayed() {
    let directory = camino_tempfile::tempdir_in("/tmp").unwrap();
    let base = directory.path();
    let paths = RuntimePaths::discover(Some(base), None).unwrap();
    let listener = UnixListener::bind(paths.control()).unwrap();
    let fake_paths = paths.clone();
    let fake = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let hello: json::Value = json::from_str(&line).unwrap();
        assert_eq!(hello["method"], "hello");
        writeln!(
            stream,
            "{}",
            json::json!({"jsonrpc":"2.0","id":hello["id"],"result":{
                "instance":uuid::Uuid::new_v4(),"supported_versions":[1],"ready":true,
                "endpoints":{"agent":fake_paths.agent(),"control":fake_paths.control()}
            }})
        )
        .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        let request: json::Value = json::from_str(&line).unwrap();
        assert_eq!(request["method"], "register");
        // Simulate successful mutation followed by an unusable response.
        writeln!(
            stream,
            "{}",
            json::json!({"jsonrpc":"2.0","id":"wrong","result":{}})
        )
        .unwrap();
        let mut extra = [0];
        assert_eq!(reader.read(&mut extra).unwrap(), 0);
        listener.set_nonblocking(true).unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    });
    let output = command(&paths)
        .arg("register")
        .arg("--socket")
        .arg(base.join("original"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    fake.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_batches_do_not_register_members_and_valid_notifications_are_silent() {
    let server = Server::start().await;
    let socket = server.upstream("original");
    let _upstream = UnixListener::bind(&socket).unwrap();
    let before = server.status();
    let mut stream = UnixStream::connect(server.paths.control()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let call = json::json!({"jsonrpc":"2.0","id":1,"method":"register","params":{"socket":socket}});
    writeln!(stream, "{}", json::json!([call])).unwrap();
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    stream.read_line(&mut line).unwrap();
    let reply: json::Value = json::from_str(&line).unwrap();
    assert!(reply["id"].is_null());
    assert_eq!(reply["error"]["code"], -32600);
    assert_eq!(server.status(), before);
    writeln!(
        stream.get_mut(),
        "{}",
        json::json!({"jsonrpc":"2.0","method":"register","params":{"socket":socket}})
    )
    .unwrap();
    writeln!(
        stream.get_mut(),
        "{}",
        json::json!({"jsonrpc":"2.0","id":2,"method":"status"})
    )
    .unwrap();
    line.clear();
    stream.read_line(&mut line).unwrap();
    let reply: json::Value = json::from_str(&line).unwrap();
    assert_eq!(reply["id"], 2);
    assert_eq!(
        reply["result"]["registrations"].as_array().unwrap().len(),
        1
    );
}
