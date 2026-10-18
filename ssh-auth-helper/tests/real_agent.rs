//! Opt-in smoke test using temporary keys and isolated OpenSSH agents only.

use std::fs;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use camino::Utf8Path;
use camino::Utf8PathBuf;
use serde_json as json;
use ssh_auth_helper_lib::control;
use ssh_auth_helper_lib::runtime::RuntimePaths;

struct Process(Option<Child>);

impl Process {
    fn spawn(command: &mut Command) -> Self {
        Self(Some(
            command
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ))
    }

    fn output(mut self) -> Output {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.0.as_mut().unwrap().try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "child exceeded five seconds");
            std::thread::sleep(Duration::from_millis(5));
        }
        self.0.take().unwrap().wait_with_output().unwrap()
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            child.wait().unwrap();
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

fn checked(command: &mut Command) -> Output {
    let output = Process::spawn(command).output();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

struct Agent {
    process: Process,
    socket: Utf8PathBuf,
    key: Utf8PathBuf,
}

impl Agent {
    fn start(base: &Utf8Path, name: &str) -> Self {
        let socket = base.join(format!("{name}.sock"));
        let key = base.join(name);
        checked(Command::new("ssh-keygen").args([
            "-q",
            "-t",
            "ed25519",
            "-N",
            "",
            "-C",
            name,
            "-f",
            key.as_str(),
        ]));
        let process = Process::spawn(Command::new("ssh-agent").args(["-D", "-a", socket.as_str()]));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() {
            assert!(Instant::now() < deadline, "agent did not bind its socket");
            std::thread::sleep(Duration::from_millis(5));
        }
        Self {
            process,
            socket,
            key,
        }
    }

    fn load(&self) {
        checked(
            Command::new("ssh-add")
                .env("SSH_AUTH_SOCK", &self.socket)
                .arg(&self.key),
        );
    }

    fn public_key(&self) -> String {
        fs::read_to_string(self.key.with_extension("pub")).unwrap()
    }
}

fn helper(paths: &RuntimePaths) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ssh-auth-helper"));
    command.arg("--runtime-dir").arg(paths.directory());
    command.env_remove("SSH_AUTH_SOCK");
    command
}

fn register(paths: &RuntimePaths, socket: &Utf8Path) -> json::Value {
    let output = checked(helper(paths).args(["register", "--socket", socket.as_str(), "--json"]));
    json::from_slice::<json::Value>(&output.stdout).unwrap()["registration"]["id"].clone()
}

fn status(paths: &RuntimePaths) -> json::Value {
    json::from_slice(&checked(helper(paths).args(["status", "--json"])).stdout).unwrap()
}

fn list(paths: &RuntimePaths) -> String {
    String::from_utf8(
        checked(
            Command::new("ssh-add")
                .env("SSH_AUTH_SOCK", paths.agent())
                .arg("-L"),
        )
        .stdout,
    )
    .unwrap()
}

fn sign(paths: &RuntimePaths, agent: &Agent) {
    checked(
        Command::new("ssh-add")
            .env("SSH_AUTH_SOCK", paths.agent())
            .arg("-T")
            .arg(agent.key.with_extension("pub")),
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires installed OpenSSH ssh-agent, ssh-keygen and ssh-add; creates isolated temporary keys"]
async fn real_agents_support_empty_health_signing_sticky_selection_and_failover() {
    let directory = camino_tempfile::tempdir_in("/tmp").unwrap();
    let base = directory.path();
    let paths = RuntimePaths::discover(Some(&base.join("helper")), None).unwrap();
    let daemon = Process::spawn(helper(&paths).arg("serve"));
    control::wait_ready(&paths, tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let mut a = Agent::start(base, "a");
    let aid = register(&paths, &a.socket);
    let empty = Process::spawn(
        Command::new("ssh-add")
            .env("SSH_AUTH_SOCK", paths.agent())
            .arg("-l"),
    )
    .output();
    assert_eq!(empty.status.code(), Some(1));
    let snapshot = status(&paths);
    assert_eq!(snapshot["active"], aid);
    assert_eq!(snapshot["registrations"][0]["health"], "available");
    a.load();
    assert_eq!(list(&paths), a.public_key());
    sign(&paths, &a);

    let mut b = Agent::start(base, "b");
    b.load();
    let old_bid = register(&paths, &b.socket);
    assert_eq!(list(&paths), a.public_key());
    b.process.stop();
    assert_eq!(list(&paths), a.public_key());
    sign(&paths, &a);

    // Keep the old inode alive so this checks replacement rather than inode reuse.
    if b.socket.exists() {
        fs::rename(&b.socket, base.join("old-b.sock")).unwrap();
    }
    b.process = Process::spawn(Command::new("ssh-agent").args(["-D", "-a", b.socket.as_str()]));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !b.socket.exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    b.load();
    let bid = register(&paths, &b.socket);
    assert_ne!(old_bid, bid);
    assert_eq!(list(&paths), a.public_key());
    a.process.stop();
    assert_eq!(list(&paths), b.public_key());
    sign(&paths, &b);
    assert_eq!(status(&paths)["active"], bid);

    b.process.stop();
    let unavailable = Process::spawn(
        Command::new("ssh-add")
            .env("SSH_AUTH_SOCK", paths.agent())
            .arg("-L"),
    )
    .output();
    assert!(!unavailable.status.success());
    assert!(status(&paths)["active"].is_null());

    let pid = rustix::process::Pid::from_raw(daemon.0.as_ref().unwrap().id().try_into().unwrap())
        .unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let output = daemon.output();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!paths.agent().exists());
    assert!(!paths.control().exists());
}
