//! Subprocess fixtures keep descriptor/environment manipulation out of the runner.

use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

fn scenario(name: &str) {
    let directory = camino_tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let output = Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/detached.py"
        ))
        .arg(env!("CARGO_BIN_EXE_ssh-auth-helper"))
        .arg(directory.path())
        .arg(name)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{name}: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn detached_daemon_closes_ssh_descriptors_and_survives_starter_group_exit() {
    scenario("detachment");
}

#[test]
fn simultaneous_registration_converges_and_failed_callers_preserve_the_daemon() {
    scenario("concurrent");
}

#[test]
fn invalid_inputs_and_read_commands_do_not_start_a_daemon() {
    scenario("no_start");
}

#[test]
fn held_lock_with_absent_endpoints_times_out_without_false_readiness() {
    scenario("lock");
}

#[test]
fn silent_hello_never_authorizes_launch() {
    scenario("silent");
}

#[test]
fn hello_timeout_followed_by_missing_socket_does_not_authorize_launch() {
    scenario("silent_then_absent");
}

#[test]
fn incompatible_hello_preserves_the_listener_and_does_not_launch() {
    scenario("incompatible");
}

#[test]
fn registration_timeout_never_reconnects_or_replays() {
    scenario("reply_timeout");
}

#[test]
fn startup_and_registration_share_one_total_budget() {
    scenario("total_budget");
}

#[test]
#[ignore = "requires OpenSSH ssh-agent, ssh-keygen and ssh-add; creates isolated temporary keys"]
fn real_agent_lists_and_signs_after_automatic_detached_startup() {
    scenario("real_agent");
}
