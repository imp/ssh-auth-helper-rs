use std::{ffi::OsStr, os::unix::ffi::OsStrExt, process::Command};

fn binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ssh-auth-helper"));
    command.env_remove("XDG_RUNTIME_DIR");
    command
}

#[test]
fn socket_path_discovery_has_no_filesystem_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("helper");
    let output = binary()
        .arg("--runtime-dir")
        .arg(&runtime)
        .arg("socket-path")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        format!("{}\n", runtime.join("agent.sock").display()).as_bytes()
    );
    assert!(output.stderr.is_empty());
    assert!(!runtime.exists());
}

#[test]
fn non_utf8_cli_path_is_rejected() {
    let output = binary()
        .arg("--runtime-dir")
        .arg(OsStr::from_bytes(b"/tmp/\xff"))
        .arg("socket-path")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

#[test]
fn invalid_paths_report_failure_without_stdout() {
    let output = binary()
        .arg("--runtime-dir")
        .arg("relative")
        .arg("socket-path")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("must be absolute")
    );
}
