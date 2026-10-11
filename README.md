# ssh-auth-helper

A Rust helper under development for keeping a stable SSH agent socket usable
across remote SSH connections and long-lived tmux sessions.

The current CLI provides path discovery and a foreground daemon with private
sockets, singleton locking, bounded control readiness, crash recovery, and
identity-checked cleanup. Upstream registration and SSH agent forwarding are
not implemented yet.

```sh
cargo run --locked -- socket-path
cargo run --locked -- --runtime-dir /tmp/my-helper socket-path
```

An explicit runtime directory takes precedence over `XDG_RUNTIME_DIR`.
Otherwise the planned directory is `$XDG_RUNTIME_DIR/ssh-auth-helper`, or
`/tmp/ssh-auth-helper-<uid>` when that variable is absent. Path discovery makes
no filesystem changes and does not establish that a directory is safe to use.
Inputs must be absolute UTF-8 paths without traversal, NULs, or line breaks;
socket path lengths are checked in bytes for Linux and macOS.

The library also provides `RuntimeDirectory::prepare`: it creates only the
final helper directory, retains its descriptor, checks ownership and mode
`0700`, and inspects existing endpoint types, owners, and mode `0600` without
replacing them. Ancestors must be trusted; only root-owned ancestor aliases
are accepted, and the helper directory itself cannot be a symlink. Existing
unsafe objects are rejected without permission repair. Peer authorization
uses kernel Unix-stream credentials and requires the same UID on both listeners.
The daemon holds `daemon.lock` for its lifetime and leaves that file in place.
Only the lockholder recovers owned stale sockets; any successful connection
protects an existing listener, even if silent or incompatible.

Run the daemon in the foreground using an isolated path whose parent exists:

```sh
cargo run --locked -- --runtime-dir /tmp/my-helper serve
```

SIGINT or SIGTERM stops acceptance, joins connection tasks, and removes only
this instance's matching socket entries. A crash may leave socket files; the
next lockholder recovers them after connection refusal and identity checks.
Unexpected objects are preserved and reported. Recovery and cleanup operate
within a same-user trust boundary; pathname checks are not atomic protection
against a malicious process running as that same user. Runtime storage must
be on a local filesystem.

A local client can check readiness independently of agent availability:

```python
import socket
with socket.socket(socket.AF_UNIX) as client:
    client.settimeout(2)
    client.connect("/tmp/my-helper/control.sock")
    client.sendall(b'{"version":1,"id":"check","op":"hello"}\n')
    print(client.makefile("r").readline().strip())
```

The reply contains the daemon instance UUID, `ready`, and both endpoint paths.
Readiness means listeners/handlers are initialized, not that a forwarded agent
is available. Agent clients currently close immediately without a fabricated
reply. Only `hello` is implemented on the control socket; other operations
return `unknown_operation`. Frames are limited to 64 KiB excluding LF, IDs to
1..128 UTF-8 bytes, incomplete requests and writes to two seconds, and active
control tasks to 32. Startup/readiness checks share a five-second budget.

Use stable Rust, selected by `rust-toolchain.toml`:

```sh
cargo build --locked
cargo test --locked --all-targets
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

This is one Cargo package with library and binary targets. `main.rs` handles
the process exit, `cli.rs` handles arguments/output, and `runtime.rs` owns path
discovery and private-directory preparation. `runtime/startup.rs` owns the lifetime guard and endpoint cleanup, `daemon.rs`
supervises listeners, and `control.rs` implements bounded version-1 JSON framing
and hello. Registry, selection, and agent relay modules follow next.

The selected foundation uses Camino paths, parking_lot for short shared-state
locks, Jiff UTC timestamps, and Tokio for asynchronous I/O and monotonic
deadlines. Forwarding and real SSH/tmux acceptance remain under development. No installation step alters personal configuration.

Linux tests requiring real foreign ownership or peer UIDs are explicitly
ignored in the ordinary test run. Run them only in an isolated root container
with Python 3 available:

```sh
cargo test --locked --test runtime -- --ignored
cargo test --locked --test startup -- --ignored
```
