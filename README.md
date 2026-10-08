# ssh-auth-helper

A Rust helper under development for keeping a stable SSH agent socket usable
across remote SSH connections and long-lived tmux sessions.

The current CLI provides path discovery. It does not start a
daemon, create sockets, register forwarded agents, or proxy signing requests.

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
uses kernel Unix-stream credentials and requires the same UID. Binding,
lifetime locking, readiness, and recovery remain under development.

Use stable Rust, selected by `rust-toolchain.toml`:

```sh
cargo build --locked
cargo test --locked --all-targets
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

This is one Cargo package with library and binary targets. `main.rs` handles
the process exit, `cli.rs` handles arguments/output, and `runtime.rs` owns path
discovery and private-directory preparation. Daemon supervision, the bounded JSON control protocol, registry,
selection, and agent relay modules will be added with their implementations.

The selected foundation uses Camino paths, parking_lot for short shared-state
locks, Jiff UTC timestamps, and Tokio for asynchronous I/O and monotonic
deadlines. Lifecycle, permissions, forwarding, and real SSH/tmux acceptance
remain under development. No installation step alters personal configuration.

Linux tests requiring real foreign ownership or peer UIDs are explicitly
ignored in the ordinary test run. Run them only in an isolated root container
with Python 3 available:

```sh
cargo test --locked --test runtime -- --ignored
```
