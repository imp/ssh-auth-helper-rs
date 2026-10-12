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
    client.sendall(b'{"jsonrpc":"2.0","id":"check","method":"hello"}\n')
    print(client.makefile("r").readline().strip())
```

The reply contains the daemon instance UUID, `ready`, and both endpoint paths.
Readiness means listeners/handlers are initialized, not that a forwarded agent
is available. Agent clients currently close immediately without a fabricated
reply. Only `hello` is implemented on the control socket; other operations
return JSON-RPC `Method not found` (-32601). Envelopes require exactly
`jsonrpc: "2.0"`; responses contain one result or error and echo the request ID.
IDs may be nonempty strings of 1..128 UTF-8 bytes, unsigned integers or null.
Negative/fractional IDs are outside this documented profile. An absent ID is
a notification and receives no response. Method params use strict schemas.

Frames are limited to 64 KiB excluding LF, incomplete requests and writes to
two seconds, and active control tasks to 32. LF, CRLF and a complete final
line at EOF are accepted. Invalid JSON receives Parse error (-32700); malformed
envelopes receive Invalid Request (-32600); invalid method params receive
Invalid params (-32602). Duplicate envelope fields are rejected. Method payloads
use ordinary JSON values; duplicate keys inside params/results keep the last
value. Output is serialized into a fixed-capacity buffer before it is queued,
and string newlines are escaped. Empty batches or batches above 32 members are
rejected before execution. Mixed batches omit notification replies, and
all-notification batches emit no frame. Aggregate replies obey the same
64 KiB limit; overflow closes the stream after potentially executed operations.
Batches are not atomic. Clients never automatically replay uncertain operations. Startup/readiness checks share a five-second budget.

Use stable Rust, selected by `rust-toolchain.toml`:

```sh
cargo build --locked --workspace
cargo test --locked --workspace --all-targets
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

The Cargo workspace has three packages, all inheriting shared Rust/Clippy lints:

- Root `ssh-auth-helper`: CLI, runtime safety, startup, supervision and helper
  operation implementations. Existing source and integration-test paths remain here.
- `crates/ssh-auth-protocol`: pure shared message schemas, including
  `HelloRequest`/`HelloResponse`. Operation payloads use `XxxRequest`/`XxxResponse`.
- `crates/jsonrpc-uds`: bounded JSON-RPC parsing and serialization with typed
  frames through `Framed<_, JsonRpcCodec>`. Requests, notifications, responses
  and batches are typed; params/results use `serde_json::Value`. The helper
  owns dispatch, deadlines and readiness calls over the typed stream. Callers
  authorize endpoints and peers before constructing the framed stream.

The wire types are small owned Serde types, with no RPC framework or raw JSON
payloads. Readiness sends one identified request and checks its response under
one absolute connect/send/receive budget. Every return closes that connection;
there are no background client tasks or automatic request replays.

Registry, selection, watch and agent relay implementations follow next.

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
